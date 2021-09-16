import optparse
#import subprocess
import logging
import parseheader
import os
#import glob
import shutil
import asyncio
import json
import numpy as np
from collections import namedtuple
import mongo_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper


BEEOND_TEMP_DIR = "/beeond/PROCESSING/TEMP/"
#MAX_FFT_LEN = 201326592

#%% Start a log for the pipeline
log = logging.getLogger("riptide_search")
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel("INFO")

#%% DDplan for definition from webpage form

DMRange = namedtuple(
    "DMRange",
    ["low_dm", "high_dm", "dm_step", "tscrunch"])


class DDPlan(object):
    def __init__(self):
        self._segments = []

    def add_range(self, low_dm, high_dm, step, tscrunch):
        self._segments.append(DMRange(low_dm, high_dm, step, tscrunch))

    def __iter__(self):
        return iter(sorted(self._segments, key=lambda x: x.tscrunch))

    def __str__(self):
        out = []
        for r in self._segments:
            out.append(str(r))
        return "\n".join(out)

    @classmethod
    def from_string(cls, plan):
        inst = cls()
        for line in plan.splitlines():
            low_dm, high_dm, dm_step, tscrunch = list(
                map(float, line.split()[:4]))
            inst.add_range(low_dm, high_dm, dm_step, int(tscrunch))
        return inst


def slices(csv):
    for value in csv.split(","):
        if ":" not in value:
            yield float(value)
        else:
            x = value.split(":")
            start = float(x[0])
            end = float(x[1])
            if len(x) > 2:
                step = float(x[2])
            else:
                step = 1
            for subvalue in np.arange(start, end, step):
                yield subvalue
                



#%% Pipeline utils


async def shell_call(cmd, cwd="./"):
    log.info(f"Shell call: {cmd}")
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=None,
        stderr=None,
        cwd=cwd)
    retcode = await proc.wait()
    if retcode != 0:
        raise Exception(f"Process return-code {retcode}")


def delete_files_if_exists(dir):
    files = os.listdir(dir)
    for file in files:
        if file.endswith(".inf") or file.endswith(".dat"):
            log.warning(
                f"Removing existing file with name {os.path.join(dir, file)}")
            os.remove(os.path.join(dir, file))



#%% PulsarX utils

async def dedisperse_all_fil(input_fils, processing_dir,
                  rootname,
                  ddplan_args,
                  beam_tag,
                  tscrunch=1,
                  fscrunch=1,
                  incoherent=False,
                  rfi_flags="kadaneF 1 2 zdot",
                  num_threads=2,
                  nbits=8,
                  segment_length=2.0,
                  zapping_threshold=4.):
    delete_files_if_exists(processing_dir)

    # generate ddplan file
    try:
        log.info("Parsing DDPlan")
        ddplan = DDPlan.from_string(ddplan_args)
    except Exception as error:
        log.exception("Unable to parse DDPlan")
        raise error


    ddplan_fname = os.path.join(processing_dir, "ddplan.txt")

    with open(ddplan_fname, 'w') as ddplan_file:
        for dm_range in ddplan:
            segment = f"{dm_range.tscrunch} 1 {dm_range.low_dm} {dm_range.dm_step} {np.int(np.ceil((dm_range.high_dm-dm_range.low_dm)/dm_range.dm_step))} \n"
            ddplan_file.write(segment)


#%%
    
    cmd = f"dedisperse_all_fil --verbose --format presto --nbits {nbits} --threads {num_threads} --zapthre {zapping_threshold} --fd {fscrunch} --td {tscrunch} -l {segment_length} --ddplan {ddplan_file} --baseline {0} {0} --rfi {rfi_flags} --rootname {rootname} {beam_tag} -f {' '.join(input_fils)}"


    # run dedispersion
    try:
        await shell_call(cmd, cwd=processing_dir)
    except Exception as error:
        log.error("dedisperse_all_fil call failed, cleaning up partial files")
        delete_files_if_exists(processing_dir)
        raise error
        

#%% riptide utils

config_file ='path_to_config_file'


async def riptide(config_file, input_infs, rootname, output_dir):
    cmd = (f"rffa -k {config_file}  "
           f"-o {rootname} {input_infs}")
    await shell_call(cmd, cwd=output_dir)



def get_fil_dict(input_file):
    filterbank_info = parseheader.parseSigprocHeader(input_file)
    filterbank_stats = parseheader.updateHeader(filterbank_info)
    return filterbank_stats




def select_data_products(beam, filter_func=None):
    dp_list = []
    for dp in (beam["data_products"]):
        if filter_func:
            if filter_func(dp["filename"]):
                dp_list.append(dp["filename"])
        else:
            dp_list.append(dp["filename"])
    return dp_list


def dmfile_from_dmrange(dm_range, outfile):
    # Generate actual dm list file
    dm_csv = f"{dm_range.low_dm}:{dm_range.high_dm}:{dm_range.dm_step}"
    dm_list = sorted(list(set(list(slices(dm_csv)))))
    np.savetxt(outfile, dm_list, fmt='%.3f')
    return outfile

#%% Pipeline


async def riptide_pipeline(data, status_callback):
    # Limit core size to avoid ephemeral-storage overflows
    # in the event of segmentation faults
    await shell_call("ulimit -c  0")

    processing_args = data["processing_args"]
    output_dir = data["base_output_dir"]
    processing_id = data["processing_id"]

    log.info(f"Creating output directory: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    output_dps = []



    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:
            
            dps = sorted(select_data_products(
                beam, lambda fname: fname.endswith(".fil")))

            if processing_args["temp_filesystem"] == "/beeond/":
                log.info("Running on Beeond")
                processing_dir = os.path.join(BEEOND_TEMP_DIR, str(processing_id))
            else:
                log.info("Running on BeeGFS")
                processing_dir = os.path.join(output_dir, "processing/")
            os.makedirs(processing_dir, exist_ok=True)
            try:
                # First dedisperse the filterbanks and output to processing directory
                log.info("Executing dedispersion and rfi cleaning")
                rfi_flags = processing_args.get("rfi_flags", "kadaneF 1 2 zdot")
                ddplan_args = processing_args["ddplan"]

                beam_name = beam["name"]
                if 'ifbf' in beam_name:
                    beam_tag = "--incoherent"
                elif 'cfbf' in beam_name:
                    beam_tag = "--ibeam {}".format(int(beam_name.strip("cfbf")))
                else:
                    log.warning(
                        "Invalid beam name. Folding with default beam name")
                    beam_tag = ""
                    
                status_callback(
                    "Dedispersing filterbanks and performing RFI mitigation")
                await dedisperse_all_fil(dps, processing_dir,
                               f"dedispersed_{processing_id}_",
                              ddplan_args, beam_tag,
                              rfi_flags)
                
            


                    
                status_callback("Riptide search")
                await riptide(config_file,
                    dps,
                    f"riptide_{processing_id}_", output_dir)

# =============================================================================
#                     # We do not keep the candidates.peasoup files as they can be massive
#                     peasoup_candidate_file = os.path.join(
#                         peasoup_output_dir, "candidates.peasoup")
#                     os.remove(peasoup_candidate_file)
# =============================================================================


                dp = dict(
                    type="riptide_csv",
                    filename=os.path.join(output_dir, "candidates.csv"),
                    directory=data["base_output_dir"],
                    beam_id=beam["id"],
                    pointing_id=pointing["id"],
                    metainfo=json.dumps({"rfi_flags":rfi_flags, "ddplan_args":ddplan_args})
                )
                output_dps.append(dp)
            except Exception as error:
                raise error
            finally:
                shutil.rmtree(processing_dir) #delete dedispersed files
    return output_dps


def pipeline(data, status_callback):
    loop = asyncio.get_event_loop()
    output_dps = loop.run_until_complete(
        riptide_pipeline(data, status_callback))
    return output_dps


#%% Run the pipeline

if __name__ == '__main__':
    parser = optparse.OptionParser()
    mongo_wrapper.add_mongo_consumer_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts, args = parser.parse_args()

    # processor = mongo_wrapper.PikaProcess(...)
    processor = mongo_wrapper.mongo_consumer_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts, pipeline)
    processor.process(pipeline_wrapper.on_receive)
