import optparse
import logging
import os
import shutil
import asyncio
import json
import numpy as np
from collections import namedtuple
import mongo_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
import tarfile
import parseheader

BEEOND_TEMP_DIR = "/beeond/PROCESSING/TEMP/"

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

#%% Pipeline utils definitions

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

def delete_files_if_exists(directory):
    files = os.listdir(directory)
    for file in files:
        if file.endswith(".inf") or file.endswith(".dat") or file.endswith(".csv") or file.endswith(".json") or file.endswith(".png") or file.endswith(".log") or file.endswith(".yaml"):
            log.warning(
                f"Removing existing file with name {os.path.join(directory, file)}")
            os.remove(os.path.join(directory, file))
            
def make_tarfile(output_path, input_path, name):
    with tarfile.open(output_path + '/' + name, "w:gz") as tar:
        tar.add(input_path, arcname=name)

def select_data_products(beam, filter_func=None):
    dp_list = []
    for dp in beam["data_products"]:
        if filter_func:
            if filter_func(dp["filename"]):
                dp_list.append(dp["filename"])
        else:
            dp_list.append(dp["filename"])
    return dp_list

def get_fil_dict(input_file):
    filterbank_info = parseheader.parseSigprocHeader(input_file)
    filterbank_stats = parseheader.updateHeader(filterbank_info)
    return filterbank_stats

def get_obs_length(filterbanks):
    return sum([get_fil_dict(fname)['tobs'] for fname in filterbanks])

#%% PulsarX and riptide definitions

async def dedisperse_all_fil(input_fils, processing_dir,
                  rootname,
                  ddplan_args,
                  beam_name,
                  tscrunch=1,
                  fscrunch=1,
                  rfi_flags="kadaneF 1 2 zdot",
                  num_threads=2,
                  segment_length=2.0,
                  zapping_threshold=4.):
    delete_files_if_exists(processing_dir)
    
    #Get beam tag
    if 'ifbf' in beam_name:
        beam_tag = "--incoherent"
        log.info("Incoherent beam")
    elif 'cfbf' in beam_name:
        beam_tag = "--ibeam {}".format(int(beam_name.strip("cfbf")))
        log.info("Beam number "+beam_tag)
    else:
        log.warning(
            "Invalid beam name. Folding with default beam name")
        beam_tag = ""
    
    #Generate ddplan file
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
    
    ddplan_file.close()
    
    cmd = f"dedisperse_all_fil --verbose --format presto --threads {num_threads} --zapthre {zapping_threshold} --fd {fscrunch} --td {tscrunch} -l {segment_length} --ddplan {ddplan_file} --baseline {0} {0} --rfi {rfi_flags} --rootname {rootname} {beam_tag} -f {' '.join(input_fils)}"

    #Run dedispersion
    try:
        await shell_call(cmd, cwd=processing_dir)
    except Exception as error:
        log.error("dedisperse_all_fil call failed, cleaning up partial files")
        delete_files_if_exists(processing_dir)
        raise error

async def riptide(config_file, rootname, processing_dir):
    
    #Generate riptide config file
    log.info("Generating riptide config file")
    config_file_path = processing_dir+'config.yaml'
    config_file_handle = open(config_file_path, 'w')
    config_file_handle.write(config_file)
    config_file_handle.close()
    
    cmd = (f"rffa -k {config_file_path}  "
           f"-o {rootname} *.inf")
    
    try:
        await shell_call(cmd, cwd=processing_dir)
    except Exception as error:
        log.error("riptide call failed, cleaning up partial files")
        delete_files_if_exists(processing_dir)
        raise error

#%% Pipeline

async def riptide_pipeline(data, status_callback):
    # Limit core size to avoid ephemeral-storage overflows
    # in the event of segmentation faults
    await shell_call("ulimit -c  0")

    #Get processing args
    processing_args = data["processing_args"]
    output_dir = data["base_output_dir"]
    processing_id = data["processing_id"]
    config_file = processing_args["config_file"]
    rfi_flags = processing_args["rfi_flags"]
    ddplan_args = processing_args["ddplan"]

    log.info(f"Creating output directory: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    output_dps = []

    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:
            
            beam_name = beam["name"]
            
            input_fil_list = sorted(select_data_products(
                beam, lambda fname: fname.endswith(".fil")))
            
            obs_length = get_obs_length(input_fil_list)

            if processing_args["temp_filesystem"] == "/beeond/":
                log.info("Running on Beeond")
                processing_dir = os.path.join(BEEOND_TEMP_DIR, str(processing_id))
            else:
                log.info("Running on BeeGFS")
                processing_dir = os.path.join(output_dir, "processing/")
            os.makedirs(processing_dir, exist_ok=True)
            
            try:
                #Dedisperse the filterbanks and output to temporary processing directory
                log.info(
                    "Dedispersing filterbanks and performing RFI mitigation")
                status_callback(
                    "Dedispersing filterbanks and performing RFI mitigation")
                await dedisperse_all_fil(input_fil_list, processing_dir,
                               f"dedispersed_{processing_id}_",
                              ddplan_args, beam_name,
                              rfi_flags)
                
                #Run riptide in the temporary processing directory
                log.info("Riptide search")
                status_callback("Riptide search")
                await riptide(config_file,
                    f"riptide_{processing_id}_", processing_dir)
            
                 
                #Add information to the riptide csv output for the multibeam filter
                #position of beam, observation length, sampling time
                log.info("Adding beam info to riptide output file")
                riptide_output_path = processing_dir+'candidates.csv'
                riptide_output_handle = open(riptide_output_path, 'a') #append mode
                filterbank_header = get_fil_dict(beam)
                riptide_output_handle.write(str(filterbank_header["ra"])+'\n')
                riptide_output_handle.write(str(filterbank_header["dec"])+'\n')
                riptide_output_handle.write(str(obs_length)+'\n')
                riptide_output_handle.write(str(filterbank_header["tsamp"])+'\n')
                riptide_output_handle.close()

                #Move the riptide csv candidate file to its final location
                shutil.move(
                        os.path.join(processing_dir, "candidates.csv"),
                        os.path.join(output_dir, "candidates.csv"))
                log.info("Transferred riptide csv candidate file to final location")
                #Create data product
                dp = dict(
                    type="riptide_candidate_tar_file",
                    filename="candidates.csv",
                    directory=data["base_output_dir"],
                    beam_id=beam["id"],
                    pointing_id=pointing["id"],
                    metainfo=json.dumps({"rfi_flags":rfi_flags, "ddplan_args":ddplan_args})
                )
                output_dps.append(dp)

            except Exception as error:
                raise error
            finally:
                shutil.rmtree(processing_dir) #delete dedispersed files and riptide files that are not tarred
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
