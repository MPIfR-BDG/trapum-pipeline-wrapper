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


def delete_file_if_exists(output_fil):
    if os.path.isfile(output_fil):
        log.warning(f"Removing existing file with name {output_fil}")
        os.remove(output_fil)


# =============================================================================
# def compare_lengths(expected, actual, tolerance=0):
#     diff = abs(expected - actual)
#     if diff > expected * tolerance:
#         log.error(f"Output file has length of {actual} "
#                   f"samples, expected {expected} samples")
#         raise Exception("Digifil output file length error")
# =============================================================================

#%% PulsarX utils

async def dedisperse_all_fil(input_fils, output_dir,
                  rootname,
                  ddplan,
                  fscrunch,
                  rfi_flags="kadaneF 1 2 zdot",
                  num_threads=2,
                  nbits=8,
                  outmean=128.,
                  outstd=6.,
                  segment_length=2.0,
                  zapping_threshold=4.):
    delete_files_if_exists(output_dir)

# =============================================================================
#     # generate filplan file
# 
#     filplan_fname = os.path.join(output_dir, "filplan.json")
# 
#     with open(filplan_fname, 'w') as filplan_file:
#         plans = []
#         for dm_range in ddplan:
#             plans.append({"time_downsample": dm_range.tscrunch,
#                           "frequency_downsample": 1,
#                           "baseline_width": 0.,
#                           "dataout_mean": outmean,
#                           "dataout_std": outstd,
#                           "dataout_nbits": nbits,
#                           "rfi_flags": ""})
#         json.dump(plans, filplan_file)
# =============================================================================





#%%


    
    cmd = f"dedisperse_all_fil -v --format presto -t {num_threads} --zapthre {zapping_threshold} --fd {fscrunch}  -l {segment_length} --ddplan {ddplan} --baseline {0} {0} -z {rfi_flags} -o {rootname} -f {' '.join(input_fils)}"


    # run dedispersion
    try:
        await shell_call(cmd, cwd=output_dir)
    except Exception as error:
        log.error("dedisperse_all_fil call failed, cleaning up partial files")
        delete_files_if_exists(output_dir)
        raise error
        

#%% Here define config file

config_file ='path_to_config_file'


async def riptide(config_file, input_infs, out_dir):
    cmd = (f"rffa -k {config_file}  "
           f"-o {out_dir} -f {out_dir}/rffa.log {input_infs}")
    await shell_call(cmd)


# =============================================================================
# def decide_fft_size(filterbank_headers):
#     nsamples = 0
#     for filterbank_header in filterbank_headers:
#         nsamples += filterbank_header['nsamples']
#     bit_length = int(nsamples).bit_length()
#     if 2**bit_length != 2 * int(nsamples):
#         return 2**bit_length
#     else:
#         return int(nsamples)
# 
# =============================================================================

def get_fil_dict(input_file):
    filterbank_info = parseheader.parseSigprocHeader(input_file)
    filterbank_stats = parseheader.updateHeader(filterbank_info)
    return filterbank_stats


# =============================================================================
# def generate_chan_mask(chan_mask_csv, filterbank_header, outfile):
#     ftop = filterbank_header['ftop']
#     fbottom = filterbank_header['fbottom']
#     nchans = filterbank_header['nchans']
#     chan_mask = np.ones(nchans)
#     for val in chan_mask_csv.split(','):
#         if len(val.split(":")) == 1:
#             rstart = float(val)
#             rend = float(val)
#         elif len(val.split(":")) == 2:
#             rstart = float(val.split(":")[0])
#             rend = float(val.split(":")[1])
#         else:
#             log.warning("Could not understand mask entry: {}".format(val))
#             continue
#         chbw = (ftop - fbottom) / nchans
#         idx0 = int(min(max((rstart - fbottom) // chbw, 0), nchans - 1))
#         idx1 = int(max(min(int((rend - fbottom) / chbw + 0.5), nchans - 1), 0))
#         chan_mask[idx0:idx1 + 1] = 0
#     np.savetxt(outfile, chan_mask, fmt='%d')
# =============================================================================


# =============================================================================
# def generate_birdie_list(birdie_csv, outfile):
#     birdies = []
#     birdies_width = []
#     for val in birdie_csv.split(','):
#         try:
#             f = val.split(":")[0]
#             w = val.split(":")[1]
#         except Exception:
#             log.warning("Could not parse birdie list entry: {}".format(val))
#             continue
#         else:
#             birdies.append(f)
#             birdies_width.append(w)
#     try:
#         np.savetxt(
#             outfile, np.c_[
#                 np.array(
#                     birdies, dtype=float), np.array(
#                     birdies_width, dtype=float)], fmt="%.2f")
#     except Exception as error:
#         log.error(error)
# =============================================================================


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

    try:
        log.info("Parsing DDPlan")
        ddplan = DDPlan.from_string(processing_args["ddplan"])
    except Exception as error:
        log.exception("Unable to parse DDPlan")
        raise error

    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:

            dps = sorted(select_data_products(
                beam, lambda fname: fname.endswith(".fil")))

            if processing_args["temp_filesystem"] == "/beeond/":
                log.info("Running on Beeond")
                processing_dir = os.path.join(
                    BEEOND_TEMP_DIR, str(processing_id))
            else:
                log.info("Running on BeeGFS")
                processing_dir = os.path.join(output_dir, "processing/")
            os.makedirs(processing_dir, exist_ok=True)
            try:

                # First merge the filterbanks and output to processing directory
                # this is done at whatever the native resolution of the data is
                log.info("Executing file merge")
                rfi_flags = processing_args.get("rfi_flags", "zdot")
                status_callback(
                    "Merging filterbanks and perform rfi mitigation")

                await dedisperse_all_fil(dps, processing_dir,
                              f"temp_merge_p_id_{processing_id}",
                              ddplan,
                              rfi_flags=rfi_flags)

                #filterbank_headers = [get_fil_dict(dp) for dp in dps]

# =============================================================================
#                 # Determine fft_size for Peasoup call
#                 if processing_args['fft_length'] == 0:
#                     fft_size = decide_fft_size(filterbank_headers)
#                 else:
#                     fft_size = processing_args['fft_length']
# =============================================================================

# =============================================================================
#                 # Hard coded for max limit - tmp assuming 4hr, 76 us
#                 # This is related to the available RAM of GTX 1080Ti GPUs
#                 # and the limitations of the 32-bit implementation
#                 # of the CUFFT library
#                 if fft_size > MAX_FFT_LEN:
#                     fft_size = MAX_FFT_LEN
#                 log.info(f"Chose base FFT length of {fft_size}")
# =============================================================================

# =============================================================================
#                 # Determine channel mask to use
#                 log.info("Determining channel mask")
#                 chan_mask_csv = processing_args["channel_mask"]
#                 chan_mask_file = "channel_mask.ascii"
#                 generate_chan_mask(
#                     chan_mask_csv, filterbank_headers[0], chan_mask_file)
# 
#                 # Determine birdie list to use
#                 log.info("Determining birdie list")
#                 birdie_list_csv = processing_args["birdie_list"]
#                 birdie_list_file = "birdie_list.ascii"
#                 generate_birdie_list(birdie_list_csv, birdie_list_file)
# =============================================================================

# =============================================================================
#                 # Set RAM limit
#                 ram_limit = processing_args['ram_limit']
# =============================================================================

                log.info("Instantiating downsampling manager")
                #tscrunches = [dm_range.tscrunch for dm_range in ddplan]

                log.info("Starting loop over DDPlan")
                for k, dm_range in enumerate(ddplan):
                    log.info(f"Processing DM range: {dm_range}")
                    search_file = os.path.join(
                        processing_dir, f"temp_merge_p_id_{processing_id}_{k+1:02d}.fil")
                    log.info(f"Searching file: {search_file}")
                    dm_list_file = "dm_list.ascii"
                    dmfile_from_dmrange(dm_range, dm_list_file)

                    #curr_fft_size = fft_size // dm_range.tscrunch
                    riptide_output_dir = os.path.join(
                        processing_dir,
                        f"dm_range_{dm_range.low_dm:03f}_{dm_range.high_dm:03f}")
                    status_callback(
                        f"Peasoup (DM: {dm_range.low_dm} - {dm_range.high_dm})")
                    
                    
                    await riptide(config_file,
                        search_file,
                        riptide_output_dir)

# =============================================================================
#                     # We do not keep the candidates.peasoup files as they can be massive
#                     peasoup_candidate_file = os.path.join(
#                         peasoup_output_dir, "candidates.peasoup")
#                     os.remove(peasoup_candidate_file)
# =============================================================================
                    os.remove(dm_list_file)
                    meta_info = dict(
                        dmstart=dm_range.low_dm,
                        dmend=dm_range.high_dm,
                        dmstep=dm_range.dm_step,
                    )
                    new_csv_file_name = "overview_dm_{:03f}_{:03f}.xml".format(
                        dm_range.low_dm, dm_range.high_dm)
                    shutil.move(
                        os.path.join(riptide_output_dir, "candidates.csv"),
                        os.path.join(output_dir, new_csv_file_name))
                    log.info("Transferred candidate file to final location")
                    dp = dict(
                        type="riptide_csv",
                        filename=new_csv_file_name,
                        directory=data["base_output_dir"],
                        beam_id=beam["id"],
                        pointing_id=pointing["id"],
                        metainfo=json.dumps(meta_info)
                    )
                    output_dps.append(dp)
            except Exception as error:
                raise error
            finally:
                shutil.rmtree(processing_dir)
    return output_dps


def pipeline(data, status_callback):
    loop = asyncio.get_event_loop()
    output_dps = loop.run_until_complete(
        riptide_pipeline(data, status_callback))
    return output_dps


if __name__ == '__main__':
    parser = optparse.OptionParser()
    mongo_wrapper.add_mongo_consumer_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts, args = parser.parse_args()

    # processor = mongo_wrapper.PikaProcess(...)
    processor = mongo_wrapper.mongo_consumer_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts, pipeline)
    processor.process(pipeline_wrapper.on_receive)
