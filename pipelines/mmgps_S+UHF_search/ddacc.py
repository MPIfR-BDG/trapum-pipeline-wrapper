import optparse
import subprocess
import logging
import parseheader
import os
import glob
import shutil
import asyncio
import math
import json
import numpy as np
from collections import namedtuple
import mongo_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
from ast import literal_eval

BEEOND_TEMP_DIR = "/beeond/PROCESSING/TEMP/"
MAX_FFT_LEN = 201326592

log = logging.getLogger("peasoup_search")
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel("INFO")

DMRange = namedtuple(
    "DMRange",
    ["low_dm", "high_dm", "dm_step", "tscrunch"])

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
        if file.endswith(".fil") or file.endswith(".json"):
            log.warning(
                f"Removing existing file with name {os.path.join(dir, file)}")
            os.remove(os.path.join(dir, file))


def delete_file_if_exists(output_fil):
    if os.path.isfile(output_fil):
        log.warning(f"Removing existing file with name {output_fil}")
        os.remove(output_fil)


def compare_lengths(expected, actual, tolerance=0):
    diff = abs(expected - actual)
    if diff > expected * tolerance:
        log.error(f"Output file has length of {actual} "
                  f"samples, expected {expected} samples")
        raise Exception("Digifil output file length error")


async def filtool(input_fils, output_dir,
                  rootname,
                  ddacc_tree,
                  fscrunch,
                  rfi_flags="zdot",
                  num_threads=2,
                  nbits=8,
                  outmean=128.,
                  outstd=6.,
                  segment_length=2.0,
                  zapping_threshold=4.):
    delete_files_if_exists(output_dir)

    # generate filplan file

    filplan_fname = os.path.join(output_dir, "filplan.json")

    with open(filplan_fname, 'w') as filplan_file:
        plans = []
        for ddacc_branch in ddacc_tree:
            for ddacc_leaf in ddacc_branch:
                plans.append({"time_downsample": ddacc_leaf.tscrunch,
                            "frequency_downsample": 1,
                            "baseline_width": 0.,
                            "dataout_mean": outmean,
                            "dataout_std": outstd,
                            "dataout_nbits": nbits,
                            "rfi_flags": ""})
        json.dump(plans, filplan_file)

    cmd = f"filtool -v -t {num_threads} --zapthre {zapping_threshold} --fd {fscrunch} --filplan {filplan_fname} -l {segment_length} --baseline {0} {0} --fillPatch rand -z {rfi_flags} -o {rootname} -f {' '.join(input_fils)}"

    # run transientx
    try:
        await shell_call(cmd, cwd=output_dir)
    except Exception as error:
        log.error("filtool call failed, cleaning up partial files")
        delete_files_if_exists(output_dir)
        raise error


async def peasoup(input_fil, dm_list, channel_mask, birdie_list,
                  candidate_limit, ram_limit, nharmonics, snr_threshold,
                  start_accel, end_accel, fft_length, out_dir, gulp_size):
    cmd = (f"peasoup -k {channel_mask} -z {birdie_list} "
           f"-i {input_fil} --ram_limit_gb {ram_limit} "
           f"--dm_file {dm_list} --limit {candidate_limit} "
           f"-n {nharmonics}  -m {snr_threshold} --acc_start {start_accel} "
           f"--acc_end {end_accel} --fft_size {fft_length} -o {out_dir} --dedisp_gulp {gulp_size}")
    await shell_call(cmd)


def decide_fft_size(filterbank_headers):
    nsamples = 0
    for filterbank_header in filterbank_headers:
        nsamples += filterbank_header['nsamples']


    return round(math.log2(nsamples))


def get_fil_dict(input_file):
    filterbank_info = parseheader.parseSigprocHeader(input_file)
    filterbank_stats = parseheader.updateHeader(filterbank_info)
    return filterbank_stats


def generate_chan_mask(chan_mask_csv, filterbank_header, outfile):
    ftop = filterbank_header['ftop']
    fbottom = filterbank_header['fbottom']
    nchans = filterbank_header['nchans']
    chan_mask = np.ones(nchans)
    for val in chan_mask_csv.split(','):
        if len(val.split(":")) == 1:
            rstart = float(val)
            rend = float(val)
        elif len(val.split(":")) == 2:
            rstart = float(val.split(":")[0])
            rend = float(val.split(":")[1])
        else:
            log.warning("Could not understand mask entry: {}".format(val))
            continue
        chbw = (ftop - fbottom) / nchans
        idx0 = int(min(max((rstart - fbottom) // chbw, 0), nchans - 1))
        idx1 = int(max(min(int((rend - fbottom) / chbw + 0.5), nchans - 1), 0))
        chan_mask[idx0:idx1 + 1] = 0
    np.savetxt(outfile, chan_mask, fmt='%d')


def generate_birdie_list(birdie_csv, outfile):
    birdies = []
    birdies_width = []
    for val in birdie_csv.split(','):
        try:
            f = val.split(":")[0]
            w = val.split(":")[1]
        except Exception:
            log.warning("Could not parse birdie list entry: {}".format(val))
            continue
        else:
            birdies.append(f)
            birdies_width.append(w)
    try:
        np.savetxt(
            outfile, np.c_[
                np.array(
                    birdies, dtype=float), np.array(
                    birdies_width, dtype=float)], fmt="%.2f")
    except Exception as error:
        log.error(error)


def select_data_products(beam, filter_func=None):
    dp_list = []
    for dp in (beam["data_products"]):
        if filter_func:
            if filter_func(dp["filename"]):
                dp_list.append(dp["filename"])
        else:
            dp_list.append(dp["filename"])
    return dp_list


def dmfile_from_dmrange(ddacc_leaf, outfile):
    # Generate actual dm list file
    dm_csv = f"{ddacc_leaf.low_dm}:{ddacc_leaf.high_dm}:{ddacc_leaf.dm_step}"
    dm_list = sorted(list(set(list(slices(dm_csv)))))
    np.savetxt(outfile, dm_list, fmt='%.3f')
    return outfile


DDACCLeaf = namedtuple("DDACCLeaf", ["low_dm", "high_dm", "dm_step", "tscrunch", "start_acc", "end_accs", "num_harm"]) # Each leaf is a processing chunk

 
class DDACCBranch(object): #Each branch is a segment
    def  __init__(self,name):
        self._leaves = []
        self._name = name

    def add_leaf(self, leaf):
        self._leaves.append(leaf)

    def __iter__(self):
        return iter(sorted(self._leaves, key=lambda x: x['tscrunch']))


    def __str__(self):
        out = []
        for r in self._leaves:
            out.append(str(r))
        return "\n".join(out)     


    def segment(self):
        return self._segment()    

class DDACCTree(object): #Each tree is a particular processing, such as a PSR-WD or DNS processing. 
    def __init__(self, name):
        self._branches = []
        self._name = name

    def add_branch(self, branch):
        self._branches.append(branch)

    
    def name(self):
        return self._name
    

class DDACCForest(object): # A collection of trees

        def __init__(self):
            self._trees = []

        def add_tree(self, tree):
            self._trees.append(tree)
    

        @classmethod
        def from_string(cls, forest):
            inst = cls()

            parsed_forest = literal_eval(forest)

            ddacc_forest = DDACCForest()

            for name, tree in parsed_forest: 

                ddacc_tree = DDACCTree(name)

                for segment, branch in tree:

                    ddacc_branch = DDACCBranch(segment)

                    for leaf in branch:
                        ddacc_leaf = DDACCLeaf(leaf)
                        ddacc_branch.add_leaf(ddacc_leaf)

                    ddacc_tree.add_branch(branch)


                ddacc_forest.add_tree(tree)
            
            return ddacc_forest


async def peasoup_pipeline(data, status_callback):
    # Limit core size to avoid ephemeral-storage overflows
    # in the event of segmentation faults
    await shell_call("ulimit -c  0")

    processing_args = data["processing_args"]
    output_dir = data["base_output_dir"]
    processing_id = data["processing_id"]
    debug_mode = data.get("debug", False)

    log.info(f"Creating output directory: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    logfile = os.path.join(output_dir, "pipeline.log")
    fh = logging.FileHandler(logfile)
    log.addHandler(fh)

    output_dps = []


    try:
        if processing_args["ddaccplan"] and (processing_args["start_acc"] or processing_args["end_acc"]):
            raise ValueError("Processing arguments contain both DDACCPlan and Acceelration ranges")

        log.info("Parsing DDACCPlan")
        ddacc_forest = DDACCForest.from_string(processing_args["ddaccplan"])

    except Exception as error:
        log.exception("Unable to parse DDACCPlan")
        raise error

    if processing_args["temp_filesystem"] == "/beeond/":
        log.info("Running on Beeond")
        processing_dir = os.path.join(
            BEEOND_TEMP_DIR, str(processing_id))
    else:
        log.info("Running on BeeGFS")
        processing_dir = os.path.join(output_dir, "processing/")

    os.makedirs(processing_dir, exist_ok=True)
    fscrunch = processing_args.get("fscrunch", 1)
    rfi_flags = processing_args.get("rfi_flags", "zdot")


    for pointing in data["data"]["pointings"]:

        for beam in pointing["beams"]:

            dps = sorted(select_data_products(
                beam, lambda fname: fname.endswith(".fil")))     

            for ddacc_tree in ddacc_forest:

                for ddacc_branch in ddacc_tree: 

                    try:

                        # First merge the filterbanks and output to processing directory
                        # this is done at whatever the native resolution of the data is
                        log.info("Executing file merge")
                        status_callback(
                            "Merging filterbanks and perform rfi mitigation")

                        await filtool(dps, processing_dir,
                                    f"temp_merge_p_id_{processing_id}",
                                    ddacc_tree,
                                    fscrunch,
                                    rfi_flags=rfi_flags)

                        filterbank_headers = [get_fil_dict(dp) for dp in dps]

                        # Determine fft_size for Peasoup call
                        if processing_args['fft_length'] == 0:
                            fft_size = decide_fft_size(filterbank_headers)
                        else:
                            fft_size = processing_args['fft_length']

                        # Hard coded for max limit - tmp assuming 4hr, 76 us
                        # This is related to the available RAM of GTX 1080Ti GPUs
                        # and the limitations of the 32-bit implementation
                        # of the CUFFT library
                        if fft_size > MAX_FFT_LEN:
                            fft_size = MAX_FFT_LEN
                        log.info(f"Chose base FFT length of {fft_size}")

                        # Determine channel mask to use
                        log.info("Determining channel mask")
                        chan_mask_csv = processing_args["channel_mask"]
                        chan_mask_file = os.path.join(processing_dir, "channel_mask.ascii")
                        generate_chan_mask(
                            chan_mask_csv, filterbank_headers[0], chan_mask_file)

                        # Determine birdie list to use
                        log.info("Determining birdie list")
                        birdie_list_csv = processing_args["birdie_list"]
                        birdie_list_file = os.path.join(processing_dir, "birdie_list.ascii")
                        generate_birdie_list(birdie_list_csv, birdie_list_file)

                        # Set RAM limit
                        ram_limit = processing_args['ram_limit']

                        log.info("Instantiating downsampling manager")

                        log.info("Starting loop over DDPlan")


                        for k, ddacc_leaf in enumerate(ddacc_branch):

                            log.info(f"Processing DM/ACC range: {ddacc_leaf}")

                            search_file = os.path.join(ddacc_leaf, processing_dir, f"temp_merge_p_id_{processing_id}_{ddacc_tree.name}_{ddacc_branch.name+1:02d}_{k+1:02d}.fil")
                            log.info(f"Searching file: {search_file}")

                            dm_list_file = os.path.join(processing_dir, f"dm_list_{ddacc_leaf.low_dm:03f}_{ddacc_leaf.high_dm:03f}_{ddacc_tree.name}_{ddacc_branch.name+1:02d}_{k+1:02d}.ascii")
                            dmfile_from_dmrange(ddacc_leaf, dm_list_file)

                            curr_fft_size = fft_size // ddacc_leaf.tscrunch
                            peasoup_output_dir = os.path.join(
                                processing_dir, f"ddacc_{ddacc_tree.name}_{ddacc_branch.name+1:02d}_{k+1:02d}_{ddacc_leaf.low_dm:03f}_{ddacc_leaf.high_dm:03f}")
                            status_callback(f"Peasoup (DM: {ddacc_leaf.low_dm} - {ddacc_leaf.high_dm})")

                            default_gulpsize = int((2048.0 / (filterbank_headers[0]['nchans'] / fscrunch)) * 1e6)

                            await peasoup(
                                search_file, dm_list_file,
                                chan_mask_file, birdie_list_file,
                                processing_args['candidate_limit'],
                                processing_args['ram_limit'],
                                ddacc_leaf.num_harm,
                                processing_args['snr_threshold'],
                                ddacc_leaf.start_acc,
                                ddacc_leaf.end_acc,
                                int(curr_fft_size),
                                peasoup_output_dir,
                                processing_args.get('gulp_size', default_gulpsize))

                            # We do not keep the candidates.peasoup files as they can be massive
                            peasoup_candidate_file = os.path.join(
                                peasoup_output_dir, "candidates.peasoup")
                            if not debug_mode:
                                os.remove(peasoup_candidate_file)
                                os.remove(dm_list_file)
                            meta_info = dict(
                                fftsize=curr_fft_size,
                                dmstart=ddacc_leaf.low_dm,
                                dmend=ddacc_leaf.high_dm,
                                dmstep=ddacc_leaf.dm_step,
                            )
                            new_xml_file_name = "overview_ddacc_{}_{:02d}_{:02d}_{:03f}_{:03f}.xml".format(ddacc_tree.name, ddacc_branch.name+1, k+1, 
                                ddacc_leaf.low_dm, ddacc_leaf.high_dm)

                            shutil.move(
                                os.path.join(peasoup_output_dir, "overview.xml"),
                                os.path.join(output_dir, new_xml_file_name))
                            log.info("Transferred XML file to final location")

                            dp = dict(
                                type="peasoup_xml",
                                filename=new_xml_file_name,
                                directory=data["base_output_dir"],
                                beam_id=beam["id"],
                                pointing_id=pointing["id"],
                                metainfo=json.dumps(meta_info)
                            )

                            output_dps.append(dp)
                    except Exception as error:
                        raise error
                    finally:
                        if not debug_mode:
                            shutil.rmtree(processing_dir)
    log.removeHandler(fh)
    if not debug_mode:
        os.remove(logfile)
    return output_dps


def pipeline(data, status_callback):
    loop = asyncio.get_event_loop()
    output_dps = loop.run_until_complete(
        peasoup_pipeline(data, status_callback))
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
