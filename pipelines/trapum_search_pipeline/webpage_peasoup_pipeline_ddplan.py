import optparse
import subprocess
import logging
import parseheader
import os
import glob
import shutil
import asyncio
import json
import numpy as np
from collections import namedtuple
import mongo_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper

BEEOND_TEMP_DIR = "/beeond/PROCESSING/TEMP/"
MAX_FFT_LEN = 201326592

log = logging.getLogger("peasoup_search")
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel("INFO")

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
            low_dm, high_dm, dm_step, tscrunch = list(map(float, line.split()[:4]))
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


async def shell_call(cmd):
    log.info(f"Shell call: {cmd}")
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    retcode = await proc.wait()
    if retcode != 0:
        raise subprocess.CalledProcessError(f"Process return-code {retcode}")


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


async def digifil(input_fils, output_fil, tscrunch=1,
                  fscrunch=1, nbits=8, nthreads=4):
    delete_file_if_exists(output_fil)
    expected_nsamps = sum([get_fil_dict(fname)["nsamples"] for fname in input_fils])
    expected_nsamps = expected_nsamps / tscrunch / fscrunch
    cmd = (f"digifil {' '.join(input_fils)} -b {nbits} -threads {nthreads} "
           f"-o {output_fil} -f {fscrunch} -t {tscrunch} ")
    try:
        await shell_call(cmd)
    except Exception as error:
        log.error("Digifil call failed, cleaning up partial files")
        delete_file_if_exists(output_fil)
        raise error
    output_nsamps = get_fil_dict(output_fil)["nsamples"]
    compare_lengths(expected_nsamps, output_nsamps)


async def iqrm(input_fil, output_fil, max_lags, threshold, samples, nchans):
    delete_file_if_exists(output_fil)
    expected_nsamps = get_fil_dict(input_fil)["nsamples"]
    cmd = (f"iqrm_apollo_cli -m {max_lags} -t {threshold} "
           f"--replacement median -s {samples} -f {nchans} "
           f"-i {input_fil} -o {output_fil}")
    try:
        await shell_call(cmd)
    except Exception as error:
        log.error("IQRM call failed, cleaning up partial files")
        delete_file_if_exists(output_fil)
        raise error
    await shell_call(cmd)
    output_nsamps = get_fil_dict(output_fil)["nsamples"]
    compare_lengths(expected_nsamps, output_nsamps)


async def peasoup(input_fil, dm_list, channel_mask, birdie_list,
                  candidate_limit, ram_limit, nharmonics, snr_threshold,
                  start_accel, end_accel, fft_length, out_dir):
    cmd = (f"peasoup -k {channel_mask_file} -z {birdie_list_file} "
           f"-i {input_fil} --ram_limit_gb {ram_limit} "
           f"--dm_file {dm_list_file} --limit {candidate_limit} "
           f"-n {nharmonics}  -m {snr_threshold} --acc_start {acc_start} "
           f"--acc_end {acc_end} --fft_size {fft_length} -o {out_dir}")
    await shell_call(cmd)


def decide_fft_size(filterbank_header):
    bit_length = int(filterbank_header['nsamples']).bit_length()
    if 2**bit_length != 2 * int(filterbank_header['nsamples']):
        return 2**bit_length
    else:
        return int(filterbank_header['nsamples'])


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


class DownsamplingManager(object):
    def __init__(self, full_res_file, tscrunches):
        self._original = full_res_file
        self._downsampling_tasks = {}
        self._downsamplings = {1: self._original}
        log.info("Preparing downsamplings for values {}".format(
            ", ".join(tscrunches)))
        self._prepare(tscrunches)

    async def _downsample(self, tscrunch, depends, outfile):
        if depends is None:
            input_fil = self._original
        else:
            input_fil = await depends
        await digifil([input_fil], outfile, tscrunch=tscrunch)
        return outfile

    def _prepare(self, tscrunches):
        tscrunches = sorted(list(set(tscrunches)))
        prev_tscrunch = None
        prev_task = None
        for tscrunch in tscrunches:
            fname = self._original.replace(".fil", f"_t{tscrunch}.fil")
            if tscrunch == 1:
                continue
            if prev_tscrunch is None:
                # in this case we have to downsample the original file
                task = asyncio.ensure_future(self._downsample(tscrunch, None, fname))
            else:
                task = asyncio.ensure_future(self._downsample(tscrunch/prev_tscrunch, prev_task, fname))
            self._downsampling_tasks[tscrunch] = task
            prev_tscrunch = tscrunch
            prev_task = task
        self._prepared = True

    async def get_downsampling(self, tscrunch):
        log.info(f"Downsampling of {tscrunch} requested")
        assert self._prepared
        assert (tscrunch == 1) or (tscrunch in self._downsampling_tasks)
        if tscrunch in self._downsamplings:
            return self._downsamplings[tscrunch]
        else:
            log.info(f"Awaiting downsampling result")
            outfile = await self._downsampling_tasks[tscrunch]
            self._downsamplings[tscrunch] = outfile
            log.info(f"Retrieved downsampled file {outfile}")
        return outfile


def dmfile_from_dmrange(dm_range, outfile):
    # Generate actual dm list file
    dm_csv = f"{dm_range.low_dm}:{dm_range.high_dm}:{dm_range.dm_step}"
    dm_list = sorted(list(set(list(slices(dm_csv)))))
    np.savetxt(outfile, dm_list, fmt='%.3f')
    return outfile


async def peasoup_pipeline(data, status_callback):
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
                processing_dir = os.path.join(BEEOND_TEMP_DIR, str(processing_id))
            else:
                log.info("Running on BeeGFS")
                processing_dir = os.path.join(output_dir, "processing/")
            os.makedirs(processing_dir, exist_ok=True)

            # First merge the filterbanks and output to processing directory
            # this is done at whatever the native resolution of the data is
            log.info("Executing file merge")
            fscrunch = processing_args.get("fscrunch", 1)
            merged_file = os.path.join(processing_dir, f"temp_merge_p_id_{processing_id}")
            status_callback("Merging filterbanks")
            await digifil(dps, merged_file, fscrunch=fscrunch)
            merged_header = get_fil_dict(merged_file)

            # Next run the IQRM algorithm on the merged fail
            log.info("Performing IQRM cleaning")
            merged_tsamp = float(merged_header['tsamp'])
            iqrm_file = merged_file.replace(".fil", "_iqrm.fil")
            iqrm_window = processing_args['window']
            iqrm_samples = int(round(processing_args['window'] / merged_tsamp))
            status_callback("IQRM cleaning")
            await iqrm(
                merged_file, iqrm_file,
                processing_args['max_lags'], processing_args['threshold'],
                iqrm_samples, int(merged_header['nchans']))

            # Clean up the merged file which is no longer required
            os.remove(merged_file)

            # Determine fft_size for Peasoup call
            if processing_args['fft_length'] == 0:
                fft_size = decide_fft_size(merged_header)
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
            chan_mask_file = "channel_mask.ascii"
            generate_chan_mask(chan_mask_csv, filterbank_header, chan_mask_file)

            # Determine birdie list to use
            og.info("Determining birdie list")
            birdie_list_csv = processing_args["birdie_list"]
            birdie_list_file = "birdie_list.ascii"
            generate_birdie_list(birdie_list_csv, birdie_list_file)

            # Set RAM limit
            ram_limit = processing_args['ram_limit']

            log.info("Instantiating downsampling manager")
            tscrunches = [dm_range.tscrunch for dm_range in ddplan]
            downsampling_manager = DownsamplingManager(iqrm_file, tscrunches)

            log.info("Starting loop over DDPlan")
            for dm_range in ddplan:
                log.info(f"Processing DM range: {dm_range}")
                search_file = await downsampling_manager.get_downsampling(dm_range.tscrunch)
                log.info(f"Searching file: {search_file}")
                dm_list_file = "dm_list.ascii"
                dmfile_from_dmrange(dm_range, dm_list_file)

                curr_fft_size = fft_size / dm_range.tscrunch
                peasoup_output_dir = os.path.join(
                    processing_dir,
                    f"dm_range_{dm_range.low_dm:03f}_{dm_range.high_dm:03f}")
                status_callback(f"Peasoup (DM: {dm_range.low_dm} - {dm_range.high_dm})")
                await peasoup(
                    search_file, dm_list_file,
                    chan_mask_file, birdie_list_file,
                    processing_args['candidate_limit'],
                    processing_args['ram_limit'],
                    int(processing_args['nharmonics']),
                    processing_args['snr_threshold'],
                    processing_args['start_accel'],
                    processing_args['end_accel'],
                    curr_fft_size,
                    peasoup_output_dir)

                # We do not keep the candidates.peasoup files as they can be massive
                peasoup_candidate_file = os.path.join(
                    peasoup_output_dir, "candidates.peasoup")
                os.remove(peasoup_candidate_file)
                os.remove(dm_list_file)
                meta_info = dict(
                    fftsize=curr_fft_size,
                    dmstart=dm_range.low_dm,
                    dmend=dm_range.high_dm,
                    dmstep=dm_range.dm_step,
                )
                new_xml_file_name = "overview_dm_{:03f}_{:03f}.xml".format(
                    dm_range.low_dm, dm_range.high_dm)
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
            shutil.rmtree(processing_dir)
    return output_dps


def pipeline(data, status_callback):
    loop = asyncio.new_event_loop()
    output_dps = loop.run_until_complete(
        peasoup_pipeline(data, status_callback))
    loop.close()
    return output_dps


if __name__ == '__main__':
    parser = optparse.OptionParser()
    mongo_wrapper.add_mongo_consumer_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts, args = parser.parse_args()

    # processor = mongo_wrapper.PikaProcess(...)
    processor = mongo_wrapper.mongo_consumer_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts, peasoup_pipeline)
    processor.process(pipeline_wrapper.on_receive)
