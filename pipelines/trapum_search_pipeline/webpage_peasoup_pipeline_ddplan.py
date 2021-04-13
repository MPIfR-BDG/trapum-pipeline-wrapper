import optparse
import subprocess
import logging
import parseheader
import os
import numpy as np
import json
from collections import namedtuple
import pika_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper


log = logging.getLogger('peasoup_search')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel('INFO')


def merge_filterbanks(digifil_script, merged_file):
    try:
        subprocess.check_call(digifil_script, shell=True)
        log.info("Successfully merged")
    except Exception as error:
        log.error(error)
        log.info("Error. Cleaning up partial file... and relaunching")
        subprocess.check_call("rm %s" % merged_file, shell=True)
        subprocess.check_call(digifil_script, shell=True)


def iqr_filter(merged_file, processing_args, output_dir):
    iqr_file = output_dir + '/' + \
        os.path.basename(merged_file)[:-4] + '_iqrm.fil'
    samples = int(round(processing_args['window'] / processing_args['tsamp']))
    iqr_script = "iqrm_apollo_cli -m %d -t %.2f --replacement median -s %d -f %d -i %s -o %s" % (
        processing_args['max_lags'], processing_args['threshold'], samples,
        processing_args['nchans'], merged_file, iqr_file)
    log.info("Script that will run..")
    log.info(iqr_script)
    try:
        subprocess.check_call(iqr_script, shell=True)
        log.info("IQR filtering done on %s" % merged_file)
        return iqr_file

    except Exception as error:
        log.info("Error. Cleaning up partial iqr file and input merged file")
        subprocess.check_call("rm %s" % iqr_file, shell=True)
        subprocess.check_call("rm %s" % merged_file, shell=True)
        log.error(error)


def call_peasoup(peasoup_script):
    log.info('Starting peasoup search..')
    log.info("peasoup command that will be run..")
    log.info(peasoup_script)
    try:
        subprocess.check_call(peasoup_script, shell=True)
        log.info("Search complete")

    except Exception as error:
        log.error(error)


def remove_username(xml_file):
    script = "sed -i \'/username/d\' %s" % xml_file
    try:
        subprocess.check_call(script, shell=True)
    except Exception as error:
        log.error(error)


def remove_temporary_files(tmp_files):
    for tmp_file in tmp_files:
        subprocess.check_call("rm %s" % tmp_file, shell=True)


def decide_fft_size(filterbank_header):
    # Decide fft_size from filterbank nsamples
    log.debug("Deciding FFT length...")
    bit_length = int(filterbank_header['nsamples']).bit_length()
    if 2**bit_length != 2 * int(filterbank_header['nsamples']):
        return 2**bit_length
    else:
        return int(filterbank_header['nsamples'])


def get_fil_dict(input_file):
    filterbank_info = parseheader.parseSigprocHeader(input_file)
    filterbank_stats = parseheader.updateHeader(filterbank_info)

    return filterbank_stats


def update_telescope_id(input_file):
    script = "alex_filterbank_change_header -telescope MeerKAT -files \"%s\"" % input_file
    try:
        subprocess.check_call(script, shell=True)
        log.info("Successfully updated")

    except Exception as error:
        log.error(error)


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


def generate_chan_mask(chan_mask_csv, filterbank_header):
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
        # start_chan_mask = int((start_freq_mask - fbottom) *
        #                              nchans / (ftop - fbottom))

        # end_chan_mask = int((end_freq_mask - fbottom) *
        #                    nchans / (ftop - fbottom))

        chbw = (ftop - fbottom) / nchans
        idx0 = int(min(max((rstart - fbottom) // chbw, 0), nchans - 1))
        idx1 = int(max(min(int((rend - fbottom) / chbw + 0.5), nchans - 1), 0))
        # if start_chan_mask < 1:
        #   log.warning("Specified frequency below lower observing frequency bound")
        #   log.info("Re-adjusting to observable bandwidth")
        #   start_chan_mask = 1
        # elif end_chan_mask > nchans:
        #   log.warning("Specified frequency above upper observing frequency bound")
        #   log.info("Re-adjusting to observable bandwidth")
        #   end_chan_mask = nchans
        chan_mask[idx0:idx1 + 1] = 0
    np.savetxt('chan_mask_peasoup', chan_mask, fmt='%d')


def generate_birdie_list(birdie_csv):
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
            'trapum.birdies', np.c_[
                np.array(
                    birdies, dtype=float), np.array(
                    birdies_width, dtype=float)], fmt="%.2f")
    except Exception as error:
        log.error(error)


def get_expected_merge_length(filterbanks):
    return sum([get_fil_dict(fname)['nsamples'] for fname in filterbanks])


DMRange = namedtuple(
    'DMRange',
    ['low_dm', 'high_dm', 'dm_step', 'tscrunch'])


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


class Filterbank(object):
    pass


def peasoup_pipeline(data):

    output_dps = []

    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory
    '''
    processing_args = data['processing_args']
    output_dir = data['base_output_dir']
    # Make output dir
    log.info("Making directory: {}".format(output_dir))
    try:
        subprocess.check_call("mkdir -p %s" % (output_dir), shell=True)
    except Exception as error:
        log.error("Error making subdir {}".format(output_dir))
        raise error
    else:
        log.info("Making directory: {}".format(output_dir))


    # To avoid core dump related interruptions
    try:
        subprocess.check_call("ulimit -c  0", shell=True)
    except Exception as error:
        log.error("ulimit execution failed")
        log.error(error)

    processing_id = data['processing_id']

    try:
        ddplan = DDPlan.from_string(processing_args['ddplan'])
    except Exception as error:
        log.exception("Unable to parse DDPlan")
        raise error

    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:
            dp_list = []
            for dp in (beam["data_products"]):
                dp_list.append(dp["filename"])

            dp_list.sort()
            all_files = ' '.join(dp_list)

            # Check for temporary file storage
            if processing_args['temp_filesystem'] == '/beeond/':
                log.info("Running on Beeond")
                processing_dir = '/beeond/PROCESSING/TEMP/%d' % processing_id
                try:
                    subprocess.check_call(
                        "mkdir -p %s" %
                        (processing_dir), shell=True)
                except BaseException:
                    log.warning(
                        "Subdirectory {} already exists".format(processing_dir))
                    pass

            else:
                log.info("Running on BeeGFS")
                processing_dir = output_dir

            fscrunch = processing_args.get("fscrunch", 1)
            fscrunch_arg = "" if fscrunch == 1 else " -f {} ".format(fscrunch)

            merged_file = "%s/temp_merge_p_id_%d.fil" % (
                processing_dir, processing_id)
            digifil_script = "digifil %s -b 8 -threads 4 -o %s %s" % (
                all_files, merged_file, fscrunch_arg)

            expected_merge_length = get_expected_merge_length(dp_list)
            log.info("Expected merge length: {} samples".format(
                expected_merge_length))
            print(digifil_script)
            merge_filterbanks(digifil_script, merged_file)

            # Get header of merged file
            filterbank_header = get_fil_dict(merged_file)
            if filterbank_header['nsamples'] != expected_merge_length:
                log.error("Merged file has unexpected length of {} samples, expected {} samples".format(
                    filterbank_header['nsamples'], expected_merge_length))
                raise Exception("Incorrect merged file length, failure in digifil processing")

            # IQR
            processing_args['tsamp'] = float(filterbank_header['tsamp'])
            processing_args['nchans'] = int(filterbank_header['nchans'])
            iqred_file = iqr_filter(
                merged_file, processing_args, processing_dir)
            remove_temporary_files([merged_file])
            iqred_header = get_fil_dict(iqred_file)
            if iqred_header['nsamples'] != expected_merge_length:
                log.error("IQRM file has unexpected length of {} samples, expected {} samples".format(
                    iqred_header['nsamples'], expected_merge_length))
                raise Exception("Incorrect merged file length, failure in IQRM processing")

            # Determine fft_size
            if processing_args['fft_length'] == 0:
                fft_size = decide_fft_size(filterbank_header)
            else:
                fft_size = processing_args['fft_length']

            if fft_size > 201326592:
                fft_size = 201326592  # Hard coded for max limit - tmp assuming 4hr, 76 us and 4k chans

            log.info("Chose FFT length of {}".format(fft_size))

            # Determine channel mask to use
            chan_mask_csv = processing_args['channel_mask']
            peasoup_chan_mask = generate_chan_mask(
                chan_mask_csv, filterbank_header)

            # Determine birdie list to use
            birdie_list_csv = processing_args['birdie_list']
            birdie_list = generate_birdie_list(birdie_list_csv)

            # Set RAM limit
            ram_limit = processing_args['ram_limit']

            log.info("Starting loop over DDPlan")
            for dm_range in ddplan:
                log.info(dm_range)
                if dm_range.tscrunch != 1:
                    log.info("Tscrunch != 1: Executing scrunch")
                    base, ext = os.path.splitext(iqred_file)
                    search_file = "{}_t{}{}".format(base, dm_range.tscrunch, ext)
                    digifil_script = "digifil {} -b 8 -threads 4 -o {} -t {}".format(
                        iqred_file, search_file, dm_range.tscrunch)
                    merge_filterbanks(digifil_script, search_file)
                else:
                    search_file = iqred_file
                log.info("Searching file: {}".format(search_file))
                # Generate actual dm list file
                dm_csv = "{}:{}:{}".format(dm_range.low_dm, dm_range.high_dm, dm_range.dm_step)
                dm_list = sorted(list(set(list(slices(dm_csv)))))
                dm_list_name = "p_id_%d_" % processing_id + \
                    "dm_%f_%f" % (dm_list[0], dm_list[-1])
                np.savetxt(dm_list_name, dm_list, fmt='%.3f')

                curr_fft_size = fft_size / dm_range.tscrunch

                subdir = "{}/dm_range_{:03f}_{:03f}".format(
                    processing_dir, dm_range.low_dm, dm_range.high_dm)

                # Initialise peasoup script
                peasoup_script = "peasoup -k chan_mask_peasoup -z trapum.birdies  -i %s --ram_limit_gb %f --dm_file %s --limit %d  -n %d  -m %.2f --acc_start %.2f --acc_end %.2f --fft_size %d -o %s" % (
                    search_file, ram_limit, dm_list_name, processing_args['candidate_limit'],
                    int(processing_args['nharmonics']), processing_args['snr_threshold'],
                    processing_args['start_accel'], processing_args['end_accel'], curr_fft_size,
                    subdir)

                call_peasoup(peasoup_script)

                # Remove merged file after searching
                cand_peasoup = subdir + '/candidates.peasoup'
                tmp_files = []
                if search_file != iqred_file:
                    tmp_files.append(search_file)
                tmp_files.append(cand_peasoup)
                tmp_files.append(dm_list_name)
                remove_temporary_files(tmp_files)

                meta_info = dict(
                    fftsize=curr_fft_size,
                    dmstart=dm_range.low_dm,
                    dmend=dm_range.high_dm,
                    dmstep=dm_range.dm_step,
                )

                new_filename = "overview_dm_{:03f}_{:03f}.xml".format(
                    dm_range.low_dm, dm_range.high_dm)

                # Transfer files to output directory if process ran on Beeond
                if processing_args['temp_filesystem'] == '/beeond/':
                    try:
                        subprocess.check_call(
                            "mv {}/overview.xml {}/{}".format(
                                subdir, output_dir, new_filename),
                            shell=True)
                        log.info("Transferred XML file from Beeond to BeeGFS")
                        os.rmdir(subdir)
                    except Exception as error:
                        log.error(error)

                dp = dict(
                    type="peasoup_xml",
                    filename=new_filename,
                    directory=data["base_output_dir"],
                    beam_id=beam["id"],
                    pointing_id=pointing["id"],
                    metainfo=json.dumps(meta_info)
                )

                output_dps.append(dp)

            remove_temporary_files([iqred_file])
            # Update xml to MongoDB
            # NOTE: Mongo updates currently disabled due to issues with the mongo
            # instance running on APSUSE.
            """
            client = MongoClient(
                'mongodb://{}:{}@10.98.76.190:30003/'.format(
                    os.environ['MONGO_USERNAME'].strip('\n'),
                    os.environ['MONGO_PASSWORD'].strip('\n')))  # Add another secret for MongoDB
            doc = parker.data(
                lxml.etree.fromstring(
                    open(
                        data["base_output_dir"] +
                        "/overview.xml",
                        "rb").read()))
            client.trapum.peasoup_xml_files.update(doc, doc, True)
            """
            if processing_args['temp_filesystem'] == '/beeond/':
                os.rmdir(processing_dir)

    return output_dps


if __name__ == '__main__':

    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts, args = parser.parse_args()

    # processor = pika_wrapper.PikaProcess(...)
    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts, peasoup_pipeline)
    processor.process(pipeline_wrapper.on_receive)
