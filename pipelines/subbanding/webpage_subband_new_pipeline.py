import os
import time
import json
import pika_wrapper
import trapum_pipeline_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
import optparse
from sqlalchemy.pool import NullPool
import subprocess
import logging
from utils import header_util

log = logging.getLogger('peasoup_search_send')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)

SIZE_MARGIN = 200 #bytes for possible header differences

def iqr_filter(merged_file, processing_args, output_dir):  # add tsamp,nchans to processing_args
    iqr_file = output_dir + '/' + \
        os.path.basename(merged_file)[:-4] + '_iqrm.fil'
    samples = int(round(processing_args['window'] / processing_args['tsamp']))
    iqr_script = "iqrm_apollo_cli -m %d -t %.2f -s %d --replacement median -f %d -i %s -o %s" % (
        processing_args['max_lags'], processing_args['threshold'], samples, processing_args['nchans'], merged_file, iqr_file)
    log.info("Script that will run..")
    log.info(iqr_script)
    # time.sleep(5)
    try:
        subprocess.check_call(iqr_script, shell=True)
        log.info("IQR filtering done on %s" % merged_file)

        return iqr_file

    except Exception as error:
        log.info("Error. Cleaning up partial file...")
        subprocess.check_call("rm %s" % iqr_file, shell=True)
        log.error(error)


def subband_fil(merged_file, processing_args):
    subbanded_file = merged_file[:-4] + '_sub.fil'
    subband_script = "ft_scrunch_threads  -d %.2f -t 4 -B 64 --no-refdm -c %d %s %s" % (
        float(processing_args['refdm']), processing_args['fscrunch'], merged_file, subbanded_file)
    log.info("Script that will be run..")
    log.info(subband_script)
    # time.sleep(5)
    try:
        subprocess.check_call(subband_script, shell=True)
        log.info("Successfully subbanded file")
        return subbanded_file
    except Exception as error:
        log.info("Error. Cleaning up partial file...")
        subprocess.check_call("rm %s" % subbanded_file, shell=True)
        log.error(error)


def merge_filterbanks(digifil_script, merged_file):

    try:
        subprocess.check_call(digifil_script, shell=True)
        log.info("Successfully merged")
    except Exception as error:
        log.info("Error. Cleaning up partial file...")
        subprocess.check_call("rm %s" % merged_file, shell=True)
        subprocess.check_call(digifil_script, shell=True)
        log.error(error)


def update_telescope_id(input_file):
    script = "alex_filterbank_change_header -telescope MeerKAT -files \"%s\"" % input_file
    try:
        subprocess.check_call(script, shell=True)
        log.info("Successfully updated")

    except Exception as error:
        log.error(error)


def call_peasoup(peasoup_script):
    log.info('Starting peasoup search..')
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
    filterbank_info = header_util.parseSigprocHeader(input_file)
    filterbank_stats = header_util.updateHeader(filterbank_info)

    return filterbank_stats

# process_manager = PikaProcess(...)
# pipeline_wrapper = TrapumPipelineWrapper(..., null_pipeline)
# process_manager.process(pipeline_wrapper.on_receive)


def subband_pipeline(data):

    output_dps = []

    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory
    '''
    processing_args = data['processing_args']
    output_dir = data['base_output_dir']
    # Make output dir
    try:
        subprocess.check_call("mkdir -p %s" % (output_dir), shell=True)
    except BaseException:
        log.info("Already made subdirectory")
        pass
    processing_id = data['processing_id']

    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:
            dp_list = []
            headers = []
            for dp in (beam["data_products"]):
                dp_list.append(dp["filename"])
                headers.append(get_fil_dict(dp["filename"]))

            total_nsamps = sum([h["nsamples"] for h in headers])
            total_nchans = headers[0]["nchans"]
            nbits = headers[0]["nbits"]
            data_nbytes = total_nsamps * total_nchans * nbits / 8
            header_nbytes = headers[0]["hdrlen"]
            expected_merged_size = data_nbytes + header_nbytes

            # Merge filterbanks
            all_files = ' '.join(dp_list)
            partial_base = os.path.basename(dp_list[0])
            partial_name = partial_base.split(
                '_')[0] + '_' + partial_base.split('_')[1]
            merged_file = "%s/%s_p_id_%d.fil" % (
                output_dir, partial_name, processing_id)
            digifil_script = "digifil %s -b 8 -threads 15 -o %s" % (
                all_files, merged_file)

            if os.path.isfile(merged_file):
                if abs(os.stat(merged_file).st_size - expected_merged_size) < SIZE_MARGIN:
                    log.info("Found merged file that is of the expected size, using that")
                else:
                    log.info("Prior merged file found but of incorrect length, creating new merged file")
                    merge_filterbanks(digifil_script, merged_file)
            else:
                log.info("No prior merged file found")
                merge_filterbanks(digifil_script, merged_file)

            # Get header of merged file
            filterbank_header = get_fil_dict(merged_file)

            # IQR
            processing_args['tsamp'] = float(filterbank_header['tsamp'])
            processing_args['nchans'] = int(filterbank_header['nchans'])

            iqred_file = output_dir + '/' + \
                os.path.basename(merged_file)[:-4] + '_iqrm.fil'
            if os.path.isfile(iqred_file):
                if abs(os.stat(iqred_file).st_size - expected_merged_size) < SIZE_MARGIN:
                    log.info("Found iqred_file file that is of the expected size, using that")
                else:
                    log.info("Prior iqred_file found but of incorrect length, creating new iqred_file file")
                    iqred_file = iqr_filter(merged_file, processing_args, output_dir)
            else:
                log.info("No prior iqred_file file found")
                iqred_file = iqr_filter(merged_file, processing_args, output_dir)

            # Subband the file
            subbanded_file = subband_fil(iqred_file, processing_args)

            # Update telescope id of subbanded file
            update_telescope_id(subbanded_file)

            # Remove tmp files after searching
            tmp_files = []
            tmp_files.extend((merged_file, iqred_file))
            remove_temporary_files(tmp_files)

            # Get header of subbanded file
            subband_header = get_fil_dict(subbanded_file)

            # Args for database updation
            sampling_number = int(round(processing_args['tsamp'] * 1e6))
            ref_dm = processing_args['refdm']
            new_chans = int(
                processing_args['nchans'] /
                processing_args['fscrunch'])

            # Metainfo from fileheader
            meta_info = dict(
                barycentric=0,
                source_name=subband_header['source_name'],
                nbits=subband_header['nbits'],
                nchans=subband_header['nchans'],
                tstart=subband_header['tstart'],
                tsamp=subband_header['tsamp'],
                nsamples=subband_header['nsamples'],
                ra=subband_header['ra'],
                dec=subband_header['dec'],
                refdm=ref_dm
            )

            dp = dict(
                type="filterbank-iqrm-%d-%dus-%ddm" %
                (new_chans,
                 sampling_number,
                 ref_dm),
                filename=os.path.basename(subbanded_file),
                directory=data["base_output_dir"],
                beam_id=beam["id"],
                pointing_id=pointing["id"],
                metainfo=json.dumps(meta_info))

            output_dps.append(dp)

    return output_dps


if __name__ == '__main__':

    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts, args = parser.parse_args()

    # processor = pika_wrapper.PikaProcess(...)
    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts, subband_pipeline)
    processor.process(pipeline_wrapper.on_receive)
