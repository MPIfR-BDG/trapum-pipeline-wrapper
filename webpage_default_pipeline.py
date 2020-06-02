import pika_wrapper
import trapum_pipeline_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
import optparse
from sqlalchemy.pool import NullPool
import subprocess
import logging


log = logging.getLogger('peasoup_search_send')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)


def iqr_filter(merged_file,processing_args): # add tsamp,nchans to processing_args

    iqr_file = "%s_iqr.fil"%merged_file[:-4]
    samples = tsamp*processing_args['window']
    iqr_script = "iqrm_apollo_cli -m %d -t %.2f -s %d -f %d -i %s -o %s"%(processing_args['max_lags'],processing_args['threshold'],samples,nchans,processing_args['merged_file'],processing_args['iqr_file'])
    try:
        subprocess.check_call(iqr_script,shell=True)
        log.info("IQR filtering done on %s"%merged_file)

        return iqr_file

    except Exception as error:
        log.error(error)


def merge_filterbanks(digifil_script):

    try:
        subprocess.check_call(digifil_script,shell=True)
        log.info("Successfully merged")
        return  
    except Exception as error:
        log.error(error)

def subband_fil(merged_file,processing_args):
    subband_script = "ft_scrunch_threads  -d %d -t 16 -B 64 -c %d %s %s"%(processing_args['refdm'],processing_args['fscrunch'])
    log.info("Script that will be run..")
    log.info(subband_script)
    try 


def call_peasoup(peasoup_script):
    log.info('Starting peasoup search..') 
    try:
        subprocess.check_call(peasoup_script,shell=True)
        log.info("Search complete")
        
    except Exception as error:
        log.error(error)

def remove_username(xml_file):
    script = "sed -i \'/username/d\' %s"%xml_file
    try:
        subprocess.check_call(script,shell=True)
    except Exception as error
        log.error(error)

def remove_temporary_files(tmp_files):
    for tmp_file in tmp_files:
        subprocess.check_call("rm %s"%tmp_file,shell=True)




#process_manager = PikaProcess(...)
#pipeline_wrapper = TrapumPipelineWrapper(..., null_pipeline)
#process_manager.process(pipeline_wrapper.on_receive)

def null_pipeline(data):

    output_dps = []

    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory
    '''
    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"][:][]
        
             
            # Processing happens here
            dp = dict(
                 type="peasoup_xml",
                 filename="overview.xml",
                 directory=data["base_output_dir"],
                 beam_id = beam["id"],
                 pointing_id = pointing["id"]
                 )
               
            output_dps.append(dp)

    return output_dps

    

if __name__ == '__main__':

    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts,args = parser.parse_args()

    #processor = pika_wrapper.PikaProcess(...)
    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts,null_pipeline)
    processor.process(pipeline_wrapper.on_receive)

