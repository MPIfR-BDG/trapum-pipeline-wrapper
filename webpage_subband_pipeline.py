import os
import time
import pika_wrapper
import trapum_pipeline_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
import optparse
from sqlalchemy.pool import NullPool
import subprocess
import logging
import parseheader

log = logging.getLogger('peasoup_search_send')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel("INFO")


def iqr_filter(merged_file,processing_args,output_dir): # add tsamp,nchans to processing_args
    iqr_file = output_dir+'/'+os.path.basename(merged_file)[:-4]+'_iqr.fil'
    samples = int(round(processing_args['window']/processing_args['tsamp']))
    iqr_script = "iqrm_apollo_cli -m %d -t %.2f -s %d -f %d -i %s -o %s"%(processing_args['max_lags'],processing_args['threshold'],samples,processing_args['nchans'],merged_file,iqr_file)
    log.info("Script that will run..")
    log.info(iqr_script)
    #time.sleep(5)
    try:
        subprocess.check_call(iqr_script,shell=True)
        log.info("IQR filtering done on %s"%merged_file)

        return iqr_file

    except Exception as error:
        log.error(error)



def subband_fil(merged_file,processing_args):
    subbanded_file = merged_file[:-4]+'_subbanded.fil'
    subband_script = "ft_scrunch_threads  -d %d -t 16 -B 64 -c %d %s %s"%(processing_args['refdm'],processing_args['fscrunch'],merged_file,subbanded_file)
    log.info("Script that will be run..")
    log.info(subband_script)
    #time.sleep(5)
    try:
        subprocess.check_call(subband_script,shell=True)
        log.info("Successfully subbanded file")
        return subbanded_file  
    except Exception as error:
        log.error(error)


def get_fil_dict(input_file):
    filterbank_info = parseheader.parseSigprocHeader(input_file)
    filterbank_stats = parseheader.updateHeader(filterbank_info)

    return filterbank_stats

def remove_temporary_files(tmp_files):
    for tmp_file in tmp_files:
        subprocess.check_call("rm %s"%tmp_file,shell=True)


#process_manager = PikaProcess(...)
#pipeline_wrapper = TrapumPipelineWrapper(..., null_pipeline)
#process_manager.process(pipeline_wrapper.on_receive)

def subband_pipeline(data):

    output_dps = []

    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory
    '''
    processing_args = data['processing_args']
    output_dir = data['base_output_dir']

    #Make output dir
    try:
        subprocess.check_call("mkdir -p %s"%(output_dir),shell=True)
    except:
        log.info("Already made subdirectory")
        pass

    processing_id = data['processing_id']

    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:
            for dp in beam["data_products"]:
                dp= beam["data_products"][0]
                input_file = dp["filename"]
             
                # Get header of merged file
                filterbank_header = get_fil_dict(input_file)      
                print(filterbank_header) 


                #IQR  
                processing_args['tsamp'] = float(filterbank_header['tsamp']) 
                processing_args['nchans'] = int(filterbank_header['nchans']) 
                iqred_file = iqr_filter(input_file,processing_args,output_dir)

                # Subband the file
                subbanded_file = subband_fil(iqred_file,processing_args)

                # Remove iqr file
                tmp_files=[]
                tmp_files.append(iqred_file)
                remove_temporary_files(tmp_files) 

                  
             
                #Calculate params for filetype
                new_chans = int(processing_args['nchans']/processing_args['fscrunch'])
                sampling_number = int(processing_args['tsamp']*1e6) 
                ref_dm = processing_args['refdm']


                dp = dict(
                     type="filterbank-iqrm-%d-%dus-%ddm"%(new_chans,sampling_number,ref_dm),
                     filename=os.path.basename(subbanded_file),
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
    pipeline_wrapper = TrapumPipelineWrapper(opts,subband_pipeline)
    processor.process(pipeline_wrapper.on_receive)

