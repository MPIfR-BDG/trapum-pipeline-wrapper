import pika_wrapper
import trapum_pipeline_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
import optparse
from sqlalchemy.pool import NullPool
import subprocess
import logging
import parseheader
import lxml
from pymongo import MongoClient
from xmljson import parker
import os




log = logging.getLogger('peasoup_search_send')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)


def merge_filterbanks(digifil_script,merged_file):

    try:
        subprocess.check_call(digifil_script,shell=True)
        log.info("Successfully merged")
    except Exception as error:
        log.info("Error. Cleaning up partial file...")
        subprocess.check_call("rm %s"%merged_file,shell=True) 
        log.error(error)
         

def call_peasoup(peasoup_script):
    log.info('Starting peasoup search..') 
    try:
        subprocess.check_call(peasoup_script,shell=True)
        log.info("Search complete")
        
    except Exception as error:
        #log.info("Error, Cleaning up partial file...")
        #subprocess.check_call("rm %s"%xml_file,shell=True)   
        log.error(error)

def remove_username(xml_file):
    script = "sed -i \'/username/d\' %s"%xml_file
    try:
        subprocess.check_call(script,shell=True)
    except Exception as error:
        log.error(error)

def remove_temporary_files(tmp_files):
    for tmp_file in tmp_files:
        subprocess.check_call("rm %s"%tmp_file,shell=True)


def decide_fft_size(filterbank_header):
    #Decide fft_size from filterbank nsamples
    log.debug("Deciding FFT length...")
    bit_length = int(filterbank_header['nsamples']).bit_length()
    if 2**bit_length!=2*int(filterbank_header['nsamples']):
        return 2**bit_length
    else:
        return int(filterbank_header['nsamples'])


def get_fil_dict(input_file):
    filterbank_info = parseheader.parseSigprocHeader(input_file)
    filterbank_stats = parseheader.updateHeader(filterbank_info)

    return filterbank_stats

def update_telescope_id(input_file):
    script = "alex_filterbank_change_header -telescope MeerKAT -files \"%s\""%input_file    
    try:
        subprocess.check_call(script,shell=True)
        log.info("Successfully updated")

    except Exception as error:
        log.error(error)





#process_manager = PikaProcess(...)
#pipeline_wrapper = TrapumPipelineWrapper(..., null_pipeline)
#process_manager.process(pipeline_wrapper.on_receive)

def peasoup_pipeline(data):

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
            dp_list=[] 
            for dp in  (beam["data_products"]):
                dp_list.append(dp["filename"])
 
           
            if len(dp_list) > 1: 
                # Merge filterbanks
                merged = 1
                all_files = ' '.join(dp_list)   
                merged_file = "%s/temp_merge_p_id_%d.fil"%(output_dir,processing_id) 
                digifil_script = "digifil %s -b 8 -threads 15 -o %s"%(all_files,merged_file)
                merge_filterbanks(digifil_script,merged_file)
            else:
                merged = 0
                merged_file = dp_list[0]  

            # Get header of merged file
            filterbank_header = get_fil_dict(merged_file)       

            # Determine fft_size
            fft_size = decide_fft_size(filterbank_header)



            # Run peasoup
            peasoup_script = "peasoup -k Ter5_16apr_rfifind.badchan_peasoup -z trapum_latest.birdies  -i %s --dm_start %.2f --dm_end %.2f --limit %d  -n %d  -m %.2f  --acc_start %.2f --acc_end %.2f  --fft_size %d -o %s"%(merged_file,processing_args['min_dm'],processing_args['max_dm'],processing_args['candidate_limit'],int(processing_args['nharmonics']),processing_args['snr_threshold'],processing_args['start_accel'],processing_args['end_accel'],fft_size,output_dir)
            call_peasoup(peasoup_script)

            # Remove merged file after searching
            cand_peasoup = data["base_output_dir"]+'/candidates.peasoup'
            tmp_files=[]
            if merged:
                tmp_files.append(merged_file)
            tmp_files.append(cand_peasoup)
            remove_temporary_files(tmp_files) 
            
             
            dp = dict(
                 type="peasoup_xml",
                 filename="overview.xml",
                 directory=data["base_output_dir"],
                 beam_id = beam["id"],
                 pointing_id = pointing["id"]
                 )
               
            output_dps.append(dp)


            # Update xml to MongoDB 
            client = MongoClient('mongodb://{}:{}@10.98.76.190:30003/'.format(os.environ['MONGO_USERNAME'], os.environ['MONGO_PASSWORD'])) # Add another secret for MongoDB
            doc = parker.data(lxml.etree.fromstring(open(data["base_output_dir"]+"/overview.xml", "rb").read()))
            client.trapum.peasoup_xml_files.update(doc, doc, True)

    return output_dps

    

if __name__ == '__main__':

    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts,args = parser.parse_args()

    #processor = pika_wrapper.PikaProcess(...)
    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts,peasoup_pipeline)
    processor.process(pipeline_wrapper.on_receive)

