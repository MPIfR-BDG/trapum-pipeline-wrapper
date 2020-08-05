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
import math
import numpy as np
import time
import json


log = logging.getLogger('peasoup_search')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel('INFO')

def merge_filterbanks(digifil_script,merged_file):

    try:
        subprocess.check_call(digifil_script,shell=True)
        log.info("Successfully merged")
    except Exception as error:
        log.error(error)
        log.info("Error. Cleaning up partial file... and relaunching")
        subprocess.check_call("rm %s"%merged_file,shell=True)
        subprocess.check_call(digifil_script,shell=True) 

def iqr_filter(merged_file,processing_args,output_dir): # add tsamp,nchans to processing_args
    iqr_file = output_dir+'/'+os.path.basename(merged_file)[:-4]+'_iqrm.fil'
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
        log.info("Error. Cleaning up partial file...")
        subprocess.check_call("rm %s"%iqr_file,shell=True) 
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


def peasoup_pipeline(data):
    f = open('sample_message.txt','w')
    f.write(str(data))
    f.close() 
    
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

    # To avoid core dump related interruptions
    try:
        process = subprocess.Popen(["ulimit", "-c", "0"])
    except Exception as error:
        log.error("ulimit execution failed")
        log.error(error) 

    processing_id = data['processing_id']

    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:
            dp_list=[] 
            for dp in  (beam["data_products"]):
                dp_list.append(dp["filename"])
 
            dp_list.sort()           
            merged = 1
            all_files = ' '.join(dp_list)   
            merged_file = "%s/temp_merge_p_id_%d.fil"%(output_dir,processing_id) 
            digifil_script = "digifil %s -b 8 -threads 15 -o %s"%(all_files,merged_file)
            print(digifil_script)
            time.sleep(2)
            merge_filterbanks(digifil_script,merged_file)

            #else:
            #    merged = 0
            #    merged_file = dp_list[0] 
               
 
            # Get header of merged file
            filterbank_header = get_fil_dict(merged_file)       

            #IQR  
            processing_args['tsamp'] = float(filterbank_header['tsamp']) 
            processing_args['nchans'] = int(filterbank_header['nchans']) 
            #if merged:  
            iqred_file = iqr_filter(merged_file,processing_args,output_dir)
            #else:
            #    iqred_file = merged_file

           
            # Determine fft_size
            #if merged:  
            fft_size = decide_fft_size(filterbank_header)
            if fft_size > 134217728:
                fft_size = 134217728 # Hard coded for max limit - tmp assuming 4hr, 76 us and 4k chans

            # Determine channel mask to use
            if processing_args['nchans'] == 4096:
                chan_mask = "Ter5_4096chans_mask_rfifind.badchan_peasoup"
            else:
                chan_mask = "256_chan_mask_rfifind.badchan_peasoup" 
             


            # DM split if needed  
            dm_list = processing_args['dm_list'].split(",")

           

            log.info("Searching full DM range... ")
            dm_list = processing_args['dm_list'].split(",")
            dm_list_float = [round(float(dm), 3) for dm in dm_list]
            dm_list_name = "p_id_%d_"%processing_id + "dm_%f_%f"%(dm_list_float[0],dm_list_float[-1]) 
            np.savetxt(dm_list_name,dm_list_float,fmt='%.3f')
            
            gulp_limit = 500  
             
            if len(dm_list) > gulp_limit:
                peasoup_script = "peasoup -k %s -z trapum_latest.birdies  -i %s --dm_file %s --ndm_trial_gulp %d --limit %d  -n %d  -m %.2f  --acc_start %.2f --acc_end %.2f  --fft_size %d -o %s"%(chan_mask,iqred_file,dm_list_name,gulp_limit,processing_args['candidate_limit'],int(processing_args['nharmonics']),processing_args['snr_threshold'],processing_args['start_accel'],processing_args['end_accel'],fft_size,output_dir)
                call_peasoup(peasoup_script)
            else:
                peasoup_script = "peasoup -k %s -z trapum_latest.birdies  -i %s --dm_file %s --limit %d  -n %d  -m %.2f  --acc_start %.2f --acc_end %.2f  --fft_size %d -o %s"%(chan_mask,iqred_file,dm_list_name,processing_args['candidate_limit'],int(processing_args['nharmonics']),processing_args['snr_threshold'],processing_args['start_accel'],processing_args['end_accel'],fft_size,output_dir)
 
                call_peasoup(peasoup_script)


            # Remove merged file after searching
            cand_peasoup = data["base_output_dir"]+'/candidates.peasoup'
            tmp_files=[]
            if 'temp_merge' in merged_file:
                tmp_files.append(merged_file)
                tmp_files.append(iqred_file)
            tmp_files.append(cand_peasoup)
            tmp_files.append(dm_list_name)
            remove_temporary_files(tmp_files)

            meta_info=dict(
                            fftsize= fft_size,
                            dmstart = dm_list[0],
                            dmend = dm_list[-1],
                            dmstep = float(dm_list[1]) - float(dm_list[0]),
                            dmgulp = gulp_limit 
                           )  
            
            dp = dict(
                 type="peasoup_xml",
                 filename="overview.xml",
                 directory=data["base_output_dir"],
                 beam_id = beam["id"],
                 pointing_id = pointing["id"],
                 metainfo=json.dumps(meta_info)
                 )
            
            output_dps.append(dp)


            # Update xml to MongoDB 
            client = MongoClient('mongodb://{}:{}@10.98.76.190:30003/'.format(os.environ['MONGO_USERNAME'].strip('\n'), os.environ['MONGO_PASSWORD'].strip('\n'))) # Add another secret for MongoDB
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

