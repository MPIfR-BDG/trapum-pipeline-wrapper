import os
import sys
import time
import json
import glob
import shutil
import tarfile
import optparse
import subprocess
import logging
import pika_wrapper
import multiprocessing
from multiprocessing.pool import ThreadPool
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
from trapum_pipeline_wrapper import TrapumPipelineWrapper



log = logging.getLogger('manual_presto_fold')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel('INFO')


def make_tarfile(output_path,input_path,name):
    with tarfile.open(output_path+'/'+name, "w:gz") as tar:
        tar.add(input_path, arcname= name)

def a_to_pdot(P_s, acc_ms2):
    LIGHT_SPEED = 2.99792458e8                 # Speed of Light in SI
    return P_s * acc_ms2 /LIGHT_SPEED


def period_modified(p0,pdot,no_of_samples,tsamp,fft_size):
    if (fft_size==0.0):
        return p0 - pdot*float(1<<(no_of_samples.bit_length()-1))*tsamp/2
    else:
        return p0 - pdot*float(fft_size)*tsamp/2

def remove_dir(dir_name):
    if 'TEMP' in dir_name:
        shutil.rmtree(dir_name)
    else:
        log.error("Directory not deleted. Not a temporary folder!")


def untar_file(tar_file,tmp_dir):
    try:
        subprocess.check_call("tar -zxvf %s -C %s"%(tar_file,tmp_dir),shell=True)
    except Exception as error:
        log.error(error)
        raise error


def execute_command(command,output_dir):
    subprocess.check_call(command,shell=True,cwd=output_dir)



def fold_and_score_pipeline(data):
    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory

    '''
    tstart = time.time()
    output_dps = []
    dp_list=[]

    processing_args = data['processing_args']
    output_dir = data['base_output_dir']
    processing_id = data['processing_id']

    #Make output dir
    try:
        subprocess.check_call("mkdir -p %s"%(output_dir),shell=True)
    except:
        log.info("Already made subdirectory")
        pass


    # Make temporary folder to keep any temporary outputs
    tmp_dir = '/beeond/PROCESSING/TEMP/%d'%processing_id
    try:
        subprocess.check_call("mkdir -p %s"%(tmp_dir),shell=True)
    except:
        log.info("Already made subdirectory")
        pass


    # Get the beam info
    for pointing in data["data"]["pointings"]:
       utc_start = pointing['utc_start']   
       for beam in pointing["beams"]:
           input_fil_list=[]                   
           for dp in  (beam["data_products"]):
               if '.fil' in dp["filename"]:
                   input_fil_list.append(dp["filename"])  
               elif '.tar.gz' in dp['filename']:
                   tarred_csv = dp["filename"]
               beam_ID = int(beam["id"]) 
               beam_name = beam["name"] 

           input_fil_list.sort()             
           input_filenames = ' '.join(input_fil_list) 
          
           # Untar csv file
           untar_file(tarred_csv,tmp_dir)
           tmp_dir = tmp_dir + '/' + os.path.basename(tarred_csv)

           #Read candidate info file into Pandas Dataframe
           cand_file = glob.glob('%s/*good_cands_to_fold_with_beam.csv'%(tmp_dir))[0]
           df = pd.read_csv(cand_file) 


           # Select only candidates with corresponding beam id and snr cutoff
           snr_cut_cands = df[df['snr'] > float(processing_args['snr_cutoff'])]
           single_beam_cands = snr_cut_cands[snr_cut_cands['beam_id']==beam_ID]
           single_beam_cands.sort_values('snr', inplace=True, ascending=False)  
          

           #Limit number of candidates to fold
           if single_beam_cands.shape[0] > processing_args['cand_limit_per_beam']:
               single_beam_cands_fold_limited = single_beam_cands.head(processing_args['cand_limit_per_beam'])
           else:
               single_beam_cands_fold_limited = single_beam_cands       
            


           # Read parameters and fold
           cand_periods = single_beam_cands_fold_limited['period'].to_numpy()
           cand_accs = single_beam_cands_fold_limited['acc'].to_numpy()
           cand_dms = single_beam_cands_fold_limited['dm'].to_numpy()
           cand_ids = single_beam_cands_fold_limited['cand_id_in_file'].to_numpy()
           xml_files = single_beam_cands_fold_limited['file'].to_numpy() # Choose first element. If filtered right, there should be just one xml filename throughout!

           tree = ET.parse(xml_files[0])
           root = tree.getroot()
  
           tsamp = float(root.find("header_parameters/tsamp").text)
           fft_size = float(root.find('search_parameters/size').text)
           no_of_samples = int(root.find("header_parameters/nsamples").text)
  
           mod_periods=[]
           pdots= []
           for i in range(len(cand_periods)):
               Pdot = a_to_pdot(cand_periods[i],cand_accs[i])
               mod_periods.append(period_modified(cand_periods[i],Pdot,no_of_samples,tsamp,fft_size))
               pdots.append(Pdot) 

           cand_mod_periods = np.asarray(mod_periods,dtype=float)
           mask_path = '/beegfs/PROCESSING/TRAPUM/RFIFIND_masks/Fermi_409chans_mask/Fermi_beam0_052838_20200704_rfifind.mask' 

           #Parallel process the folds
           no_of_cands = len(cand_mod_periods) 


           command_list=[]
           for i in range(no_of_cands):
               folding_packet={}
               folding_packet['period'] = cand_mod_periods[i]
               folding_packet['acc'] = cand_accs[i]
               folding_packet['pdot'] = pdots[i]
               folding_packet['dm'] = cand_dms[i]
               output_name= "%s_%s_candidate_no_%03d_dm_%.2f_acc_%.2f"%(beam_name,utc_start,cand_ids[i],folding_packet['dm'],folding_packet['acc'])
               script = "prepfold -ncpus 1 -nsub 256 -mask %s -noxwin -topo -p %s -pd %s -dm %s %s -o %s"%(mask_path,str(folding_packet['period']),str(folding_packet['pdot']),str(folding_packet['dm']),input_filenames,output_name)
               command_list.append(script) 
                   

           pool = ThreadPool(multiprocessing.cpu_count()) 
           for command in command_list:
               pool.apply_async(execute_command,args=(command,tmp_dir))
           
           pool.close()
           pool.join()    
           
           log.info("Folding done for all candidates. Scoring all candidates...")
           subprocess.check_call("python2 webpage_score.py --in_path=%s"%tmp_dir,shell=True)
           log.info("Scoring done...")


           sys.exit(0)

           #Create tar file of tmp directory in output directory 
           subprocess.check_call("rm *.csv",shell=True,cwd=tmp_dir) # Remove the csv files
           log.info("Tarring up all folds and the score file")
           tar_name = os.path.basename(output_dir) + "folds_and_scores.tar.gz"         
           make_tarfile(output_dir,tmp_dir,tar_name) 
           log.info("Tarred")

           # Remove contents in temporary directory
           remove_dir(tmp_dir)
           log.info("Removed temporary files")


           # Add tar file to dataproduct
           dp = dict(
                type="fold_tar_file",
                filename=tar_name,
                directory=output_dir,
                beam_id=beam_ID,
                pointing_id=pointing["id"],
                metainfo=json.dumps("tar_file:folded_archives")
                )
          
           output_dps.append(dp)

    tend = time.time()
    print ("Time taken is : %f s"%(tend-tstart))
    return output_dps
    



if __name__=="__main__":

    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts,args = parser.parse_args()

    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts,fold_and_score_pipeline)
    processor.process(pipeline_wrapper.on_receive)

    
    get_params_from_csv_and_fold(opts)
    

