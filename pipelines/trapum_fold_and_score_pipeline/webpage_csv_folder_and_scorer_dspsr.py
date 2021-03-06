import os
import sys
import time
import json
import glob
import shlex
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



log = logging.getLogger('dspsr_folder')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel('INFO')

#MODELS = ["clfl2_trapum_Ter5.pkl", "clfl2_PALFA.pkl"]



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



def generate_dspsr_cand_file(tmp_dir, beam_name, utc_name, cand_mod_periods, cand_dms, cand_accs, cand_ids, ra_coord, dec_coord):

    cand_file_path = '%s/%s_cands.txt'%(tmp_dir,beam_name)
    source_name_prefix = "%s_%s"%(beam_name,utc_name)
    with open(cand_file_path,'a') as f:
        f.write("SOURCE RA DEC PERIOD DM ACC\n")
        for i in range(len(cand_mod_periods)):
            f.write("%s_candidate_no_%d_dm_%.2f_acc_%.2f_p_%.2f_ms %s %s %f %f %f\n"%(source_name_prefix, cand_ids[i],  ra_coord,dec_coord,cand_mod_periods[i],cand_dms[i],cand_accs[i])) 
        f.close()

    return cand_file_path


def parse_pdmp_stdout(stream):
    for line in stream.splitlines():
        if line.startswith("Best DM"):
            dm = float(line.split()[3])
            break
    else:
        raise Exception("no best DM")
    for line in stream.splitlines():
        if line.startswith("Best TC Period"):
            tc = float(line.split()[5])
            break
    else:
        raise Exception("no best TC period")
    return tc, dm






def convert_to_std_format(ra, dec):
    # Convert hour angle strings to degrees


    ra_deg = int(ra/10000)
    ra_min = int(ra/100) - 100*ra_deg
    ra_sec = ra - 10000*ra_deg - 100*ra_min
       
    dec_deg = int(dec/10000)
    dec_min = int(dec/100) - 100*dec_deg
    dec_sec = dec - 10000*dec_deg - 100*dec_min

    ra_coord = "{}:{}:{:.2f}".format(ra_deg, abs(ra_min), abs(ra_sec))
    #print(ra_coord)
    dec_coord = "{}:{}:{:.2f}".format(dec_deg, abs(dec_min), abs(dec_sec))

    return ra_coord,dec_coord

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
           log.info("Untarring the filtered candidate information to %s"%tmp_dir)  
           untar_file(tarred_csv,tmp_dir)
           tmp_dir = tmp_dir + '/' + os.path.basename(tarred_csv)

           #Read candidate info file into Pandas Dataframe
           log.info("Reading candidate info...") 
           cand_file = glob.glob('%s/*good_cands_to_fold_with_beam.csv'%(tmp_dir))[0]
           df = pd.read_csv(cand_file) 


           # Select only candidates with corresponding beam id and snr cutoff
           log.info("Selecting candidates for  beam ID %d and SNR higher than %f"%(beam_ID, processing_args['snr_cutoff']))  
           snr_cut_cands = df[df['snr'] > float(processing_args['snr_cutoff'])]
           single_beam_cands = snr_cut_cands[snr_cut_cands['beam_id']==beam_ID]
           single_beam_cands.sort_values('snr', inplace=True, ascending=False)  
          

           # If no candidates found in this beam, skip to next message
           if single_beam_cands.shape[0] == 0:
               log.info("No candidates found for this beam. Skipping to next message") # Something better ? 
               dp = dict(
                    type="no_candidate_statement",
                    filename="invalid",
                    directory="invalid",
                    beam_id=beam_ID,
                    pointing_id=pointing["id"],
                    metainfo=json.dumps("no_candidate_for_beam")
                    )
          
               output_dps.append(dp)
               return output_dps
 
           #Limit number of candidates to fold
           log.info("Setting a maximum limit of %d candidates per beam"%(processing_args['cand_limit_per_beam'])) 
           if single_beam_cands.shape[0] > processing_args['cand_limit_per_beam']:
               single_beam_cands_fold_limited = single_beam_cands.head(processing_args['cand_limit_per_beam'])
           else:
               single_beam_cands_fold_limited = single_beam_cands       
            

 
           # Read parameters and fold
           log.info("Reading all necessary candidate parameters") 
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
           ra = float(root.find("header_parameters/src_raj").text)
           dec =  float(root.find("header_parameters/src_dej").text)

           ra_coord,dec_coord = convert_to_std_format(ra,dec)

           log.info("Modifying period to starting epoch reference")
               
           mod_periods=[]
           pdots= []
           for i in range(len(cand_periods)):
               Pdot = a_to_pdot(cand_periods[i],cand_accs[i])
               mod_periods.append(period_modified(cand_periods[i],Pdot,no_of_samples,tsamp,fft_size))
               pdots.append(Pdot) 

           cand_mod_periods = np.asarray(mod_periods,dtype=float)
             

           log.info("Generating predictor file for DSPSR")
           # Generate a candidate file to parse for DSPSR in -w format
           try:
               pred_w_file = generate_dspsr_cand_file(tmp_dir, beam_name, utc_name, cand_mod_periods, cand_dms, cand_accs, cand_ids, ra_coord, dec_coord)
               log.info("Predictor file ready for folding: %s"%(pred_w_file))
           except Exception as error:
               log.error(error)
               log.error("Predictor candidate file generation failed")   
           

           # Run DSPSR
           log.info("DSPSR will be launched with the following command:")
           try:
               script = "dspsr %s -cpu 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 -k meerkat -t 12 -U 256 -L 20 -Lmin 15 -w %s"%(input_filenames, pred_w_file) 
               log.info(dspsr_script)
               subprocess.check_call(dspsr_script,shell=True,cwd=tmp_dir)
               log.info("DSPSR folding successful")

           except Exception as error:
               log.error(error)
               log.error("DSPSR failed")   
                  

           # Run clfd
              
           log.info("CLFD will be run to clean the archives")

           try:
               subprocess.check_call("clfd --no-report *.ar",shell=True,cwd=tmp_dir)
               log.info("CLFD run was successful")    
           except Exception as error:
               log.error(error)
               log.error("CLFD failed")   
          

           # Run pdmp
           log.info("Optimise candidates with PDMP") 
           wd = os.getcwd()
           os.chdir(tmp_dir)    
           try: 
               for clfd_ar in glob.glob("*.ar.clfd"):
                   tc, dm = parse_pdmp_stdout(subprocess.check_output(shlex.split("pdmp -mc 32 -ms 32 -g {}.png/png {}".format(clfd_ar, clfd_ar))))
                   os.system("pam --period {} -d {} -m {}".format(str(tc/1000.0), dm, clfd_ar))
               log.info("PDMP run complete")    

           except Exception as error:
               log.error(error)  

            
           os.chdir(wd)    
           log.info("Folding done for all candidates. Scoring all candidates...")
           subprocess.check_call("python2 webpage_score.py --in_path=%s"%tmp_dir,shell=True)
           log.info("Scoring done...")

           log.info("Deleting CLFD archives...")
           subprocess.check_call("rm *.ar.clfd",shell=True,cwd=tmp_dir) 
           log.info("Done")

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
    

