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



log = logging.getLogger('dspsr_folder')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel('INFO')

AI_PATH = '/'.join(ubc_AI.__file__.split('/')[:-1]) + '/trained_AI/'
MODELS = ["clfl2_trapum_Ter5.pkl", "clfl2_PALFA.pkl"]



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



def generate_dspsr_cand_file(tmp_dir, beam_name, cand_mod_periods, cand_dms, cand_accs, ra_coord, dec_coord)

    # Write a header 
    cand_file_path = '%s/%s_cands.txt'%(tmp_dir,beam_name)
    with open(cand_file_path,'a') as f:
        f.write("SOURCE RA DEC PERIOD DM ACC\n")
        for i range(len(cand_mod_periods)):
            f.write("%s %s %s %f %f %f\n"%(beam_name, ra_coord,dec_coord,cand_mod_periods[i],cand_dms[i],cand_accs[i])) 
        f.close()

    return cand_file_path


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


def extract_and_score(path, models):
    # Load model
    classifiers = []
    for model in models:
        with open(os.path.join(AI_PATH, model), "rb") as f:
            classifiers.append(cPickle.load(f))
            log.info("Loaded model {}".format(model))
    # Find all files
    arfiles = glob.glob("{}/*.ar.clfd".format(path))
    log.info("Retrieved {} archive files from {}".format(
        len(arfiles), path))
    scores = []
    readers = [pfdreader(f) for f in arfiles]
    for classifier in classifiers:
        scores.append(classifier.report_score(readers))
    log.info("Scored with all models")
    combined = sorted(zip(arfiles, *scores), reverse=True, key=lambda x: x[1])
    log.info("Sorted scores...")
    names = "\t".join(["#{}".format(model.split("/")[-1]) for model in models])
    with open("{}/pics_scores.txt".format(path)) as fout:
        fout.write("#arfile\t{}\n".format(names))
        for row in combined:
            scores = ",".join(row[1:])
            fout.write("{},{}\n".format(row[0], scores))
    log.info("Written to file in {}".format(path))



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
           print (beam_ID,processing_args['snr_cutoff'])
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

           ra = float(root.find("header_parameters/src_raj").text)
           dec =  float(root.find("header_parameters/src_dej").text)


           ra_coord,dec_coord = convert_to_std_format(ra,dec)
           
           mod_periods=[]
           pdots= []
           for i in range(len(cand_periods)):
               Pdot = a_to_pdot(cand_periods[i],cand_accs[i])
               mod_periods.append(period_modified(cand_periods[i],Pdot,no_of_samples,tsamp,fft_size))
               pdots.append(Pdot) 

           cand_mod_periods = np.asarray(mod_periods,dtype=float)
             

           # Generate a candidate file to parse for DSPSR in -w format
           cand_file = generate_dspsr_cand_file(tmp_dir, beam_name, cand_mod_periods, cand_dms, cand_accs, ra_coord, dec_coord)
           

           # Run DSPSR
           subprocess.check_call("dspsr %s -cpus 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 -k meerkat -t 12 -U 256 -L 20 -Lmin 15 -w %s -O sample"%(input_filenames,cand_file),shell=True,cwd=tmp_dir)

           # Run clfd
 
           subprocess.check_call("clfd --no-report *.ar",shell=True,cwd=tmp_dir)
           subprocess.check_call("rm *.ar",shell=True,cwd=tmp_dir) # Remove original archives ?
          

           # Run pdmp
           for clfd_ar in glob.glob("*.ar.clfd"):
               subprocess.check_call("pdmp -mc 32 -ms 32 -g %s.png/png --no-report %s"%(clfd_ar,clfd_ar),shell=True,cwd=tmp_dir)

            
           log.info("Folding done for all candidates. Scoring all candidates...")
           score_ar_files()

           subprocess.check_call("python2 webpage_score.py --in_path=%s"%tmp_dir,shell=True)
           log.info("Scoring done...")

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
    

