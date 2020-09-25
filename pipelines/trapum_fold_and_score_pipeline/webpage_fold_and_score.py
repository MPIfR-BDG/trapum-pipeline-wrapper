import os
import glob
import sys
import time
import json
import logging
from datetime import datetime
import numpy as np
import re
import xml.etree.ElementTree as ET
import subprocess
import optparse
from optparse import OptionParser
import trapum_pipeline_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
import pika_wrapper
import optparse
from sqlalchemy.pool import NullPool
import subprocess
import logging
from utils import header_util


log = logging.getLogger('webpage_presto_fold')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel('INFO')


def period_modified(p0,pdot,no_of_samples,tsamp,fft_size):
    if (fft_size==0.0):
        return p0 - pdot*float(1<<(no_of_samples.bit_length()-1))*tsamp/2
    else:
        return p0 - pdot*float(fft_size)*tsamp/2

def a_to_pdot(P_s, acc_ms2):
    LIGHT_SPEED = 2.99792458e8                 # Speed of Light in SI
    return P_s * acc_ms2 /LIGHT_SPEED


def middle_epoch(epoch_start, no_of_samples, tsamp):
     return epoch_start +0.5*no_of_samples*tsamp 

def make_tarfile(output_path,input_path,name):
    with tarfile.open(output_path+'/'+name, "w:gz") as tar:
        tar.add(input_path, arcname= name)

def fold_and_score_pipeline(data):
    with open('sample_message.txt','a') as f:
        f.write(data)
    f.close()

    output_dps = []
    dp_list=[]

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

    xml_files = []
    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:
            for dp in  (beam["data_products"]):      
                if 'xml' in dp['filename']:
                    #xml_file = dp['filename']
                    xml_files.append(dp['filename'])
                else:
                    dp_list.append(dp["filename"])

            # Sort the filterbanks for folding
            dp_list.sort()

            if len(xml_files) > 1:
                for xml_file in xml_files:
                    tar_name, output_path = extract_fold_and_score(processing_args,processing_id,output_dir,xml_file,dp_list)
                    dp = dict(
                    type="candidate_tar_file",
                    filename=tar_name,
                    directory=output_path,
                    beam_id=beam["id"],
                    pointing_id=pointing["id"],
                    metainfo=json.dumps("tar_file:folds+scored")
                     ) 
                    output_dps.append(dp)
            else:
                tar_name, output_path = extract_fold_and_score(processing_args,processing_id,output_dir,xml_files[0],dp_list)
                dp = dict(
                type="candidate_tar_file",
                filename=tar_name,
                directory=output_path,
                beam_id=beam["id"],
                pointing_id=pointing["id"],
                metainfo=json.dumps("tar_file:folds+scored")
                 )
                output_dps.append(dp)
       

    return output_dps
                     
def extract_fold_and_score(processing_args,processing_id,output_dir,xml_file,dp_list):
    xml={}

    tree = ET.parse(xml_file)
    root = tree.getroot()

    #initiate empty arrays
    mod_period=[]
    period=[]
    acc=[]
    pdot=[]
    dm=[]
    snr=[]

    #datetime parameters
    xml['datetime'] = root.find('misc_info/utc_datetime').text.replace(":","-")
    #Header Parameters
    xml['ra'] = root.find('header_parameters/src_raj').text
    xml['dec'] = root.find('header_parameters/src_dej').text
    source_name = root.find('header_parameters/source_name').text
    xml['source_name'] = source_name.replace(" ","").replace(":","").replace(",","")
    #raw_data_filename=root.find('header_parameters/rawdatafile').text 
    xml['epoch_start'] = float(root.find("header_parameters/tstart").text)
    xml['tsamp'] = float(root.find("header_parameters/tsamp").text)
    xml['nchans'] = int(root.find("header_parameters/nchans").text)
    xml['no_of_samples'] = int(root.find("header_parameters/nsamples").text)

    #Search Parameters
    xml['infile_name'] = root.find("search_parameters/infilename").text
    xml['killfile_name'] = root.find("search_parameters/killfilename").text
    xml['fft_size'] = float(root.find('search_parameters/size').text)


    for P in root.findall("candidates/candidate/period"):
        period.append(float(P.text))
    for A in root.findall("candidates/candidate/acc"):
        acc.append(float(A.text))
    for D in root.findall("candidates/candidate/dm"):
        dm.append(float(D.text))
    for s in root.findall("candidates/candidate/snr"):
        snr.append(float(s.text))
    
    for i in range(len(period)):
        Pdot = a_to_pdot(period[i],acc[i])
        mod_period.append(period_modified(period[i],Pdot,xml['no_of_samples'],xml['tsamp'],xml['fft_size']))
        pdot.append(Pdot)


    # Get number of candidates
    tmp1 = subprocess.getoutput("grep \'candidate id\' %s | tail -1 | awk \'{print $2}\'| grep -o \'.*\'"%xml_file)
    no_of_cands  = int(re.findall(r"'(.*?)'", tmp1, re.DOTALL)[0])+1
         

    # Set all paths
    #output_path = output_dir
    output_path = xml_file.split('/overview.xml')[0]
 
    source_name = xml['source_name']    
    #mask_path = "/beegfs/u/prajwalvp/trapum_processing/01_RFIFIND/2020-04-16-00:59:26_cfbf00000_p_id_15940_iqrm_sub_rfifind.mask" #Hardcoded
    if xml['nchans'] == 4096:
        mask_path = "/beegfs/PROCESSING/TRAPUM/RFIFIND_masks/Ter5_16apr20_4096chan_freq_mask_light/Ter5_full_res_stats_time_2_rfifind.mask" #Hardcoded
    if xml['nchans']== 256:
        mask_path = "/beegfs/PROCESSING/TRAPUM/RFIFIND_masks/Ter5_16apr20_256chan_mask/2020-04-16-00:59:26_cfbf00000_p_id_15940_iqrm_sub_rfifind.mask" # Hardcoded

    batch_no = 18 # No of cores


    # Make the output directory
    try:
        subprocess.check_call("mkdir -p %s"%output_path,shell=True)
    except:
        log.info("Subdirectory already made")
        try:
            subprocess.check_call("rm *pfd* *.txt",shell=True,cwd=output_path)
        except:
            pass
        pass
   
    # Get group of filenames
    if len(dp_list)==1:            
        input_name = dp_list[0]
    else:
        input_name = ' '.join(dp_list)

    # Run in batches
    extra = no_of_cands%batch_no
    batches = int(no_of_cands/batch_no) +1
    for x in range(batches):
        start = x*batch_no
        if(x==batches-1):
            end = x*batch_no+extra
        else:
            end = (x+1)*batch_no   
        for i in range(start,end):
            folding_packet={}
            folding_packet['period'] = mod_period[i]
            folding_packet['acc'] = acc[i]
            folding_packet['pdot'] = pdot[i] 
            folding_packet['dm'] = dm[i] 
            output_name= "dm_%.2f_acc_%.2f_candidate_number_%d"%(folding_packet['dm'],folding_packet['acc'],i)
            try:
                process = subprocess.Popen("prepfold -ncpus 1 -mask %s -noxwin -topo -p %s -pd %s -dm %s %s -o %s"%(mask_path,str(folding_packet['period']),str(folding_packet['pdot']),str(folding_packet['dm']),input_name,output_name),shell=True,cwd=output_path)
            except Exception as error:
                log.error(error)
 
        if  process.communicate()[0]==None:
            continue
        else:
            time.sleep(60)

    while True:
        if len(glob.glob('%s/*.pfd'%output_path)) == no_of_cands:
            log.info("Folding done for all candidates. Scoring all candidates...") 
            tar_name = subprocess.getoutput("python2 webpage_score.py --in_path=%s"%output_path)
            log.info("Scoring done...")     
            # Remove original files 
            subprocess.check_call("rm *pfd* *.txt",shell=True,cwd=output_path)
            return os.path.basename(tar_name),output_path
        else:
            log.info("Still not finished folding...")
            time.sleep(60)     

                   
if __name__ == '__main__':

    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts,args = parser.parse_args()

    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts,fold_and_score_pipeline)
    processor.process(pipeline_wrapper.on_receive)

