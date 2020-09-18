import numpy as np
import pandas as pd
import glob
import xml.etree.ElementTree as ET
import os
import optparse
import re
import subprocess
import itertools
import logging
import sys
import pika_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
import time
import tarfile
import json
import shutil

log = logging.getLogger('manual_presto_fold')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel('INFO')

#parser.add_option('--p_tol',type=float,help='period tolerance',dest="p_tol",default=5e-4)
#parser.add_option('--dm_tol',type=float,help='dm tolerance',dest="dm_tol",default=5e-3)

def make_tarfile(output_path,input_path,name):
    with tarfile.open(output_path+'/'+name, "w:gz") as tar:
        tar.add(input_path, arcname= name)


def remove_dir(dir_name):
    if 'TEMP' in dir_name:
        shutil.rmtree(dir_name)
    else:
        log.error("Directory not deleted. Not a temporary folder!") 


def candidate_filter_pipeline(data):
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

   # Get an xml list per pointing
    for pointing in data["data"]["pointings"]:
       xml_list=[]
       beam_id_list=[]  
       for beam in pointing["beams"]:
           for dp in  (beam["data_products"]):
               xml_list.append(dp["filename"])
               beam_id_list.append(beam["id"]) 

               
   
       # Make temporary folder to keep any temporary outputs
       tmp_dir = '/beeond/PROCESSING/TEMP/%d'%processing_id
       try:
           subprocess.check_call("mkdir -p %s"%(tmp_dir),shell=True)
       except:
           log.info("Already made subdirectory")
           pass

    

         
       # Run the candidate filtering code
       try:
          xml_list2 = ','.join(xml_list)
          subprocess.check_call("candidate_filter.py -i %s -o %s/%d -c /home/psr/software/candidate_filter/candidate_filter/default_config.json --rfi /home/psr/software/candidate_filter/candidate_filter/known_rfi.txt --p_tol %f --dm_tol %f"%(xml_list2,tmp_dir,processing_id,processing_args['p_tol'],processing_args['dm_tol']),shell=True)
          log.info("Filtered csvs have been written")
       except Exception as error:
          log.error(error)


       # insert beam ID in good cands to fold csv file for later reference

       log.info("Adding beam id column to folding csv file")
       df = pd.read_csv('%s/%d_good_cands_to_fold.csv'%(tmp_dir,processing_id))
       all_xml_files = df['file'].values

   
       beam_id_values = []
 
       for i in range(len(all_xml_files)):
           ind = xml_list.index(all_xml_files[i])
           beam_id_values.append(beam_id_list[ind])

       df['beam_id'] = np.asarray(beam_id_values)
       df.to_csv('%s/%d_good_cands_to_fold_with_beam.csv'%(tmp_dir,processing_id))
       log.info("New beam id column added to folding csv file")
    
          
       # Tar up the csv files
       log.info("Tarring up all csvs")
       tar_name = os.path.basename(output_dir)+"_csv_files.tar.gz"
       make_tarfile(output_dir,tmp_dir,tar_name)   
       log.info("Tarred.")
        

       # Remove contents in temporary directory
       remove_dir(tmp_dir)  
       log.info("Removed temporary files")

       # Add tar file to dataproduct
       dp = dict(
                 type="candidate_tar_file",
                 filename=tar_name,
                 directory=output_dir,
                 beam_id=beam_id_list[0], # Note: This is just for convenience. Technically needs all beam ids 
                 pointing_id=pointing["id"],
                 metainfo=json.dumps("tar_file:filtered_csvs")
                 ) 

       output_dps.append(dp)
    
    return output_dps

if __name__=="__main__":
    
    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts,args = parser.parse_args()

    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts,candidate_filter_pipeline)
    processor.process(pipeline_wrapper.on_receive)


