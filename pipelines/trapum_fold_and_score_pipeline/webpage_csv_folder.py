import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import glob
import xml.etree.ElementTree as ET
import os
import commands
import optparse
import re
import subprocess
import itertools
import logging
import sys



log = logging.getLogger('manual_presto_fold')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)



log.setLevel('INFO')

#parser.add_option('--p_tol',type=float,help='period tolerance',dest="p_tol",default=5e-4)
#parser.add_option('--dm_tol',type=float,help='dm tolerance',dest="dm_tol",default=5e-3)



def get_params_from_csv_and_fold(fold_dp_list,csv_file,output_dir,beam_id):

    # Read file
    all_data = pd.read_csv(csv_file)

    # Select candidates of particular beam id
    beam_data = all_data[all_data['beam_id']==beam_id]  

    cand_periods = beam_data['period'].to_numpy()
    cand_accs = beam_data['acc'].to_numpy()
    cand_dms = beam_data['dm'].to_numpy()
    cand_ids = beam_data['cand_id_in_file'].to_numpy()
    filename = beam_data['file'].to_numpy()

    tree = ET.parse(filename)
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


    no_of_cands = len(cand_mod_periods)
    batch_no = opts.batch_no
    
    # Fold all csv file candidates in batches
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
           folding_packet['period'] = cand_mod_periods[i]
           folding_packet['acc'] = cand_accs[i]
           folding_packet['pdot'] = pdots[i]
           folding_packet['dm'] = cand_dms[i]
           output_name= "candidate_no_%03d_dm_%.2f_acc_%.2f"%(cand_ids[i],folding_packet['dm'],folding_packet['acc'])
           #output_path = filename.split('overview.xml')[0][:-1]
           output_path = output_dir

           # Get input filterbank
           input_filenames = fold_dp_list 
           print (input_filenames)
                     
               
           try:
               process = subprocess.Popen("prepfold -ncpus 1 -nsub 64 -mask %s -noxwin -topo -p %s -pd %s -dm %s %s -o %s"%(opts.mask_path,str(folding_packet['period']),str(folding_packet['pdot']),str(folding_packet['dm']),input_filenames,output_name),shell=True,cwd=output_path)
               #script = "prepfold -ncpus 1 -nsub 64 -mask %s -noxwin -topo -p %s -pd %s -dm %s %s -o %s"%(opts.mask_path,str(folding_packet['period']),str(folding_packet['pdot']),str(folding_packet['dm']),input_name,output_name)
               #log.info(script)
               #log.info(output_path)
           except Exception as error:
               log.error(error)
       if  process.communicate()[0]==None:
           continue
       else:
           time.sleep(10)



def fold_and_score_pipeline(data):
    output_dps = []
    fold_dp_list=[]

    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory
    '''
    processing_args = data['processing_args']
    output_dir = data['base_output_dir']
    mask_path = "/beegfs/PROCESSING/TRAPUM/RFIFIND_masks/Fermi_409chans_mask/Fermi_beam0_052838_20200704_rfifind.mask" # Hardcoded default

    # Make temporary folder to keep temporary outputs
    tmp_dir = '/beeond/PROCESSING/TEMP/%d'%processing_id
    try:
        subprocess.check_call("mkdir -p %s"%(tmp_dir),shell=True)
    except:
        log.info("Already made subdirectory")
        pass

    #Make output dir
    try:
        subprocess.check_call("mkdir -p %s"%(output_dir),shell=True)
    except:
        log.info("Already made subdirectory")
        pass
    processing_id = data['processing_id']

    
    # Loop through each pointing
    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:
            for dp in  (beam["data_products"]):
                if '.fil' in dp["filename"]:
                    fold_dp_list.append(dp["filename"])
                else:
                    csv_file = dp["filename"]
                

            fold_dp_list.sort()
           
               
        # Run the PRESTO folding on the remaining_cands file
        try:
           get_params_from_csv_and_fold(fold_dp_list,csv_file,output_dir) 
        except Exception as error:
           log.error(error)

        # Run scoring
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

if __name__=="__main__":
    
    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts,args = parser.parse_args()

    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts,fold_and_score_pipeline)
    processor.process(pipeline_wrapper.on_receive)
