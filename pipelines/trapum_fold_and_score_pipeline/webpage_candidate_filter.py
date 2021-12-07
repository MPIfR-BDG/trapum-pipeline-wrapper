import numpy as np
import pandas as pd
import os
import optparse
import subprocess
import logging
import mongo_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
import tarfile
import json
import shutil

#%% Start a log for the pipeline

log = logging.getLogger('candidate_filter')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel('INFO')

#%% Pipeline utils definitions

def make_tarfile(output_path, input_path, name):
    with tarfile.open(output_path + '/' + name, "w:gz") as tar:
        tar.add(input_path, arcname=name)

def remove_dir(dir_name):
    if 'TEMP' in dir_name:
        shutil.rmtree(dir_name)
    else:
        log.error("Directory not deleted. Not a temporary folder!")

#%% Pipeline

def candidate_filter_pipeline(data, status_callback):
    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory
    '''
    
    #Get processing args
    processing_args = data['processing_args']
    output_dir = data['base_output_dir']
    snr_cutoff = processing_args.get("snr_cutoff", 9.5)
    processing_id = data['processing_id']
    output_dps = []
    
    # Make output dir
    try:
        subprocess.check_call("mkdir -p %s" % (output_dir), shell=True)
    except BaseException:
        log.info("Already made subdirectory")
        pass

    # Make a list of candidate files in a pointing (can be peasoup xml or riptide csv)
    for pointing in data["data"]["pointings"]:
        candidate_files_list = []
        beam_id_list = []
        
        #Get the candidate file for each beam
        for beam in pointing["beams"]:
            for dp in (beam["data_products"]):
                candidate_files_list.append(dp["filename"]) 
                beam_id_list.append(beam["id"])

        # Make temporary folder to keep any temporary outputs
        tmp_dir = '/beeond/PROCESSING/TEMP/%d' % processing_id
        try:
            subprocess.check_call("mkdir -p %s" % (tmp_dir), shell=True)
        except BaseException:
            log.info("Already made subdirectory")
            pass

        #Collate all the candidate file names for this poiting
        candidate_files_list_path = "{}/candidate_files_list".format(tmp_dir)
        with open(candidate_files_list_path, "w") as f:
            for candidate_file in candidate_files_list:
                f.write("{}\n".format(candidate_file))
                
        # Run the candidate filtering code
        log.info("Filtering")
        status_callback("filtering")
        try:
            subprocess.check_call(
                "candidate_filter.py -i %s -o %s/%d -c /home/psr/software/candidate_filter/candidate_filter/default_config.json --rfi /home/psr/software/candidate_filter/candidate_filter/known_rfi.txt --p_tol %f --dm_tol %f" %
                (candidate_files_list_path, tmp_dir, processing_id, processing_args['p_tol'], processing_args['dm_tol']), shell=True)
            log.info("Filtered csvs have been written")
        except Exception as error:
            log.error(error)

        # Apply SNR cut and insert beam ID in good cands to fold csv file for
        # later reference
        df = pd.read_csv(
            '%s/%d_good_cands_to_fold.csv' %
            (tmp_dir, processing_id))

        df_snr_cut = df[df['snr'] > snr_cutoff]
        log.info("Applied SNR cut of {}".format(snr_cutoff))

        log.info("Adding beam id column to folding csv file")
        all_candidate_files = df_snr_cut['file'].values

        beam_id_values = []

        for i in range(len(all_candidate_files)):
            ind = candidate_files_list.index(all_candidate_files[i])
            beam_id_values.append(beam_id_list[ind])

        df_snr_cut['beam_id'] = np.asarray(beam_id_values)

        df_snr_cut.to_csv(
            '%s/%d_good_cands_to_fold_with_beam.csv' %
            (tmp_dir, processing_id))
        log.info("New beam id column added to folding csv file")

        # Tar up the csv files
        log.info("Tarring up all csvs")
        tar_name = os.path.basename(output_dir) + "_csv_files.tar.gz"
        make_tarfile(output_dir, tmp_dir, tar_name)
        log.info("Tarred.")

        # Remove contents in temporary directory
        remove_dir(tmp_dir)
        log.info("Removed temporary files")

        # Add tar file to dataproduct
        dp = dict(
            type="candidate_tar_file",
            filename=tar_name,
            directory=output_dir,
            # Note: This is just for convenience. Technically needs all beam
            # ids
            beam_id=beam_id_list[0],
            pointing_id=pointing["id"],
            metainfo=json.dumps("tar_file:filtered_csvs")
        )

        output_dps.append(dp)

    return output_dps

if __name__ == "__main__":

    parser = optparse.OptionParser()
    mongo_wrapper.add_mongo_consumer_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts, args = parser.parse_args()

    # processor = mongo_wrapper.PikaProcess(...)
    processor = mongo_wrapper.mongo_consumer_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts, candidate_filter_pipeline)
    processor.process(pipeline_wrapper.on_receive)