import sys
sys.path.append('/home/psr')
import cPickle, glob, ubc_AI
from ubc_AI.data import pfdreader
import os
import subprocess
import time
import json
import logging
import tarfile
import optparse

log = logging.getLogger('webpage_score')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)

#AI_PATH = '/'.join(ubc_AI.__file__.split('/')[:-1])
AI_PATH = '/beegfs/PROCESSING/TRAPUM/pics_trained_models'
models = ["clfl2_trapum_Ter5.pkl", "clfl2_PALFA.pkl"]


def make_tarfile(output_path,input_path,name):
    with tarfile.open(output_path+'/'+name, "w:gz") as tar:
        tar.add(input_path, arcname= name)


def get_id_from_cand_file(filename):
    return(filename.split('_')[-1].strip(".ar"))



def extract_and_score(opts):
    path = opts.in_path
    file_type = opts.file_type
    # Load model
    classifiers = []
    for model in models:
        with open(os.path.join(AI_PATH, model), "rb") as f:
            classifiers.append(cPickle.load(f))
            log.info("Loaded model {}".format(model))
    # Find all files
    arfiles = sorted(glob.glob("{}/*.{}".format(path,file_type)),key=get_id_from_cand_file)
    log.info("Retrieved {} archive files from {}".format(
        len(arfiles), path))
    scores = []
    readers = [pfdreader(f) for f in arfiles]
    for classifier in classifiers:
        scores.append(classifier.report_score(readers))
    log.info("Scored with all models")
    #combined = sorted(zip(arfiles, *scores), reverse=True, key=lambda x: x[1]) Skip sorting
    combined  = zip(arfiles, *scores)
    #log.info("Sorted scores...")
    names = ",".join(["{}".format(model.split("/")[-1]) for model in models])
    with open("{}/pics_scores.txt".format(path),"w") as fout:
        fout.write("arfile,{}\n".format(names))
        for row in combined:
            scores = ",".join(map(str,row[1:]))
            fout.write("{},{}\n".format(row[0], scores))
    log.info("Written to file in {}".format(path))





if __name__ == '__main__':

    parser = optparse.OptionParser()
    parser.add_option('--in_path',type=str,help='input path for files to be scored',dest="in_path")
    parser.add_option('--file_type',type=str,help='Type of file (ar/pfd/ar2/clfd)',dest="file_type",default="ar")
    opts,args = parser.parse_args()
    extract_and_score(opts)


