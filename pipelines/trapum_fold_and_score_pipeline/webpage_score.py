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



def extract_and_score(path):
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




def extract_and_score_old(opts):
    # Load model
    classifier = cPickle.load(open(AI_PATH+'/trained_AI/clfl2_PALFA.pkl','rb'))
    log.info("Loaded model")

    # Find all files
    pfdfile = glob.glob('%s/*.pfd'%(opts.in_path))
    log.info("Retrieved pfd files from %s"%(opts.in_path))

    AI_scores = classifier.report_score([pfdreader(f) for f in pfdfile])
    log.info("Scored with model")

    # Sort based on highest score
    pfdfile_sorted = [x for _,x in sorted(zip(AI_scores,pfdfile),reverse=True)]
    AI_scores_sorted = sorted(AI_scores,reverse=True)
    log.info("Sorted scores..")
 
    text = '\n'.join(['%s %s' % (pfdfile_sorted[i], AI_scores_sorted[i]) for i in range(len(pfdfile))])

    fout = open('%s/pics_original_descending_scores.txt'%opts.in_path,'w')
    fout.write(text)
    log.info("Written to file in %s"%opts.in_path)
    fout.close()

    #tar_name = os.path.basename(opts.in_path)+"_presto_cands.tar.gz"
    #make_tarfile(opts.in_path,opts.in_path,tar_name)
    #print tar_name

if __name__ == '__main__':

    parser = optparse.OptionParser()
    parser.add_option('--in_path',type=str,help='input path for files to be scored',dest="in_path")
    opts,args = parser.parse_args()
    extract_and_score(opts)


