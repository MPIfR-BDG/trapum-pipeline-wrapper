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

AI_PATH = '/'.join(ubc_AI.__file__.split('/')[:-1])

def make_tarfile(output_path,input_path,name):
    with tarfile.open(output_path+'/'+name, "w:gz") as tar:
        tar.add(input_path, arcname= name)


def extract_and_score(opts):
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


