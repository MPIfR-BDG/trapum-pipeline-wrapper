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



parser = optparse.OptionParser()
parser.add_option('-X',type=str,help='Path to PEASOUP xml file subdirectoies',dest="xml_path")
parser.add_option('-O',type=str,help='path to output file',dest="output_path",default=".")
parser.add_option('-P',type=str,help='path to par files',dest="par_path",default='/beegfs/u/prajwalvp/presto_ephemerides/Ter5/par_files_scott')
parser.add_option('-H',type=int,help='harmonic number to search upto',dest="h_no",default=16)
parser.add_option('--p_tol',type=float,help='period tolerance',dest="p_tol",default=5e-4)
parser.add_option('--dm_tol',type=float,help='dm tolerance',dest="dm_tol",default=5e-3)
parser.add_option('--batch_no',type=float,help='Number of folds in one batch',dest="batch_no",default=22)
opts,args = parser.parse_args()


known_rfi_freqs = [866.6666,346.666666,1000.0000,400.0000,766.02380371,577.7777,408.1726074218758,102.04315185546875]


def a_to_pdot(P_s, acc_ms2):
    LIGHT_SPEED = 2.99792458e8                 # Speed of Light in SI
    return P_s * acc_ms2 /LIGHT_SPEED


def period_modified(p0,pdot,no_of_samples,tsamp,fft_size):
    if (fft_size==0.0):
        return p0 - pdot*float(1<<(no_of_samples.bit_length()-1))*tsamp/2
    else:
        return p0 - pdot*float(fft_size)*tsamp/2

def sortKeyFunc(s):
    return int(re.search('f1_(.*)_',s).group(1).split('_')[0])


def get_params_from_pars(par_path):

    known_psrs={}
    known_psrs['names']=[]
    known_psrs['F0']=[]
    known_psrs['DM']=[]

    known_pars=glob.glob(par_path+'/*.par')
    for kp in known_pars:
        known_psrs['names'].append(os.path.basename(kp)[:-4])
        known_psrs['F0'].append(commands.getoutput('grep \"F0\" %s'%kp).split()[1])
        known_psrs['DM'].append(commands.getoutput('grep \"DM\" %s'%kp).split()[1])

    return known_psrs


def get_params_from_xml(xml_path):
    xml_file = xml_path + '/' + 'overview.xml'
    tree = ET.parse(xml_file)
    root = tree.getroot()
    cand_periods = np.array([a.text for a in root.findall("candidates/candidate/period")],dtype=float)
    cand_accs = np.array([a.text for a in root.findall("candidates/candidate/acc")],dtype=float)
    tsamp = float(root.find("header_parameters/tsamp").text)
    fft_size = float(root.find('search_parameters/size').text)
    nsamples = int(root.find("header_parameters/nsamples").text)


    mod_periods=[]
    pdots= []
    for i in range(len(cand_periods)):
        Pdot = a_to_pdot(cand_periods[i],cand_accs[i])
        mod_period.append(period_modified(cand_periods[i],Pdot,xml['no_of_samples'],xml['tsamp'],xml['fft_size']))
        pdot.append(Pdot) 

    cand_mod_periods = np.asarray(mod_period,dtype=float)

    # Modify periods
    cand_freqs = 1/cand_mod_periods
    cand_dms = np.array([a.text for a in root.findall("candidates/candidate/dm")],dtype=float)
    cand_snrs = np.array([a.text for a in root.findall("candidates/candidate/snr")],dtype=float)
    cand_nh = np.array([a.text for a in root.findall("candidates/candidate/nh")],dtype=int)

    return cand_mod_periods,cand_freqs,cand_dms,cand_snrs,cand_nh


def get_possible_rfi_indices(opts,cand_freqs):
    
    # Whole number indices
    #numbers = np.arange(1,opts.h_no,1)

    h = np.arange(1,opts.h_no)
    ratios = np.outer(h,1.0/h)
    all_ratios = np.unique(np.sort(ratios.ravel()))


    # known rfi frequencies


    w_harmonics=[]
       
    for number in all_ratios:
        for rfi in known_rfi_freqs:            
            w_harmonics.append([a for a in cand_freqs if abs(a - round(number*rfi))/(a)<opts.p_tol])

    all_harmonics=sorted(list(set(list(itertools.chain(*w_harmonics)))))
    rfi_indices = np.array([np.where(cand_freqs==h)[0][0] for h in all_harmonics])

    return rfi_indices


def get_possible_pulsars_and_harmonics_indices(opts,known_psrs,known_freqs,known_dms,cand_freqs,cand_dms):
    psr_indices=[]
    reduced_ph_indices=[]

    #Pulsar indices
    known_freq_indices,cand_freq_indices = np.where(np.isclose(*np.ix_(known_freqs, cand_freqs), rtol=opts.p_tol))

    with open('possible_known_psrs.txt','a') as f:
        #f.truncate(0)
        f.write(xml_path+'\n')
        f.write("PULSAR CAND_NO SNR F0 CAND_F0 DM CAND_DM\n")
        for ind in sorted(cand_freq_indices):
           idx = known_freq_indices[np.where(cand_freq_indices==ind)[0][0]]
           if np.isclose(cand_dms[ind],known_dms[idx],rtol=opts.dm_tol):
               f.write(known_psrs['names'][idx]+' '+str(ind)+' '+str(cand_snrs[ind])+' '+str(known_freqs[idx])+' '+str(cand_freqs[ind])+' '+str(known_dms[idx])+' '+str(cand_dms[ind])+'\n')
               log.info("Potential redetection....")
               log.info("PULSAR CAND_NO SNR F0 CAND_F0 DM CAND_DM\n")
               log.info(known_psrs['names'][idx]+' '+str(ind)+' '+str(cand_snrs[ind])+' '+str(known_freqs[idx])+' '+str(cand_freqs[ind])+' '+str(known_dms[idx])+' '+str(cand_dms[ind])+'\n')
               psr_indices.append(ind)     
        f.close()

    #Harmonic indices - Reduce to harmonics of Ter5A and C only
    h = np.arange(1,opts.h_no)
    ratios = np.outer(h,1.0/h)
    all_ratios = np.unique(np.sort(ratios.ravel()))

    known_freqs = [118.53825602,86.48163691]


    for i,freq in enumerate(known_freqs):
        r = all_ratios*freq            
        p = abs(1-cand_freqs/r[np.searchsorted(r,cand_freqs)-1])

        if np.searchsorted(r,cand_freqs).max()==len(r):
            tmp = np.searchsorted(r,cand_freqs)    
            fixed = np.where(tmp==tmp.max(), tmp.max()-1, tmp) 
            q = abs(1-cand_freqs/r[fixed])
        else:
            q = abs(1-cand_freqs/r[np.searchsorted(r,cand_freqs)])
        l = np.vstack((p, q)).min(axis=0)
        ph_indices = np.where(l<opts.p_tol)[0]

        for ph in sorted(ph_indices):
            if np.isclose(cand_dms[ph],known_dms[i],rtol=opts.dm_tol):
                reduced_ph_indices.append(ph)
                #print freq,known_psrs['names'][i],ph
                

   
    return np.array(psr_indices),np.array(reduced_ph_indices)




if __name__=="__main__":
        
    # Get all xmls 
    
    xml_path = opts.xml_path

    log.info("Reading XML file from %s"%xml_path) 
    tree = ET.parse(xml_path+'/overview.xml')
    root = tree.getroot()
    cand_periods = np.array([a.text for a in root.findall("candidates/candidate/period")],dtype=float)
    cand_dms = np.array([a.text for a in root.findall("candidates/candidate/dm")],dtype=float)
    cand_accs = np.array([a.text for a in root.findall("candidates/candidate/acc")],dtype=float)
    cand_snrs = np.array([a.text for a in root.findall("candidates/candidate/snr")],dtype=float)
    cand_nh = np.array([a.text for a in root.findall("candidates/candidate/nh")],dtype=int)
    tsamp = float(root.find("header_parameters/tsamp").text)
    fft_size = float(root.find('search_parameters/size').text)
    nsamples = int(root.find("header_parameters/nsamples").text)


    log.info("Total number of candidates produced: %d"%len(cand_periods))


    # Modify period

    log.info("Calculating modified period based on starting epoch reference")
    mod_periods=[]
    pdots = []
    for i in range(len(cand_periods)):
        Pdot = a_to_pdot(cand_periods[i],cand_accs[i])
        mod_periods.append(period_modified(cand_periods[i],Pdot,nsamples,tsamp,fft_size))
        pdots.append(Pdot)

    cand_mod_periods = np.asarray(mod_periods,dtype=float)
    cand_freqs = 1/cand_mod_periods






    log.info("Retrieving RFI candidates from catalogue of sources....")
    rfi_indices = get_possible_rfi_indices(opts,cand_freqs)
    log.info("Done. Number of RFI based candidates: %d"%len(rfi_indices))
    print rfi_indices
    
    log.info("Retrieving known pulsar parameters from par files...")        
    known_psrs = get_params_from_pars(opts.par_path)
    known_freqs = np.array(known_psrs['F0'],dtype=float)
    known_dms = np.array(known_psrs['DM'],dtype=float)
    log.info("Done")


    log.info("Getting possible pulsar  and corresponding harmonic candidates indices")
    p_indices,ph_indices = get_possible_pulsars_and_harmonics_indices(opts,known_psrs,known_freqs,known_dms,cand_freqs,cand_dms)
    log.info("Done: Number of pulsars: %d , Number of Ter5A and C harmonics: %d"%(len(p_indices),len(ph_indices)))
    print p_indices,ph_indices
    


    log.info("Setting up paths for folding") 
    input_name = root.find("search_parameters/infilename").text 
    mask_path = "/beegfs/PROCESSING/TRAPUM/RFIFIND_masks/Ter5_16apr20_frankenmask_improved/frankenbeam_mask_rfifind.mask"
    output_path = xml_path
     


    log.info("First folding the candidates that do not make the RFI,pulsar or harmonics cut")
    remaining_cand_indices = [i for i in range(len(cand_mod_periods)) if i not in rfi_indices and i not in ph_indices and i not in p_indices]
    log.info("Number of caniddates: %d"%(len(remaining_cand_indices)))


    # Fold interesting candidates    
    no_of_cands = len(remaining_cand_indices)
    batch_no = opts.batch_no


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
           folding_packet['period'] = cand_mod_periods[remaining_cand_indices[i]]
           folding_packet['acc'] = cand_accs[remaining_cand_indices[i]]
           folding_packet['pdot'] = pdots[remaining_cand_indices[i]]
           folding_packet['dm'] = cand_dms[remaining_cand_indices[i]]
           output_name= "candidate_no_%03d_dm_%.2f_acc_%.2f"%(remaining_cand_indices[i],folding_packet['dm'],folding_packet['acc'])
           try:
               process = subprocess.Popen("prepfold -ncpus 1 -nsub 64 -mask %s -noxwin -topo -p %s -pd %s -dm %s %s -o %s"%(mask_path,str(folding_packet['period']),str(folding_packet['pdot']),str(folding_packet['dm']),input_name,output_name),shell=True,cwd=output_path)
               #print "prepfold -ncpus 1 -nsub 64 -mask %s -noxwin -topo -p %s -pd %s -dm %s %s -o %s"%(mask_path,str(folding_packet['period']),str(folding_packet['pdot']),str(folding_packet['dm']),input_name,output_name)
           except Exception as error:
               print error
       if  process.communicate()[0]==None:
           continue
       else:
           time.sleep(10)


    # Fold possible redetections
    log.info("Folding possible redetections")
    redetection_indices = np.unique(np.concatenate([p_indices,ph_indices]))
    no_of_cands = len(redetection_indices)


    if len(redetection_indices) == 0:
        log.info("No known pulsar detections found. Exiting...")
        sys.exit(0)


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
           folding_packet['period'] = cand_mod_periods[redetection_indices[i]]
           folding_packet['acc'] = cand_accs[redetection_indices[i]]
           folding_packet['pdot'] = pdots[redetection_indices[i]]
           folding_packet['dm'] = cand_dms[redetection_indices[i]]
           output_name= "redetect_candidate_no_%03d_dm_%.2f_acc_%.2f"%(redetection_indices[i],folding_packet['dm'],folding_packet['acc'])
           try:
               process = subprocess.Popen("prepfold -ncpus 1 -nsub 64 -mask %s -noxwin -topo -p %s -pd %s -dm %s %s -o %s"%(mask_path,str(folding_packet['period']),str(folding_packet['pdot']),str(folding_packet['dm']),input_name,output_name),shell=True,cwd=output_path)
               #print "prepfold -ncpus 1 -nsub 64 -mask %s -noxwin -topo -p %s -pd %s -dm %s %s -o %s"%(mask_path,str(folding_packet['period']),str(folding_packet['pdot']),str(folding_packet['dm']),input_name,output_name)
           except Exception as error:
               print error
       if  process.communicate()[0]==None:
           continue
       else:
           time.sleep(60)
    subprocess.check_call("python2 webpage_score.py --in_path=%s"%output_path,shell=True)
