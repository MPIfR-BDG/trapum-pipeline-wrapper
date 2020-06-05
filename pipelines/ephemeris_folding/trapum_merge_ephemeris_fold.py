import subprocess
import glob
import optparse
import parseheader
import os
import logging
import sys
import time
import pika_process

log = logging.getLogger('trapum_all_beam_merger')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
#logging.basicConfig(filename='trapum_beam_merger.log',filemode='a',format=FORMAT,level=logging.DEBUG)
logging.basicConfig(format=FORMAT,level=logging.DEBUG)


def get_no_of_subdirectories_in_path(path): #### Latest structure
    script = 'find %s/* -maxdepth 0 -type d'%path
    beam_list = subprocess.getoutput(script)
    return [os.path.basename(b) for b in beam_list.split('\n')]


def get_beam_list(path):  #### Old structure
    script = 'find %s -name "*cfbf*" | awk -F[_,_] \'{print $3}\' | sort | uniq'%path
    unique_beam_list = subprocess.getoutput(script)
    return unique_beam_list.split('\n')


def read_par_list(filename):
    with open(filename,'r') as f:
        pars=f.readlines()
    return [p.strip('\n') for p in pars]
    
    
if __name__=="__main__":

    # Pass arguments
    parser = optparse.OptionParser()
    pika_process.add_pika_producer_opts(parser)
    parser.add_option('--length',type=str,help='Length in seconds to merge files',dest="length",default='full')
    parser.add_option('--observation_path',type=str,help='path to observation files',dest="observation")
    parser.add_option('--merged_file_path',type=str,help='path to merged files',dest='merged_path',default="/beegfs/PROCESSING/TRAPUM/known_pulsar_folding")
    parser.add_option('--number_of_beams',type=int,help='Merge first N beams',dest='beam_no',default=288)
    parser.add_option('--beam_numbers',type=str,help='Give beam numbers as list',dest='beam_list',default="unset")
    parser.add_option('-P',type=str,help='Par file path',dest='par',default="/beegfs/u/prajwalvp/meertime_ephemerides")
    parser.add_option('--par_list',type=str,help='List of par files to fold with',dest='par_list',default="unset")
    parser.add_option('-C',type=int,help='Number of CPUS for prepfold',dest='cpus',default=10)
    parser.add_option('-B',type=int,help='Number of Batches',dest='batches',default=15)
    parser.add_option('-M',type=str,help='PRESTO mask full path',dest='mask',default='unset')
    parser.add_option('--ds_t',type=int,help='downsample factor in time',dest='time_ds',default=1)
    parser.add_option('--ds_f',type=int,help='downsample factor in frequency',dest='freq_ds',default=1)
    opts,args = parser.parse_args() 
    
    new_paths = [x[0] for x in os.walk(opts.observation)]


    # Setup logging config
    log_type=opts.log_level
    log.setLevel(log_type.upper())



    for new_path in new_paths:
        if not glob.glob(new_path+'/*.meta'):
            log.debug("No meta file found in path %s"%new_path)
            continue
        else:
             # Find total number of beams
            log.info("Beam files found in path %s"%new_path)
            beam_list = get_no_of_subdirectories_in_path(new_path)
            log.info("Number of coherent beams found: %d"%len(beam_list[:-1]))

            # Reduced beam list
            if opts.beam_list=='unset':
                unique_beam_list = beam_list[:opts.beam_no]
            else:
                unique_beam_list = ['cfbf'+'{0:05d}'.format(int(s)) for s in opts.beam_list.split(',')]

            print(unique_beam_list)
            log.info("Number of beams to process: %d"%len(unique_beam_list))

            # Retrieve partial filename and create subdirectory
            try:
                subdirectory_name = new_path.split('/')[-2]+'/'+new_path.split('/')[-1]
                final_merge_path = opts.merged_path + '/' + subdirectory_name
                subprocess.check_call("mkdir -p %s"%(final_merge_path),shell=True)
            except:
                log.info("Subdirectory already made")
                pass

            info={}
            info['output_path'] = final_merge_path

            for beam_name in unique_beam_list:
                files_per_beam = sorted(glob.glob(new_path+'/'+beam_name+'/*.fil'))

                if len(files_per_beam) ==1:
                    log.info("No need to merge since one file recorded per beam")
                    sys.exit(0)

                # Get file header
                file_info1 = parseheader.parseSigprocHeader(files_per_beam[0])
                file_info = parseheader.updateHeader(file_info1)

                if opts.length =='full':
                   log.info("Merging all files for the beam")
                   full_combined_path = final_merge_path + '/'+"%s_full_combined.fil"%beam_name

                   info['correct_header'] = "/beegfs/u/prajwalvp/alex_change_filterbank_header -telescope MeerKAT -files %s"%full_combined_path
                   # Check for downsampling in time and frequency
                   if opts.time_ds==1:
                       info['digifil_script'] = "/beegfs/u/prajwalvp/digifil %s/*.fil -f %d -threads 15 -b 8 -o %s"%(new_path+'/'+beam_name,opts.freq_ds,full_combined_path)
                   else:
                       info['digifil_script'] = "/beegfs/u/prajwalvp/digifil %s/*.fil -f %d -t %d -threads 15 -b 8 -o %s"%(new_path+'/'+beam_name,opts.freq_ds,opts.time_ds,full_combined_path)  
                   if opts.par_list=='unset':
                       info['prepfold_script'] = "prepfold -mask %s -ncpus %d -npart 128 -timing %s/%s.par -o %s_full %s"%(opts.mask,opts.cpus,opts.par,file_info['source_name'],beam_name,full_combined_path)
                       print (info)
                   else:
                       par_list = read_par_list(opts.par_list)
                       for par in par_list:
                           info['prepfold_script'] = "prepfold -mask %s -ncpus %d -npart 128 -timing %s -o %s_full %s"%(opts.mask,opts.cpus,par,beam_name,full_combined_path)
                           print (info)
                          
                            
                  
                   # Publish to Rabbit
                   #log.debug("Ready to publish to RabbitMQ")
                   #pika_process.publish_info(opts,info)
                else:
                   no_of_files_per_merge = int(round(float(opts.length)/file_info['tobs']))
                   log.info("Number of files in merge: %d"%no_of_files_per_merge)
                   print(len(files_per_beam),file_info['tobs'])
                   no_of_merges = int(round(len(files_per_beam)/no_of_files_per_merge))
                   log.info("Number of merges: %d"%no_of_merges)

                   actual_length = no_of_files_per_merge*float(file_info['tobs'])
                   log.info("Closest length to given length: %f"%actual_length)

                   for merges in range(no_of_merges):
                       print(merges)
                       if merges==no_of_merges-1:
                           subgroup_files = " ".join(files_per_beam[merges*no_of_files_per_merge:])
                           print("Last merge for beam")
                       else:
                           subgroup_files = " ".join(files_per_beam[merges*no_of_files_per_merge:(merges+1)*no_of_files_per_merge])
                           print(subgroup_files)
                       merge_name = 'length_%s_s_merge_%d_'%(opts.length,merges)+os.path.basename(files_per_beam[merges*no_of_files_per_merge])
                       info['correct_header'] = "/beegfs/u/prajwalvp/alex_filterbank_change_header -telescope MeerKAT -files %s/%s"%(final_merge_path,merge_name)
                       if opts.time_ds==1:
                           digifil_script = "digifil %s -threads 15 -f %d -b 8  -o %s/%s"%(subgroup_files,opts.freq_ds,final_merge_path,merge_name)
                           info['digifil_script'] = digifil_script
                           info['prepfold_script'] = "prepfold -mask %s -ncpus %d -npart 128 -timing %s/%s.par -o %s_merge_%d %s/%s"%(opts.mask,opts.cpus,opts.par,file_info['source_name'],beam_name,merges,final_merge_path,merge_name)
                           print(info)
                       else:
                           digifil_script = "digifil %s -threads 15 -t %d -f %d -b 8 -o %s/%s"%(subgroup_files,opts.time_ds,opts.freq_ds,final_merge_path,merge_name)
                           #print (digifil_script)
                           info['digifil_script'] = digifil_script
                           info['prepfold_script'] = "prepfold -mask %s -ncpus %d -npart 128 -timing %s/%s.par -o %s_merge_%d %s/%s"%(opts.mask,opts.cpus,opts.par,file_info['source_name'],beam_name,merges,final_merge_path,merge_name) 
                           print (info) 

                       # Publish to Rabbit
                       #log.debug("Ready to publish to RabbitMQ")
                       #pika_process.publish_info(opts,info)
                       


                     
