import subprocess
import glob
import optparse
import parseheader
import os
import logging
import sys
import pika_process
import json

log = logging.getLogger('trapum_beam_merger_receive')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(filename='trapum_beam_merger.log',filemode='a',format=FORMAT,level=logging.DEBUG)
#logging.basicConfig(format=FORMAT,level=logging.DEBUG)


def receive_and_merge(message,opts):
    info = json.loads(message.decode("utf=8"))
    digifil_script = info['digifil_script']
    correct_header = info['correct_header']
    prepfold_script = info['prepfold_script']
    print (digifil_script)

    try:
        subprocess.check_call(digifil_script,shell=True)
        subprocess.check_call(correct_header,shell=True)
        log.debug("Successfully merged.Preparing to prepfold..")
        if len(prepfold_script) > 1:
            for script in prepfold_script:
                subprocess.check_call(script,shell=True,cwd=info["output_path"])
        elif type(prepfold_script)==list:
            subprocess.check_call(prepfold_script[0],shell=True,cwd=info["output_path"])
        else:
            subprocess.check_call(prepfold_script,shell=True,cwd=info["output_path"])


    #except subprocess.CalledProcessError:
    #    log.info("Partial file exists. Removing and creating a new one....")   
    #    subprocess.check_call("rm %s/%s"%(info['output_path'],info['filename']),shell=True)
    #    print ("rm %s/%s"%(info['output_path'],info['filename']))
    #    subprocess.check_call(digifil_script,shell=True)
        # Publish to Rabbit
        #pika_process.publish_info(opts,info)

    except Exception as e:
        log.error("Merge and failed for %s in path %s"%(prepfold_script,info['output_path']))
        log.error(e)


        


if __name__=="__main__":

    # Update all input arguments
    consume_parser = optparse.OptionParser()
    pika_process.add_pika_process_opts(consume_parser)
    opts,args = consume_parser.parse_args()


    # Setup logging config
    log_type=opts.log_level
    log.setLevel(log_type.upper())

    #Consume message from RabbitMQ 
    processor = pika_process.pika_process_from_opts(opts)
    processor.process(lambda message: receive_and_merge(message,opts))

