import pika_wrapper
import trapum_pipeline_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper
import optparse
from sqlalchemy.pool import NullPool

#process_manager = PikaProcess(...)
#pipeline_wrapper = TrapumPipelineWrapper(..., null_pipeline)
#process_manager.process(pipeline_wrapper.on_receive)

def null_pipeline(data):

    output_dps = []

    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory
    '''
    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:
            # Processing happens here
            dp = dict(
                 type="peasoup_xml",
                 filename="overview.xml",
                 directory=data["base_output_dir"],
                 beam_id = beam["id"],
                 pointing_id = pointing["id"]
                 )
               
            output_dps.append(dp)

    return output_dps

    

if __name__ == '__main__':

    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts,args = parser.parse_args()

    #processor = pika_wrapper.PikaProcess(...)
    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts,null_pipeline)
    processor.process(pipeline_wrapper.on_receive)

