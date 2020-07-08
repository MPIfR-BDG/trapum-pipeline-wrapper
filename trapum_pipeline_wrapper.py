import os
import xxhash
import json
import datetime
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_models import Processing, Hardware, FileType, DataProduct, t_processing_inputs, ProcessingArgument
import pika_wrapper

log = logging.getLogger('trapum_pipeline_wrapper')


# Create processing , inputs for processing, submit time 



class TrapumPipelineWrapper(object):
    def __init__(self, opts, pipeline_callable):
        self._pipeline_callable = pipeline_callable
        self._processing_id = None
        self._opts = opts
        self._session_engine = create_engine(opts.database, connect_args={'connect_timeout':100} ,echo=False, poolclass=NullPool)
        self._session_factory = sessionmaker(
            bind=self._session_engine)
        self._hardware_id = self.get_hardware_id()

    @contextmanager
    def session(self):
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as error:
            session.rollback()
            raise error
        finally:
            session.close()

    def get_hardware_id(self):
        with self.session() as session:
            hardware = session.query(Hardware).filter(
                Hardware.name.ilike("dave"),
                ).first()
            if hardware:
                self._hardware_id = hardware.id
            else:
                hardware = Hardware(name="dave")
                session.add(hardware)
                session.flush()
                self._hardware_id = hardware.id

    def _generate_filehash(self, filepath):
        xx = xxhash.xxh64()
        with open(filepath, 'rb') as f:
            xx.update(f.read(10000))
            xx.update('{}'.format(os.path.getsize(filepath)))
        return xx.hexdigest()


    def on_receive(self, message):
        # here we parse the argument model
        """
        {
            "processing_id": 1,
            "data": { "pointings": [{}]
            }
            "processing_args": {}
        }
        """
        # If this fails there should be no update to
        # the database it should just result in the pika
        # wrapper passing the message to the fail queue
        data = json.loads(message.decode())
        with self.session() as session:
            processing = session.query(Processing).get(data["processing_id"])
            if processing is None:
                raise Exception("No Processing entry with ID = {}".format(
                    data["processing_id"]))
            self._processing_id = processing.id 
            processing.start_time = datetime.datetime.utcnow()
            processing.process_status = "running"
            session.add(processing)
        try:
            data_products = self._pipeline_callable(data)
            self.on_success(data, data_products)
        except Exception as error:
            log.exception("Error from pipeline: {}".format(str(error)))
            self.on_fail()
            raise error

    def on_success(self, data, data_products):
        '''
         required from pipeline: Filetype, filename, beam id , pointing id, directory

        '''
          
        with self.session() as session:
            processing = session.query(Processing).get(self._processing_id)
            now = datetime.datetime.utcnow()
            processing.end_time = now

            processing_args = {"processing":"folding_test"} 

            result = session.query(ProcessingArgument).filter(ProcessingArgument.arguments_json.like(json.dumps(processing_args))).first()
            if result is None:
                new_pargs = ProcessingArgument(arguments_json=json.dumps(processing_args)) 
                session.add(new_pargs)
                session.flush()
                pargs_id = new_pargs.id
            else:
                pargs_id = result.id 
            # Create next processing
            next_processing = Processing(
                pipeline_id=1, # !!
                hardware_id=self.get_hardware_id(),
                arguments_id=pargs_id,
                submit_time=now,
                process_status="enqueued"
                ) 

   
            for dp in data_products:
                ft = session.query(FileType).filter(
                    FileType.name.ilike(dp['type'])).first()
                if ft is None:
                    ft = FileType(name=dp['type'], description="unknown")
                    session.add(ft)
                    session.flush()
                filehash = self._generate_filehash(os.path.join(dp['directory'],dp['filename']))
                data_product = DataProduct(
                    filename=dp['filename'],
                    filepath=dp['directory'],
                    upload_date=now,
                    modification_date=now,
                    file_type_id=ft.id,
                    beam_id=dp["beam_id"],
                    pointing_id=dp["pointing_id"],
                    processing_id=processing.id,
                    filehash=filehash, 
                    available=True,
                    locked=True
                    )
                session.add(data_product)
                next_processing.inputs.append(data_product) 
                session.flush()
                dp["id"] = data_product.id    
            processing.process_status = "success"

            for pointing in data["data"]["pointings"]:
                for beam in pointing["beams"]:
                    for dp in beam["data_products"]:
                        dp_result = session.query(DataProduct).get(dp["id"])    
                        next_processing.inputs.append(dp_result) 
                                        
            session.add(processing)
            session.flush()
            session.add(next_processing)
            session.flush()

            # Message for next process
            message = {}
            message["processing_id"] = next_processing.id
            message["base_output_dir"] = data["base_output_dir"].replace("/{}_".format(processing.id),"/{}_".format(next_processing.id))    
            message["processing_args"] = {"processing":"folding_test"} 
            message["data"] = {}
            message["data"]["pointings"] = []
            
            for pointing in data["data"]["pointings"]:
                current_pointing = {}
                current_pointing["id"] = pointing["id"]
                current_pointing["beams"] = []
                message["data"]["pointings"].append(current_pointing)
                for beam in pointing["beams"]:
                    current_beam = {}
                    current_beam["id"] = beam["id"]
                    current_beam["data_products"] = []
                    current_pointing["beams"].append(current_beam)
                    for dp in next_processing.inputs:
                        if dp.beam_id == beam["id"]:
                            current_beam["data_products"].append({"id":dp.id,"filename":os.path.join(dp.filepath,dp.filename)}) 
                        
            
            # Publish to Pika            
            self._opts.queue = "folding"  ## !!
            producer = pika_wrapper.pika_producer_from_opts(self._opts)
            producer.publish(json.dumps(message))


                       

    def on_fail(self):
        with self.session() as session:
            processing = session.query(Processing).get(self._processing_id)
            processing.end_time = datetime.datetime.utcnow()
            processing.process_status = "failed"
            session.add(processing)

    @staticmethod
    def add_options(parser):
        parser.add_option('','--db', dest="database", type=str,
            help='SQLAlchemy database descriptor')


def null_pipeline(data):
    pass

#process_manager = PikaProcess(...)
#pipeline_wrapper = TrapumPipelineWrapper(..., null_pipeline)
#process_manager.process(pipeline_wrapper.on_receive)
