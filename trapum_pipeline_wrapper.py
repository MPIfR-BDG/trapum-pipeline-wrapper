import os
import xxhash
import datetime
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_models import Processing, Hardware, FileType, DataProduct

log = logging.getLogger('trapum_pipeline_wrapper')


class TrapumPipelineWrapper(object):
    def __init__(self, opts, pipeline_callable):
        self._pipeline_callable = pipeline_callable
        self._processing_id = None
        self._opts = opts
        self._session_engine = create_engine(opts.database, echo=False, poolclass=NullPool)
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

    def on_receive(self, message, status_callback):
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
        data = message
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
            data_products = self._pipeline_callable(data, status_callback)
            self.on_success(data_products)
        except Exception as error:
            log.exception("Error from pipeline: {}".format(str(error)))
            self.on_fail()
            raise error

    def on_success(self, data_products):
        '''
         required from pipeline: Filetype, filename, beam id , pointing id, directory

        '''

        with self.session() as session:
            processing = session.query(Processing).get(self._processing_id)
            now = datetime.datetime.utcnow()
            processing.end_time = now

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
                    metainfo=dp["metainfo"],
                    filehash=filehash,
                    available=True,
                    locked=True
                    )
                session.add(data_product)
            processing.process_status = "success"
            session.add(processing)

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
