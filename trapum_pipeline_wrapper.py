
import json
import datetime
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_models import Processing, Hardware, FileType, DataProduct

log = logging.getLogger('trapum_pipeline_wrapper')


class TrapumPipelineWrapper(object):
    def __init__(self, database, pipeline_callable):
        self._pipeline_callable = pipeline_callable
        self._processing = None
        self._session_engine = create_engine(database, echo=False)
        self._session_factory = sessionmaker(
            bind=self._session_engine, poolclass=NullPool)
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
            hardware = session(Hardware).query.filter(
                Hardware.name == "dave",
                )
            if hardware:
                self._hardware_id = hardware.id
            else:
                hardware = Hardware(...)
                session.add(hardware)
                session.flush()
                self._hardware_id = hardware.id

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
        data = json.loads(message)
        with self.session() as session:
            self._processing = session(Processing).query.get(data["processing_id"])
            if self._processing is None:
                raise Exception("No Processing entry with ID = {}".format(
                    data["processing_id"]))
            self._processing.start_time = datetime.datetime.utcnow()
            self._processing.process_status = "running"
            session.add(self._processing)
        try:
            data_products = self._pipeline_callable(data)
            self.on_success(data_products)
        except Exception as error:
            log.exception("Error from pipeline: {}".format(str(error)))
            self.on_fail()
            raise error

    def on_success(self, data_products):
        with self.session() as session:
            now = datetime.datetime.utcnow()
            self._processing.end_time = now
            for dp in data_products:
                ft = session(FileType).query.filter(
                    FileType.name.like_(dp['type'])).first()
                if ft is None:
                    ft = FileType(name=dp['type'], description="unknown")
                    session.add(ft)
                    session.flush()
                data_product = DataProduct(
                    filename=dp['filename'],
                    filepath=dp['directory'],
                    upload_date=now,
                    modification_date=now,
                    file_type_id=ft.id,
                    beam_id=self._data["metadata"]["beam_ids"][0],
                    pointing_id=self._data["metadata"]["pointing_ids"],
                    processing_id=self._processing.id
                    )
                session.add(data_product)
            self._processing.process_status = "success"
            session.add(self._processing)

    def on_fail(self):
        with self.session() as session:
            self._processing.end_time = datetime.datetime.utcnow()
            self._processing.process_status = "failed"
            session.add(self._processing)

    def add_options(self, parser):
        parser.add_option('','--db', dest="database", type=str,
            help='SQLAlchemy database descriptor')


def null_pipeline(data):
    pass

process_manager = PikaProcess(...)
pipeline_wrapper = TrapumPipelineWrapper(..., null_pipeline)
process_manager.process(pipeline_wrapper.on_receive)
