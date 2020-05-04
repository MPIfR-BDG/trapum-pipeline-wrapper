
import json
import datetime
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_models import Processing, Hardware

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
            "metadata":
            {
                "proessing_id":2,
            }
            "arguments":
            {
                "dm_low": 1.3,
                "dm_high": 100.03
            }
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
            self._pipeline_callable(data)
        except Exception as error:
            log.exception("Error from pipeline: {}".format(str(error)))
            self.on_fail()
            raise error

    def on_success(self):
        with self.session() as session:
            self._processing.end_time = datetime.datetime.utcnow()
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
