import signal
import sys
import time
import pymongo
import logging
import json
import traceback
import socket
import datetime

log = logging.getLogger('mongo_wrapper')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)

DEFAULT_RETRIES = 3


class MongoConsumer(object):
    def __init__(self, pipline_name, collection, filter_):
        self._pipline_name = pipline_name
        self._collection = collection
        self._filter = filter_
        self._current = None
        self._sleep_time = 20
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        log.info("Signal handler called with signal {}".format(signum))
        if self._current is not None:
            log.info("Marking message as processable again")
            self._collection.update_one(
                {"_id": self._current["_id"]},
                {"$set": {
                    "eligible": True,
                    "state": "interrupted",
                    "last_state_transition": datetime.datetime.utcnow()
                }})
        sys.exit(0)

    def _get_one(self):
        message = self._collection.find_one_and_update(
            {"eligible": True, "pipeline_name": self._pipline_name},
            {"$set": {
                "eligible": False,
                "acquired_at": datetime.datetime.utcnow(),
                "acquired_by": socket.gethostname(),
                "state": "acquired",
                "last_state_transition": datetime.datetime.utcnow()
            }},
            sort=[
                ("priority", pymongo.DESCENDING),
                ("processing_id", pymongo.ASCENDING)
            ])
        return message

    def _mark_failed(self, error, stack_trace):
        max_retries = self._current.get("max_retries", DEFAULT_RETRIES)
        eligible = self._current.get("processing_attemps", 0) < max_retries
        self._collection.update_one(
            {"_id": self._current["_id"]},
            {"$set": {
                "eligible": eligible,
                "state": "failed",
                "last_state_transition": datetime.datetime.utcnow(),
                "processing_attemps": self._current.get("processing_attemps", 0) + 1,
                "last_error": str(error),
                "last_error_time": datetime.datetime.utcnow(),
                "last_stack_trace": stack_trace
            }})

    def _mark_success(self):
        self._collection.update_one(
            {"_id": self._current["_id"]},
            {"$set": {
                "eligible": False,
                "state": "complete",
                "last_state_transition": datetime.datetime.utcnow(),
                "completed_at": datetime.datetime.utcnow()
            }})

    def _update_state(self, state):
        self._collection.update_one(
            {"_id": self._current["_id"]},
            {"$set": {
                "state": state,
                "last_state_transition": datetime.datetime.utcnow(),
            }})

    def process(self, message_handler):
        while True:
            message = self._get_one()
            if message is None:
                log.info(
                    "No message received, sleeping for {} seconds".format(
                        self._sleep_time))
                time.sleep(self._sleep_time)
                continue
            else:
                self._current = message
                log.info("Received message: {}".format(message))
                try:
                    log.info("Calling handler")
                    message_handler(message, self._update_state)
                except Exception as error:
                    log.exception("Message handler failure")
                    self._mark_failed(
                        error, traceback.format_exc())
                else:
                    log.info("Message successfully processed")
                    self._mark_success()
                finally:
                    self._current = None


def add_mongo_consumer_opts(parser):
    parser.add_option('', '--mongo', dest='mongo', type=str,
                      help='MongoDB URL')
    parser.add_option('', '--pipeline', dest='pipeline', type=str,
                      help='Pipeline name')
    parser.add_option('', '--log_level', dest='log_level', type=str,
                      help='Logging level for mongo consumer logger',
                      default="INFO")


def mongo_consumer_from_opts(opts):
    log.setLevel(opts.log_level.upper())
    logging.getLogger("pymongo").setLevel("WARN")
    client = pymongo.MongoClient(opts.mongo)
    collection = client.trapum.processing_queue
    process = MongoConsumer(opts.pipeline, collection, {})
    return process


def test_process(mongo_consumer):
    """
    The point of this test is to simulate a long running handler.
    Upon receipt of a SIGINT (Ctrl-C) or SIGTERM the pika_process
    should push the current message back to the queue.
    """
    def handler(message, status_callback):
        log.info("Handler received message: {}".format(message))
        print("handler called")
        for ii in range(10):
            print("sleeping")
            time.sleep(10)
            print("updating status")
            status_callback("{}%".format(ii*10))
    mongo_consumer.process(handler)



















