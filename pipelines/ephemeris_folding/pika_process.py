import signal
import sys
import time
import pika
import logging
import json



log = logging.getLogger('pikaprocess')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)

class PikaChannel(object):
    def __init__(self, host, port, user, password, vhost):
        self._connection = None
        self._channel = None
        self._queues = []
        self._credentials = pika.PlainCredentials(user, password)
        self._parameters = pika.ConnectionParameters(host, port, vhost, self._credentials)
        
    def add_queue(self, **opts):
        log.info("Registering queue with options: {}".format(opts))
        self._queues.append(opts)
    
    def __enter__(self):
        log.debug("Establishing RabbitMQ connection")
        self._connection = pika.BlockingConnection(self._parameters)
        self._channel = self._connection.channel()
        for queue in self._queues:
            log.debug("Declaring RabbitMQ queue: {}".format(queue))
            self._channel.queue_declare(**queue)
        return self._channel

    def __exit__(self, type, value, traceback):
        log.debug("Closing RabbitMQ connection")
        self._connection.close()
   

class PikaProcess(object):
    def __init__(self, host, port, user, pwd, vhost,
                 input_q_params, success_q_params,
                 fail_q_params, sleep_time=30):
        self._current = None
        self._current_priority = 0
        self._sleep_time = sleep_time
        self._channel_manager = PikaChannel(host, port, user, pwd, vhost)
        self._input_q_params = input_q_params
        self._success_q_params = success_q_params
        self._fail_q_params = fail_q_params
        self._channel_manager.add_queue(**self._input_q_params)
        self._channel_manager.add_queue(**self._success_q_params)
        self._channel_manager.add_queue(**self._fail_q_params)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        log.info("Signal handler called with signal {}".format(signum))
        if self._current is not None:
            log.info("Returning current message, '{}', to the input queue with priority {}".format(
                self._current, self._current_priority+1))
            self._return_to_input(self._current)
        sys.exit(0)
            
    def _get_input_message(self):
        with self._channel_manager as channel:
            method_frame, header_frame, body = channel.basic_get(queue=self._input_q_params['queue'])
            if method_frame is not None:
                if method_frame.NAME != 'Basic.GetEmpty':
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    return method_frame, header_frame, body
        return None, None, None

    def _send_success_message(self, message):
        with self._channel_manager as channel:
            channel.basic_publish(exchange='', routing_key=self._success_q_params["queue"], body=message,
                                  properties=pika.BasicProperties(delivery_mode = 2, priority=0))

    def _send_fail_message(self, message):
        with self._channel_manager as channel:
            channel.basic_publish(exchange='', routing_key=self._fail_q_params["queue"], body=message,
                                  properties=pika.BasicProperties(delivery_mode = 2, priority=0))

    def _return_to_input(self, message):
        with self._channel_manager as channel:
            channel.basic_publish(exchange='', routing_key=self._input_q_params["queue"], body=message,
                                  properties=pika.BasicProperties(delivery_mode = 2, priority=self._current_priority+1))
            
    def process(self, message_handler):
        while True:
            mf, hf, message = self._get_input_message()
            if message is None:
                log.info("No message received, going to sleep for {} seconds".format(self._sleep_time))
                time.sleep(self._sleep_time)
                continue
            else:
                self._current = message
                self._current_priority = hf.priority
                log.info("Received message: '{}' with priority ".format(message,hf.priority))
                try:
                    log.info("Calling handler")
                    message_handler(message)
                except Exception as error:
                    log.exception("Message handler failure")
                    self._send_fail_message(message)
                else:
                    log.info("Message successfully processed")
                    self._send_success_message(message)
                finally:
                    self._current = None
                    self._current_priority = 0

def add_pika_process_opts(parser):
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='RabbitMQ host', default="10.244.34.10")
    parser.add_option('-p', '--port', dest='port', type=int,
                      help='RabbitMQ port', default=5672)
    parser.add_option('-u', '--user', dest='user', type=str,
                     help='RabbitMQ username', default="guest")
    parser.add_option('-k', '--password', dest='password', type=str,
                      help='RabbitMQ password', default="guest")
    parser.add_option('', '--vhost', dest='vhost', type=str,
                      help='RabbitMQ vhost', default="/")
    parser.add_option('', '--input', dest='input_queue', type=str,
                      help='Name of input queue', default="test-input")
    parser.add_option('', '--success', dest='success_queue', type=str,
                      help='Name of success queue', default="test-success")
    parser.add_option('', '--fail', dest='fail_queue', type=str,
                      help='Name of fail queue', default="test-fail")
    parser.add_option('', '--sleep_time', dest='sleep_time', type=float,
                      help='Time to sleep when input queue is empty',
                      default=30.0)
    parser.add_option('', '--log_level',dest='log_level',type=str,
                      help='Logging level for pikaprocess logger', default="INFO")


class PikaProducer(object):
    def __init__(self, host, port, user, pwd, vhost, queue_params):
        self._channel_manager = PikaChannel(host, port, user, pwd, vhost)
        self._queue_params = queue_params
        self._channel_manager.add_queue(**self._queue_params)

    def publish(self, messages, priority=0):
        with self._channel_manager as channel:
            if hasattr(messages,"__iter__") and not isinstance(messages,(str,bytes)):
                for message in messages:
                    log.info("Publishing message '{}' to queue '{}'".format(message,self._queue_params["queue"]))
                    channel.basic_publish(exchange='', routing_key=self._queue_params["queue"], body=message,
                                          properties=pika.BasicProperties(delivery_mode = 2, priority=priority))
            else:
                log.info("Publishing message '{}' to queue '{}'".format(messages,self._queue_params["queue"]))
                channel.basic_publish(exchange='', routing_key=self._queue_params["queue"], body=messages,
                                      properties=pika.BasicProperties(delivery_mode = 2, priority=priority))

def add_pika_producer_opts(parser):
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='RabbitMQ host', default="10.244.34.10")
    parser.add_option('-p', '--port', dest='port', type=int,
                      help='RabbitMQ port', default=5672)
    parser.add_option('-u', '--user', dest='user', type=str,
                      help='RabbitMQ username', default="guest")
    parser.add_option('-k', '--password', dest='password', type=str,
                      help='RabbitMQ password', default="guest")
    parser.add_option('', '--vhost', dest='vhost', type=str,
                      help='RabbitMQ vhost', default="/")
    parser.add_option('-q', '--queue', dest='queue', type=str,
                      help='Name of queue to publish to', default="beam_merge")
    parser.add_option('', '--log_level',dest='log_level',type=str,
                      help='Logging level for pikaprocess logger', default="INFO")
    
                
def pika_producer_from_opts(opts):
    log.setLevel(opts.log_level.upper())
    logging.getLogger("pika").setLevel("WARN")
    producer = PikaProducer(opts.host, opts.port,
                           opts.user, opts.password,
                           opts.vhost,
                           {"queue":opts.queue, "durable": True, "arguments":{"x-max-priority":10}})
    return producer

def pika_process_from_opts(opts):
    log.setLevel(opts.log_level.upper())
    logging.getLogger("pika").setLevel("WARN")
    process = PikaProcess(opts.host, opts.port,
                          opts.user, opts.password,
                          opts.vhost,
                          {"queue":opts.input_queue, "durable": True, "arguments":{"x-max-priority":10}},
                          {"queue":opts.success_queue, "durable": True, "arguments":{"x-max-priority":10}},
                          {"queue":opts.fail_queue, "durable": True, "arguments":{"x-max-priority":10}},
                          opts.sleep_time)
    return process

def test_process(pika_process):
    """
    The point of this test is to simulate a long running handler.
    Upon receipt of a SIGINT (Ctrl-C) or SIGTERM the pika_process
    should push the current message back to the queue.
    """
    def handler(message):
        log.info("Handler received message: {}".format(message))
        time.sleep(1000000)
    pika_process.process(handler)

def publish_info(opts,info):
    producer = pika_producer_from_opts(opts)
    message = json.dumps(info)
    log.debug("Sending info to the user's info queue %s"%message)
    try:
        producer.publish(message)
        log.info("Successfully sent message")
    except Exception as e:
        log.error(e)






if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    #add_pika_process_opts(parser)
    add_pika_producer_opts(parser)
    opts,args = parser.parse_args()
    #process = pika_process_from_opts(opts)
    #test(process)
    producer = pika_producer_from_opts(opts)
    producer.publish(["these are a","few messages"])



    
