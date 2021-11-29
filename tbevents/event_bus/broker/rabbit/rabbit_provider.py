from queue import Empty

from kombu import Connection, Queue, Exchange, Producer

from tbevents.event_bus.broker.broker_provider import BrokerProvider
from tbevents.event_bus.broker.broker_settings import BrokerSettings
from tbevents.event_bus.models.event import Event
from tbevents.event_bus.utils.logger import logger
from time import sleep


class RabbitMQProvider(BrokerProvider):
    def __init__(self, broker_configuration: BrokerSettings):
        self.last_message_tag = {}
        self.config = broker_configuration

        self.connection = Connection(hostname=self.config.get_host(),
                                     port=self.config.get_port(),
                                     userid=self.config.get_user(),
                                     password=self.config.get_password(),
                                     virtual_host=self.config.get_virtual_host(),
                                     heartbeat=0, connect_timeout=30)
        self.connection.connect()

        self.send_connection = Connection(hostname=self.config.get_host(),
                                          port=self.config.get_port(),
                                          userid=self.config.get_user(),
                                          password=self.config.get_password(),
                                          virtual_host=self.config.get_virtual_host(),
                                          connect_timeout=30, heartbeat=0)
        self.queues = {}

    def establish_connection(self):
        revived_connection = self.connection.clone()
        revived_connection.ensure_connection(max_retries=3)
        return revived_connection

    def disconnect(self):
        self.connection.close()

    def rabbit_disconnected(self, queue_name):
        return self.queues[queue_name].consumer.connection is None or not self.queues[
            queue_name].consumer.connection.connected

    def get_simple_queue(self, queue_name):
        if not queue_name in self.queues or self.rabbit_disconnected(queue_name):
            conn = self.establish_connection()
            self.queues[queue_name] = conn.SimpleQueue(
                name=Queue(name=queue_name, channel=conn))

        return self.queues[queue_name]

    def process_next_message(self, queue_name, callback, model_validator, max_retry=0):
        sub_queue = self.get_simple_queue(queue_name)
        retry_count = 0
        while True:
            try:
                msg = sub_queue.get(block=False, timeout=20)
                try:
                    e = Event(**msg.payload)
                except ValueError as ve:
                    logger.error(f"Rejecting not valid event payload: {msg.payload}")
                    msg.ack()
                    return True
                try:
                    if model_validator is not None:
                        try:
                            call_result = callback(e, model_validator(**e.payload))
                        except Exception as error:
                            logger.error(f"Invalid payload for type.  Errors: {str(error)}")
                            msg.ack()
                            return True
                    else:
                        call_result = callback(e)
                    if call_result:
                        msg.ack()
                        return True
                    else:
                        logger.debug("Callback returning false")
                        msg.requeue()
                        return False
                except:
                    msg.requeue()
                    return False
            except IOError:
                logger.error("Lost connection with Rabbit")
                self.queues = {}
                return False
            except Empty as e:
                if retry_count < max_retry:
                    sleep(0.1)
                    retry_count += 1
                else:
                    return True

    def declare_topic(self, topic_name):
        with self.connection as _conn:
            _conn.connect()
            channel = _conn.channel()
            topic = Exchange(
                name=f"{self.config.get_event_name_prefix()}{topic_name}", type="topic", channel=channel
            )
            topic.declare()

    def append_to_topic(self, topic_name, queue_name, routing_key=None):
        with self.connection as _conn:
            _conn.connect()
            channel = _conn.channel()

            if not topic_name.startswith(self.config.get_event_name_prefix()):
                topic_name = f"{self.config.get_event_name_prefix()}{topic_name}"

            topic = Exchange(
                name=f"{topic_name}", type="topic", channel=channel
            )
            topic.declare()

            queue = Queue(name=queue_name, channel=channel, routing_key=routing_key)
            queue.declare()

            queue.bind_to(exchange=f"{topic_name}", routing_key=routing_key)

    def send_message(self, message, topic):
        # with self.send_connection as _conn:
        _conn = self.send_connection
        _conn.connect()
        # channel = _conn.channel()
        with _conn.channel() as channel:
            producer = Producer(channel)

            logger.debug(f"Insert data on TOPIC: {topic}")

            if not topic.startswith(self.config.get_event_name_prefix()):
                topic = f"{self.config.get_event_name_prefix()}{topic}"

            producer.publish(body=message, exchange=topic, routing_key=None)

            logger.debug(f"Message {message} sent to topic {topic}!")
