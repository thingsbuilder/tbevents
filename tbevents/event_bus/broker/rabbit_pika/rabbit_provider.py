import pika
import json
from pika.exchange_type import ExchangeType

from tbevents.event_bus.broker.broker_provider import BrokerProvider
from tbevents.event_bus.broker.broker_settings import BrokerSettings
from tbevents.event_bus.models.event import Event
from tbevents.event_bus.utils.logger import logger
from time import sleep


class RabbitPikaMQProvider(BrokerProvider):
    def __init__(self, broker_configuration: BrokerSettings):
        self.last_message_tag = {}
        self.config = broker_configuration

        self._params = pika.connection.ConnectionParameters(
            host=self.config.get_host(),
            port=self.config.get_port(),
            virtual_host=self.config.get_virtual_host(),
            credentials=pika.credentials.PlainCredentials(self.config.get_user(), password=self.config.get_password()))
        self._conn = None
        self._channel = None

    def connect(self):
        if not self._conn or self._conn.is_closed:
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()

    def establish_connection(self):
        revived_connection = self.connection.clone()
        revived_connection.ensure_connection(max_retries=3)
        return revived_connection

    def disconnect(self):
        if self._conn and self._conn.is_open:
            logger.debug('closing queue connection')
            self._conn.close()

    def rabbit_disconnected(self, queue_name):
        pass

    def get_simple_queue(self, queue_name):
        pass

    def process_next_message(self, queue_name, callback, model_validator, max_retry=0):
        pass

    def _declare_topic(self, topic_name):
        self._channel.exchange_declare(exchange=f"{self.config.get_event_name_prefix()}{topic_name}",
                                       exchange_type=ExchangeType.topic,
                                       durable=True
                                       )

    def declare_topic(self, topic_name):
        try:
            if self._channel is None:
                self.connect()
            self._declare_topic(topic_name)
        except pika.exceptions.ConnectionClosed:
            logger.debug('reconnecting to rabbit')
            self.connect()
            self._declare_topic(topic_name)

    def append_to_topic(self, topic_name, queue_name, routing_key=None):
        pass

    def _publish(self, message, topic):
        properties = pika.BasicProperties(
            content_type='application/json', priority=0, delivery_mode=2,
            content_encoding="utf-8")
        logger.debug(f"Trying send message{message}")
        self._channel.basic_publish(exchange=topic,
                                    routing_key="",
                                    body=json.dumps(message, indent = 4, sort_keys = True, default = str).encode(),
                                    properties=properties)
        logger.debug('message sent: %s', message)

    def send_message(self, message, topic):

        logger.debug(f"Insert data on TOPIC: {topic}")

        if not topic.startswith(self.config.get_event_name_prefix()):
            topic = f"{self.config.get_event_name_prefix()}{topic}"

        try:
            self._publish(message, topic)
        except pika.exceptions.ConnectionClosed:
            logger.debug('reconnecting to queue')
            self.connect()
            self._publish(message, topic)
