from tbevents.event_bus.broker.broker_provider import BrokerProvider
from tbevents.event_bus.broker.broker_settings import BrokerSettings
from tbevents.event_bus.utils.logger import logger


class FakeProvider(BrokerProvider):

    def __init__(self, broker_configuration: BrokerSettings):
        self.topics = {}
        self.queues = {}
        print("teste")

    def declare_topic(self, topic_name):
        if topic_name not in self.topics:
            self.topics[topic_name] = {}

    def append_to_topic(self, topic_name, queue_name, routing_key=None):
        self.declare_topic(topic_name)
        if queue_name not in self.topics:
            self.topics[topic_name][queue_name] = []

    def disconnect(self):
        pass

    def process_next_message(self, queue_name, callback, max_retry=5):
        topic, queue_name = queue_name.split("/")
        if topic in self.topics and queue_name in self.topics[topic] and len(self.topics[topic][queue_name]) > 0:
            print(self.topics[topic][queue_name].pop(0))

    def send_message(self, message, topic):
        for queue in self.topics[topic]:
            self.topics[topic][queue].append(message)
            logger.info(f"Fake_provider: message {message} sended to topic {topic}")
