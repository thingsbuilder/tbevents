from abc import ABC, abstractmethod


class BrokerProvider(ABC):

    @abstractmethod
    def append_to_topic(self, topic_name, queue_name, routing_key=None):
        pass

    @abstractmethod
    def declare_topic(self, topic_name):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def process_next_message(self, queue_name, callback, max_retry=5):
        pass

    @abstractmethod
    def send_message(self, message, topic):
        pass


