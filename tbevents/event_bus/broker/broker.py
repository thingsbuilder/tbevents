# -*- coding: utf-8 -*-

from typing import List
from tbevents.event_bus.broker.broker_provider_factory import BrokerProviderFactory


class Broker:
    def __init__(self, broker_provider=None):
        if broker_provider is None:
            self.broker_provider = BrokerProviderFactory.get_instance()
        else:
            self.broker_provider = broker_provider

    def declare_topic(self, topic_name):
        self.broker_provider.declare_topic(topic_name)

    def append_to_topic(self, topic_name, queue_name, routing_key=None):
        self.broker_provider.append_to_topic(topic_name, queue_name, routing_key=routing_key)

    def declare_queues(self, queues_names: List):
        self.broker_provider.declare_queues(queues_names)

    def send_message(self, message, topic):
        self.broker_provider.send_message(message, topic)

    def requeue(self, queue):
        self.broker_provider.requeue(queue)

    def process_next_message(self, channel_name, callback, model_validator, max_retry=5):
        return self.broker_provider.process_next_message(channel_name, callback, model_validator, max_retry=max_retry)
