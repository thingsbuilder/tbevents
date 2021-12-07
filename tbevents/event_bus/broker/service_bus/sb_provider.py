import datetime
import hashlib
import json
import uuid
from typing import Callable

from azure.servicebus import (
    ServiceBusClient,
    ServiceBusReceiver,
    ServiceBusReceivedMessage,
)
from azure.core.exceptions import ResourceNotFoundError
from azure.servicebus import ServiceBusMessage
from azure.servicebus.management import ServiceBusAdministrationClient
from tbevents.event_bus.broker.broker_provider import BrokerProvider
from tbevents.event_bus.broker.broker_settings import BrokerSettings
from tbevents.event_bus.models.event import Event
from tbevents.event_bus.utils.logger import logger


class TopicProperties(object):
    pass


class SBProvider(BrokerProvider):
    def __init__(self, broker_configuration: BrokerSettings):
        self.config = broker_configuration
        self.connection = ServiceBusClient.from_connection_string(
            self.config.get_host()
        )
        self.senders = {}

    def disconnect(self):
        pass

    def requeue(self, queue_name):
        pass

    def get_sb_subscription_name(self, subscription):
        """
            deal with Service Bus topic length (50 chars)
        """
        if len(subscription) <= 50:
            return subscription
        else:
            hash = int(hashlib.sha1(subscription.encode("utf-8")).hexdigest(), 16) % (
                10 ** 8
            )
            return subscription[0:42] + str(hash)

    def get_sb_topic_name(self, topic_name):
        """
            deal with Service Bus topic length (260 chars)
        """
        if len(topic_name) <= 260:
            return topic_name
        else:
            hash = int(hashlib.sha1(topic_name.encode("utf-8")).hexdigest(), 16) % (
                10 ** 8
            )
            return topic_name[0:259] + str(hash)

    @staticmethod
    def _validation_tb_event(
        receiver: ServiceBusReceiver,
        msg: ServiceBusReceivedMessage,
        payload: dict,
    ) -> Event:
        try:
            e = Event(**payload)
            return e
        except ValueError as ve:
            logger.error(f"Rejecting not valid event payload: {payload}")
            msg.dead_letter = "Rejecting not valid event payload"
            receiver.dead_letter_message(msg)

    @staticmethod
    def _dispatch_callback(
        receiver: ServiceBusReceiver,
        msg: ServiceBusReceivedMessage,
        e: Event,
        model_validator,
        callback,
    ):
        try:
            if model_validator is not None:
                try:
                    call_result = callback(e, model_validator(**e.payload))
                except Exception as error:
                    msg_error = f"Invalid payload for type.  Errors: {str(error)}"
                    logger.error(msg_error)
                    msg.dead_letter = msg_error
                    receiver.dead_letter_message(msg)
            else:
                call_result = callback(e)
            if call_result:
                receiver.complete_message(msg)
            else:
                logger.debug("Callback returning false")
                receiver.defer_message(msg)
                return False
        except:
            receiver.defer_message(msg)
            return False

    def process_next_message(self, queue_name, callback, model_validator, max_retry=0):
        t_q = queue_name.split("/")
        topic_name = self.get_sb_topic_name(
            f"{self.config.get_event_name_prefix()}_{t_q[0].lower()}"
        )
        subscription = t_q[1].lower()
        with self.connection.get_subscription_receiver(
            topic_name=topic_name, subscription_name=subscription
        ) as receiver:
            received_msgs = receiver.receive_messages(max_wait_time=2)
            for msg in received_msgs:
                msg_json = json.loads(str(msg.message))
                e = self._validation_tb_event(receiver, msg, msg_json)
                if not e:
                    return False
                self._dispatch_callback(receiver, msg, e, model_validator, callback)
            return True

    def get_or_create_topic(self, servicebus_mgmt_client, topic_name):
        try:
            return servicebus_mgmt_client.get_topic(topic_name)
        except ResourceNotFoundError:
            logger.info(f"Creating topic {topic_name}")
            return servicebus_mgmt_client.create_topic(
                topic_name,
                max_size_in_megabytes=5120,
                default_message_time_to_live="PT21600M",
            )

    def declare_topic(self, topic_name):
        topic_name = self.get_sb_topic_name(
            f"{self.config.get_event_name_prefix()}_{topic_name.lower()}"
        )
        with ServiceBusAdministrationClient.from_connection_string(
            self.config.get_host()
        ) as servicebus_mgmt_client:
            return self.get_or_create_topic(servicebus_mgmt_client, topic_name)

    def append_to_topic(self, topic_name, queue_name, routing_key=None):
        topic_name = self.get_sb_topic_name(
            f"{self.config.get_event_name_prefix()}_{topic_name.lower()}"
        )
        subscription = queue_name.split("/")[1].lower()

        with ServiceBusAdministrationClient.from_connection_string(
            self.config.get_host()
        ) as servicebus_mgmt_client:
            topic = self.get_or_create_topic(servicebus_mgmt_client, topic_name)
            try:
                return servicebus_mgmt_client.get_subscription(topic_name, subscription)
            except ResourceNotFoundError:
                logger.info(
                    f"Creating subscription {subscription} @ topic {topic.name}"
                )
                return servicebus_mgmt_client.create_subscription(
                    topic_name, subscription, max_delivery_count=2000
                )

    def declare_queues(self, queues_names):
        raise NotImplementedError()

    def send_message(self, message, topic, routing_key=None):
        topic_name = self.get_sb_topic_name(
            f"{self.config.get_event_name_prefix()}_{topic.lower()}"
        )
        with self.connection.get_topic_sender(topic_name=topic_name) as sender:
            sender.send_messages(ServiceBusMessage(json.dumps(message, default=str)))
