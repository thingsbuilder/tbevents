import json
import uuid
from datetime import datetime, timedelta
from time import sleep

from tbevents.event_bus.broker.broker import Broker
from tbevents.event_bus.utils.logger import logger
from tbevents.singleton import Singleton


class TbWorker(metaclass=Singleton):
    EVENT_STORE_NAME = "event_store"
    NEW_EVENT_TOPIC = "new_event_topic_created"
    EVENT_STREAM_NAME = "event_stream"

    def __init__(self, worker_name: str, broker_provider=None, health_check_period=30000,
                 health_check_period_on_critical_error=30000, announce_new_topic=False):
        if len(worker_name) > 50:
            raise ValueError('Max worker name length: 50')
        self.worker_name = worker_name
        self.broker = Broker(broker_provider)
        self.health_check_period = health_check_period
        self.default_health_check_period = health_check_period
        self.health_check_period_on_critical_error = health_check_period_on_critical_error
        self._listening_events = {}
        self._publish_events = []
        self._health_check_functions = []
        self._validators = {}
        self.can_execute = 0
        self.announce_new_topic = announce_new_topic
        self.last_health_check = datetime.now() - timedelta(
            milliseconds=self.health_check_period + 1)  # force first health check

        self.create_new_event_topic()

    def append_health_check_function(self, func_name, retry_period=5000):
        from inspect import signature
        if signature(func_name).return_annotation is not bool:
            raise TypeError(f"{func_name} must return a boolean")
        self._health_check_functions.append(
            {'func': func_name, 'retry_period': retry_period, 'last_check': datetime.now() - timedelta(days=1)})
        if self.default_health_check_period > retry_period:
            self.health_check_period = retry_period
            logger.debug(f"New health check interval: {retry_period}")

    def append_listening_event(self, event_name, func_name, validation_model=None):
        '''

        :param event_name:
        :param func_name: callback function to process incoming message. Should return False ONLY if a message must be reprocess.
        :param validation_model: a pydantic validation class for the payload message. If you pass a validation model,
               func_name must accept at least 2 arguments: the event message and the pydantic object (already validated)
        :return:
        '''
        from inspect import signature
        if signature(func_name).return_annotation is not bool:
            raise TypeError(f"{func_name} must return a boolean")
        if validation_model is not None and len(signature(func_name).parameters) < 2:
            raise TypeError("If you expect validate a message you must accept the validate object in callback function")
        queue_name = f"{event_name.lower()}/{self.worker_name}"
        self._listening_events[queue_name] = func_name
        self._validators[queue_name] = validation_model
        if event_name.lower() is not self.NEW_EVENT_TOPIC and self.worker_name == "event_store":
            return
        self.broker.append_to_topic(topic_name=event_name.lower(),
                                    queue_name=queue_name)

    def _register_event_store(self, event_name):
        if self.worker_name != "event_store":
            # register worker to listen this event
            queue_name = f"{event_name.lower()}/{self.EVENT_STORE_NAME}"
            self.broker.append_to_topic(topic_name=event_name.lower(),
                                        queue_name=queue_name)
            # inform worker that we created a new event topic
            self.broker.append_to_topic(topic_name=self.NEW_EVENT_TOPIC,
                                        queue_name=f"{self.NEW_EVENT_TOPIC}/{self.EVENT_STORE_NAME}")

            enveloped_message = self._build_event_payload(event_name="new_topic_published",
                                                          aggregate_id=str(uuid.uuid4()),
                                                          aggregate_type="event_store",
                                                          payload={"queue_name": queue_name}, )

            self.broker.send_message(message=enveloped_message, topic=self.NEW_EVENT_TOPIC)

    def append_publish_event(self, event_name):
        if event_name.lower() == self.EVENT_STREAM_NAME:
            raise ValueError("Publish on Event Stream is a internal function")
        try:
            self._publish_events.append(event_name)
            self.broker.declare_topic(event_name.lower())
            if self.announce_new_topic:
                self._register_event_store(event_name)
        except Exception as e:
            logger.error(str(e))
            raise e

    def _build_event_payload(self, event_name, aggregate_id, aggregate_type, payload):
        return {
            "event_name": event_name,
            "aggregate_id": aggregate_id,
            "aggregate_type": aggregate_type,
            "sender": self.worker_name,
            "date_time": datetime.utcnow(),
            "payload": payload,
            "version": "1.0.0.0"
        }

    def send_event(self, event_name: str, aggregate_id: str, aggregate_type: str, message: dict):
        if event_name not in self._publish_events:
            raise NotImplementedError(f"Event {event_name} not declared")
        if not isinstance(message, dict):
            try:
                message = json.loads(message)
            except Exception as e:
                raise ValueError(f"{message} is not a valid JSON") from None
        if aggregate_type is None or aggregate_type == '':
            raise ValueError("Aggregate type is mandatory")
        if aggregate_id is None or aggregate_id == '':
            raise ValueError("Aggregate id is mandatory")

        enveloped_message = self._build_event_payload(event_name=event_name, aggregate_id=aggregate_id,
                                                      aggregate_type=aggregate_type, payload=message)

        try:
            self.broker.send_message(message=enveloped_message, topic=event_name.lower())
        except Exception as e:
            logger.error(str(e))
            raise e

    def health_checks(self):
        previous_status = self.can_execute
        for check in self._health_check_functions:
            current_millis = datetime.now()
            if (current_millis - check['last_check']).total_seconds() * 1000 >= check['retry_period']:
                [x for x in self._health_check_functions if x['func'] == check['func']][0][
                    'last_check'] = current_millis
                if not check['func']():
                    self.can_execute = 1
                    self.health_check_period = check["retry_period"]
                    msg = f'Health check {check["func"].__name__} return False. Stop consuming for {check["retry_period"] / 1000} seconds.'
                    if previous_status < 1:
                        logger.info(msg)
                    else:
                        logger.debug(msg)
                    return
        if self.can_execute > 0:
            self.can_execute = 0
            self.health_check_period = self.default_health_check_period
            logger.info("Resuming consuming...")

    def check_events(self) -> int:
        try:
            for event_name, function in list(self._listening_events.items()):
                process = self.broker.process_next_message(event_name, function, self._validators[event_name],
                                                           max_retry=0)
                if process is False:
                    return 1
            return 0
        except Exception as err:
            logger.error(err, exc_info=True)
            return 1  # return cause

    def start_listening(self):
        try:
            logger.info("Starting consumer...")
            while True:
                current_millis = datetime.now()
                if (current_millis - self.last_health_check).total_seconds() * 1000 >= self.health_check_period:
                    logger.debug("Health check time...")
                    self.health_checks()
                    self.last_health_check = datetime.now()
                try:
                    if self.can_execute < 1:
                        self.can_execute = self.check_events()
                        if self.can_execute > 0:
                            logger.error("Something happened on run... stop consuming")

                    sleep(0.02)
                except Exception as err:
                    self.can_execute = 2
                    self.health_check_period = self.health_check_period_on_critical_error
                    logger.critical(str(err))

        except KeyboardInterrupt:
            logger.debug('interrupted!')

    def create_new_event_topic(self):
        self.append_publish_event(self.NEW_EVENT_TOPIC)
