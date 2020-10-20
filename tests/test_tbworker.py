from time import sleep

import pytest, logging
from tbevents.event_bus.broker.fake_provider.fake_provider import FakeProvider
from tbevents.event_bus.broker.fake_provider.fake_settings import FakeSettings
from tbevents.event_bus.models.event import Event
from tbevents.event_bus.utils.logger import logger
from tbevents.singleton import Singleton
from tbevents.tb_worker import TbWorker

result = {}
health_check_called = 0
health_check_return_value = False


def callback(e: Event) -> bool:
    global result
    result = e.payload
    return True


def invalid_callback(e: Event):  # bool
    pass


def callback_without_validation_parameter(e: Event) -> bool:
    pass


def health_check() -> bool:
    global health_check_called
    health_check_called += 1
    return True


def health_check_return() -> bool:
    global health_check_return_value
    return health_check_return_value


def invalid_health_check():  # ->bool
    pass


class TestTbWorker:

    def test_inject_broker_provider_(self):
        Singleton._instances = {}
        tb = TbWorker("Unit_Test", FakeProvider(FakeSettings()))
        assert 'new_event_topic_created' in tb.broker.broker_provider.topics

    def test_declare_new_event(self):
        Singleton._instances = {}
        tb = TbWorker("Unit_Test", FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        assert "new_event" in tb.broker.broker_provider.topics

    def test_listen_event(self):
        Singleton._instances = {}
        tb = TbWorker("Unit_Test", FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        tb.append_listening_event('new_event', callback)
        assert 'new_event/Unit_Test' in tb.broker.broker_provider.topics['new_event']

    def test_send_event(self):
        Singleton._instances = {}
        tb = TbWorker("Unit_Test", FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        tb.append_listening_event('new_event', callback)
        tb.send_event("new_event", "1", "aggregate", {'field': 'value'})
        assert 'field' in tb.broker.broker_provider.topics['new_event']['new_event/Unit_Test'][0]['payload']
        assert 'field' in tb.broker.broker_provider.topics['new_event']['new_event/event_store'][0]['payload']

    def test_receive_event(self):
        Singleton._instances = {}
        global result
        tb = TbWorker("Unit_Test", FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        tb.append_listening_event('new_event', callback)
        tb.send_event("new_event", "1", "aggregate", {'field': 'value'})
        tb.check_events()
        assert result == {'field': 'value'}
        logger.info(tb.broker.broker_provider.topics)

    def test_health_check(self):
        Singleton._instances = {}
        global health_check_called
        tb = TbWorker("Unit_Test", FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        tb.append_health_check_function(health_check, 100)
        assert health_check_called == 0
        sleep(0.1)
        tb.health_checks()
        sleep(0.1)
        tb.health_checks()
        assert health_check_called == 2

    def test_worker_name_50_chars_limit(self):
        Singleton._instances = {}
        name = "x" * 51
        assert len(name) > 50
        with pytest.raises(ValueError):
            tb = TbWorker(worker_name=name, broker_provider=FakeProvider(FakeSettings()))

    def test_health_check_should_return_boolean(self):
        Singleton._instances = {}
        tb = TbWorker(worker_name="Unit test", broker_provider=FakeProvider(FakeSettings()))
        with pytest.raises(TypeError):
            tb.append_health_check_function(invalid_health_check, 100)

    def test_callback_should_return_boolean(self):
        Singleton._instances = {}
        tb = TbWorker(worker_name="Unit test", broker_provider=FakeProvider(FakeSettings()))
        with pytest.raises(TypeError):
            tb.append_listening_event('new_event', invalid_callback)

    def test_callback_should_receive_validation_model(self):
        Singleton._instances = {}

        class ValidationModel:
            pass

        tb = TbWorker(worker_name="Unit test", broker_provider=FakeProvider(FakeSettings()))
        with pytest.raises(TypeError):
            tb.append_listening_event('new_event', callback_without_validation_parameter,
                                      validation_model=ValidationModel)

    def test_cant_call_append_publish_event_stream_event(self):
        Singleton._instances = {}
        tb = TbWorker(worker_name="Unit test", broker_provider=FakeProvider(FakeSettings()))
        with pytest.raises(ValueError):
            tb.append_publish_event(TbWorker.EVENT_STREAM_NAME)

    def test_cant_publish_undeclared_event(self):
        Singleton._instances = {}
        tb = TbWorker(worker_name="Unit test", broker_provider=FakeProvider(FakeSettings()))
        with pytest.raises(NotImplementedError):
            tb.send_event(event_name="test", aggregate_id="0", aggregate_type="test", message='')

    def test_message_must_be_a_json(self):
        Singleton._instances = {}
        tb = TbWorker(worker_name="Unit test", broker_provider=FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        with pytest.raises(ValueError):
            tb.send_event(event_name="new_event", aggregate_id="0", aggregate_type="test", message='')

    def test_message_must_have_aggregate_type(self):
        Singleton._instances = {}
        tb = TbWorker(worker_name="Unit test", broker_provider=FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        with pytest.raises(ValueError):
            tb.send_event(event_name="new_event", aggregate_id="0", aggregate_type='', message={'test': 0})

    def test_message_must_have_aggregate_id(self):
        Singleton._instances = {}
        tb = TbWorker(worker_name="Unit test", broker_provider=FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        with pytest.raises(ValueError):
            tb.send_event(event_name="new_event", aggregate_id='', aggregate_type='ticket', message={'test': 0})

    def test_should_stop_consuming(capsys):
        global health_check_return_value
        health_check_return_value = False
        Singleton._instances = {}
        tb = TbWorker(worker_name="Unit test", broker_provider=FakeProvider(FakeSettings()))
        tb.append_health_check_function(health_check_return, 100)
        sleep(0.1)
        tb.health_checks()
        assert tb.can_execute == 1
        health_check_return_value = True
        sleep(0.2)
        tb.health_checks()
        assert tb.can_execute == 0
