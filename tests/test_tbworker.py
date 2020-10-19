import pytest
from tbevents.event_bus.broker.fake_provider.fake_provider import FakeProvider
from tbevents.event_bus.broker.fake_provider.fake_settings import FakeSettings
from tbevents.event_bus.utils.logger import logger
from tbevents.tb_worker import TbWorker


def callback(event) -> bool:
    print(event)


class TestTbWorker:

    def test_inject_broker_provider_(self):
        tb = TbWorker("Unit_Test", FakeProvider(FakeSettings()))
        assert tb.broker.broker_provider.topics == {}
        # assert 'NEW_EVENT_TOPIC_CREATED' in tb.broker.broker_provider.topics

    def test_declare_new_event(self):
        tb = TbWorker("Unit_Test", FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        assert "new_event" in tb.broker.broker_provider.topics

    def test_listen_event(self):
        tb = TbWorker("Unit_Test", FakeProvider(FakeSettings()))
        tb.append_publish_event("new_event")
        tb.append_listening_event('new_event', callback)

        assert 'new_event/UNIT_TEST' in tb.broker.broker_provider.topics['new_event']
