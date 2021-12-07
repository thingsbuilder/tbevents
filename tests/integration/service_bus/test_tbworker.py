import pytest

from tbevents.event_bus.broker.service_bus.sb_provider import SBProvider
from tbevents.event_bus.broker.service_bus.sb_settings import SBSettings
from tbevents.event_bus.models.event import Event
from tbevents.singleton import Singleton
from tbevents.tb_worker import TbWorker


def callback(e: Event) -> bool:
    global result
    result = e.payload
    return True


class TestServiceBus:
    @pytest.fixture
    def worker(self):
        Singleton._instances = {}
        return TbWorker("Service_Bus", SBProvider(SBSettings()))

    def test_inject_service_bus_provider(self, worker):
        assert 'new_event_topic_created' in worker.NEW_EVENT_TOPIC

    def test_declare_new_event(self, worker):
        worker.append_publish_event("new_event")

    def test_send_event(self, worker):
        global result
        worker.append_publish_event("pubsub")
        worker.send_event("pubsub", "123", "pubsub", {"test": "send message"})
        worker.check_events()
        assert result == {"test": "send message"}