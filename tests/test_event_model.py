import pytest

from tbevents.event_bus.models.event import Event

valid_event = {
    "_id": "5f78b9a370715def302b0ce0",
    "event_name": "customer_order_received",
    "aggregate_id": "654127",
    "aggregate_type": "customer_order",
    "sender": "frontend",
    "date_time": "2020-10-03T17:49:23.410Z",
    "payload": {},
    "version": "1.0.0.0"
}


class TestEvent:

    def test_should_valid_event_payload(self):
        Event(**valid_event)

    def test_should_have_version(self):
        invalid_event = valid_event.copy()
        invalid_event.pop("version")
        with pytest.raises(ValueError):
            Event(**invalid_event)

    def test_should_have_payload(self):
        invalid_event = valid_event.copy()
        invalid_event.pop("payload")
        with pytest.raises(ValueError):
            Event(**invalid_event)

    def test_should_have_date_time(self):
        invalid_event = valid_event.copy()
        invalid_event.pop("date_time")
        with pytest.raises(ValueError):
            Event(**invalid_event)

    def test_should_have_sender(self):
        invalid_event = valid_event.copy()
        invalid_event.pop("sender")
        with pytest.raises(ValueError):
            Event(**invalid_event)

    def test_should_have_aggregate_type(self):
        invalid_event = valid_event.copy()
        invalid_event.pop("aggregate_type")
        with pytest.raises(ValueError):
            Event(**invalid_event)

    def test_should_have_aggregate_id(self):
        invalid_event = valid_event.copy()
        invalid_event.pop("aggregate_id")
        with pytest.raises(ValueError):
            Event(**invalid_event)

    def test_should_have_event_name(self):
        invalid_event = valid_event.copy()
        invalid_event.pop("event_name")
        with pytest.raises(ValueError):
            Event(**invalid_event)

