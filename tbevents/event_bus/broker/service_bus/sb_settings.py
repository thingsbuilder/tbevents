import os

from tbevents.event_bus.broker.broker_settings import BrokerSettings


class SBSettings(BrokerSettings):
    def __init__(
        self,
        host=None,
        user=None,
        password=None,
        port=None,
        virtual_host="/",
        prefix="",
    ):
        if host is None:
            self.host = os.environ.get("EVENTS_SB_ENDPOINT", "").replace('"', "")
        self.prefix = os.environ.get("PREFIX", prefix)
        self._validate_host()

    def get_host(self):
        return self.host

    def get_user(self):
        raise NotImplementedError()

    def get_password(self):
        raise NotImplementedError()

    def get_port(self):
        raise NotImplementedError()

    def get_virtual_host(self):
        raise NotImplementedError()

    def get_event_name_prefix(self):
        return self.prefix

    def _validate_host(self):
        if self.host is None:
            raise ValueError("servicebus_endpoint1 should not be empty")
