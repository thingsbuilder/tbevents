from tbevents.event_bus.broker.broker_settings import BrokerSettings


class FakeSettings(BrokerSettings):

    def __init__(self, host=None, user=None, password=None, port=None):
        pass

    def get_host(self):
        pass

    def get_user(self):
        pass

    def get_password(self):
        pass

    def get_port(self):
        pass

    def get_virtual_host(self):
        pass