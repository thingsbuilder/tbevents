import os
from tbevents.event_bus.broker.service_bus.sb_provider import SBProvider
from tbevents.event_bus.broker.service_bus.sb_settings import SBSettings
from tbevents.event_bus.broker.rabbit.rabbit_provider import RabbitMQProvider
from tbevents.event_bus.broker.rabbit.rabbit_settings import RabbitSettings

if os.environ.get("PROVIDER") == "RabbitMQ":
    DEFAULT_BROKER_PROVIDER_CLASS = RabbitMQProvider
    DEFAULT_BROKER_CONFIGURATION_CLASS = RabbitSettings
else:
    DEFAULT_BROKER_PROVIDER_CLASS = SBProvider
    DEFAULT_BROKER_CONFIGURATION_CLASS = SBSettings
