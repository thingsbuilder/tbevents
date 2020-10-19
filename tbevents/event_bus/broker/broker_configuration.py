import os

from tbevents.event_bus.broker.rabbit.rabbit_provider import RabbitMQProvider
from tbevents.event_bus.broker.rabbit.rabbit_settings import RabbitSettings
DEFAULT_BROKER_PROVIDER_CLASS = RabbitMQProvider
DEFAULT_BROKER_CONFIGURATION_CLASS = RabbitSettings
