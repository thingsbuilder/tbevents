import os

if os.environ.get("PROVIDER") == "Service Bus":
    from tbevents.event_bus.broker.service_bus.sb_provider import SBProvider
    from tbevents.event_bus.broker.service_bus.sb_settings import SBSettings
    DEFAULT_BROKER_PROVIDER_CLASS = SBProvider
    DEFAULT_BROKER_CONFIGURATION_CLASS = SBSettings
else:
    from tbevents.event_bus.broker.rabbit.rabbit_provider import RabbitMQProvider
    from tbevents.event_bus.broker.rabbit.rabbit_settings import RabbitSettings
    DEFAULT_BROKER_CONFIGURATION_CLASS = RabbitSettings
    DEFAULT_BROKER_PROVIDER_CLASS = RabbitMQProvider
