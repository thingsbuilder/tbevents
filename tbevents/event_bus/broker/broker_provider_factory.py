from tbevents.event_bus.broker.broker_configuration import DEFAULT_BROKER_CONFIGURATION_CLASS
from tbevents.event_bus.broker.broker_configuration import DEFAULT_BROKER_PROVIDER_CLASS
from tbevents.event_bus.broker.broker_provider import BrokerProvider


class BrokerProviderFactory(object):
    _BROKER_PROVIDER = None

    @classmethod
    def get_instance(
        cls,
        db_provider_class=DEFAULT_BROKER_PROVIDER_CLASS,
        broker_configuration=DEFAULT_BROKER_CONFIGURATION_CLASS,
    ):
        """
        Gets a default broker provider instance.
        Creates one if it hasn't been created yet.
        :returns: BrokerProvider
        """
        if not db_provider_class or not issubclass(db_provider_class, BrokerProvider):
            raise TypeError(
                "The db provider class must be of a subclass of {0}".format(
                    BrokerProvider.__name__
                )
            )
        if not BrokerProviderFactory._BROKER_PROVIDER:
            BrokerProviderFactory._BROKER_PROVIDER = db_provider_class(
                broker_configuration()
            )
        return BrokerProviderFactory._BROKER_PROVIDER
