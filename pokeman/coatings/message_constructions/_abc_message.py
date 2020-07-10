from pokeman.utils.custom_abc import ABCMeta, abstract_attribute


class AbstractBasicMessageConstruction(metaclass=ABCMeta):
    """
    Abstract base class for Enterprise Integration Patterns,
    in specific Message Constructions.
    """

    @abstract_attribute
    def app_id(self):
        pass

    @abstract_attribute
    def exchange(self):
        pass

    @abstract_attribute
    def routing_key(self):
        pass
