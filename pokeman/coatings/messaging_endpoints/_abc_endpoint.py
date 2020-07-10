from pokeman.utils.custom_abc import ABCMeta, abstract_attribute


class AbstractBasicMessagingEndpoint(metaclass=ABCMeta):
    """
    Abstract base class for Enterprise Integration Patterns,
    in specific Messaging Endpoints.
    """

    @abstract_attribute
    def exchange(self):
        pass

    @abstract_attribute
    def queue(self):
        pass

    @abstract_attribute
    def callback_method(self):
        pass

    @abstract_attribute
    def qos(self):
        pass
