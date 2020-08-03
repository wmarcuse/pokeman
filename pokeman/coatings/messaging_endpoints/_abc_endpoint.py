from pokeman.utils.custom_abc import ABCMeta, abstract_attribute


# TODO: Add more arguments https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.basic_consume
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
