from abc import ABC, abstractmethod
from uuid import uuid4

# Message Constructions
from .message_constructions import BasicMessage
from.messaging_endpoints import PollingConsumer, BasicMessageConsumer, SelectiveConsumer
from .resolver_attribute_methods.selective_consumer import start, on_message

import logging

LOGGER = logging.getLogger(__name__)

# TODO: Methods get_app_id and set_channel to abstract Resolvers instead of Resolver


class AbstractMessageConstructionResolver(ABC):
    """
    Abstract base clase for a EIP Message Construction resolver.
    """
    def __init__(self, coating):
        self.coating = coating
        self.blueprint = {
            'type': None,
            'app_id': None,
        }
        self.resolve_coating()

    def resolve_coating(self):
        self.get_build_type()
        self.get_app_id()
        self.set_exchange()
        self.set_routing_key()
        self.set_response_queue()

    def get_build_type(self):
        if 'Resolver' not in self.__class__.__name__:
            raise NameError('Resolver not found in {CLASS_NAME}'.format(CLASS_NAME=self.__class__.__name__))
        self.blueprint['type'] = self.__class__.__name__.replace('Resolver', '')

    @abstractmethod
    def get_app_id(self):
        pass

    @abstractmethod
    def set_exchange(self):
        pass

    @abstractmethod
    def set_routing_key(self):
        pass

    def set_response_queue(self):
        pass


class AbstractMessagingEndpointResolver(ABC):
    """
    Abstract base clase for a EIP Messaging Endpoint resolver.
    """
    def __init__(self, coating):
        self.coating = coating
        self.blueprint = {
            'type': None,
            'app_id': None
        }
        self.resolve_coating()

    def resolve_coating(self):
        self.get_build_type()
        self.set_exchange()
        self.set_queue()
        self.set_callback_method()
        self.set_qos()

    def get_build_type(self):
        if 'Resolver' not in self.__class__.__name__:
            raise NameError('Resolver not found in {CLASS_NAME}'.format(CLASS_NAME=self.__class__.__name__))
        self.blueprint['type'] = self.__class__.__name__.replace('Resolver', '')

    @abstractmethod
    def set_exchange(self):
        pass

    @abstractmethod
    def set_queue(self):
        pass

    @abstractmethod
    def set_callback_method(self):
        pass

    @abstractmethod
    def set_qos(self):
        pass

    def set_response_queue(self):
        pass


class BasicMessageResolver(AbstractMessageConstructionResolver):
    """
    Template resolver for EIP Message Construction > BasicMessage
    """
    def get_app_id(self):
        """
        This method sets the app id in the blueprint.
        """
        self.blueprint['app_id'] = self.coating.app_id

    def set_exchange(self):
        """
        This methods sets the exchange in the blueprint.
        """
        self.blueprint['exchange'] = self.coating.exchange

    def set_routing_key(self):
        """
        This method sets the routing key in the blueprint
        """
        self.blueprint['routing_key'] = self.coating.routing_key


class PollingConsumerResolver(AbstractMessagingEndpointResolver):
    """
    Template resolver for EIP Messaging Endpoint > PollingConsumer
    """

    def set_exchange(self):
        """
        This methods sets the exchange in the blueprint.
        """
        self.blueprint['exchange'] = self.coating.exchange

    def set_queue(self):
        self.blueprint['queue'] = self.coating.queue

    def set_callback_method(self):
        self.blueprint['callback_method'] = self.coating.callback_method

    def set_qos(self):
        self.blueprint['qos'] = self.coating.qos


class BasicMessageConsumerResolver(AbstractMessagingEndpointResolver):
    """
    Template resolver for EIP Messaging Endpoint > BasicMessageConsumer
    """

    def set_exchange(self):
        """
        This methods sets the exchange in the blueprint.
        """
        self.blueprint['exchange'] = self.coating.exchange

    def set_queue(self):
        self.blueprint['queue'] = self.coating.queue

    def set_callback_method(self):
        self.blueprint['callback_method'] = self.coating.callback_method

    def set_qos(self):
        self.blueprint['qos'] = self.coating.qos


class SelectiveConsumerResolver(BasicMessageConsumerResolver):
    """
    Template resolver for EIP Messaging Endpoint > SelectiveConsumer
    """

    def __init__(self, coating):
        super().__init__(coating=coating)
        self.set_reference()

    def set_reference(self):
        self.blueprint['reference'] = {
            'correlation_id': {
                'value': self.coating.correlation_id,
                'start_method': start,
                'on_message_method': on_message
            }
        }


# TODO MAYBE: Maybe bring the Ptypes validation to the Wingman.
class Wingman:
    """
    The Wingman is considered the coating resolver servant of the Foreman,
    it takes a provided EIP coating object and makes a blueprint
    of it that can be digested by the builders that the Foreman
    manages.
    """
    # Message Constructions
    _templates = {
        BasicMessage: BasicMessageResolver,
        PollingConsumer: PollingConsumerResolver,
        BasicMessageConsumer: BasicMessageConsumerResolver,
        SelectiveConsumer: SelectiveConsumerResolver
    }

    def __init__(self):
        """
        This method resets the Wingman on initialization.
        """
        self.reset()

    def reset(self):
        """
        This method resets the blueprint object to None.
        """
        self._blueprint = None

    def resolve(self, coating):
        """
        This method resolves the provided coating object to a bluerint.

        :param coating: The provided EIP coating.
        :type coating: pokeman.coating

        :return: The blueprint.
        """
        if coating.__class__ in self._templates.keys():
            _coating = coating
        else:
            raise ValueError("No coating resolver implemented for {COATING}".format(COATING=coating.__class__.__name__))

        resolver = self._templates[_coating.__class__](coating=_coating)
        self._blueprint = resolver.blueprint
        blueprint = self._blueprint
        self.reset()
        return blueprint
