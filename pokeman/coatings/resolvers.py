from abc import ABC, abstractmethod
from uuid import uuid4

# Message Constructions
from .message_constructions import BasicMessage

import logging

LOGGER = logging.getLogger(__name__)


class AbstractMessageConstructionResolver(ABC):
    """
    Abstract base clase for a EIP Message Construction resolver.
    """
    def __init__(self, coating):
        self.coating = coating
        self.blueprint = {
            'type': None,
            'app_id': None,
            'channel': {}
        }
        self.channel_reference = {}
        self.current_context_channel_id = None
        self.resolve_coating()

    def resolve_coating(self):
        self.get_build_type()
        self.get_app_id()
        self.set_channel()
        self.set_exchange()
        self.set_routing_key()
        self.set_response_queue()

    def get_build_type(self):
        if 'Resolver' not in self.__class__.__name__:
            raise NameError('Resolver not found in {CLASS_NAME}'.format(CLASS_NAME=self.__class__.__name__))
        self.blueprint['type'] = self.__class__.__name__.replace('Resolver', '')

    def _channel_references_generator(self):
        for _channel_id, _context in self.channel_reference.items():
            yield _channel_id, _context

    @abstractmethod
    def get_app_id(self):
        pass

    @abstractmethod
    def set_channel(self):
        pass

    @abstractmethod
    def set_exchange(self):
        pass

    @abstractmethod
    def set_routing_key(self):
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

    def set_channel(self):
        """
        This methods sets the channel in the blueprint.
        """
        channel_id = str(uuid4())
        self.channel_reference[channel_id] = {}
        self.channel_reference[channel_id]['active'] = False
        self.blueprint['channel'][channel_id] = {}

    def set_exchange(self):
        """
        This methods sets the exchange in the blueprint.
        """
        exchange_name = self.coating.exchange
        channel_generator = self._channel_references_generator()
        channel_id = None
        for _channel_id, _context in channel_generator:
            if _context['active'] is False:
                channel_id = _channel_id
                self.current_context_channel_id = channel_id  # TODO: Quick fix for non-multistep
                _context['active'] = True
                _context['exchange'] = exchange_name
                channel_generator.close()
                break
            else:
                channel_generator.close()
                raise IndexError('{RESOLVER} failed, not enough channels for resource mapping available'.format(
                    RESOLVER=self.__class__.__name__
                ))
        channel_path = self.blueprint['channel'][channel_id]
        channel_path['exchange'] = {}
        channel_path['exchange'][exchange_name] = {}
        exchange_path = channel_path['exchange'][exchange_name]
        # exchange_path['routing_key'] = self.coating.routing_key

    def set_routing_key(self):
        """
        This method sets the routing key in the blueprint
        """
        self.blueprint['channel'][self.current_context_channel_id]['routing_key'] = self.coating.routing_key


# TODO MAYBE: Maybe bring the Ptypes validation to the Wingman.
class Wingman:
    """
    The Wingman is considered the coating resolver servant of the Foreman,
    it takes a provided EIP coating object and makes a blueprint
    of it that can be digested by the builders that the Foreman
    manages.
    """
    # Message Constructions
    _templates = {BasicMessage: BasicMessageResolver}

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
