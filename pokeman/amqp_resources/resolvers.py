from abc import ABC, abstractmethod
from enum import Enum, IntFlag, auto

from pokeman.amqp_resources.globals import Exchange, Queue
from pokeman.amqp_resources import __all__ as _c__all__


class Action(Enum):
    """
    This Enum object hold the possible resource handling actions.
    """
    CREATE = 0x1
    DELETE = 0x0


class Priority(IntFlag):
    """
    Priority flags are set auto 8-bit integers.

    .. note::
        The assigned integer flags are processed
        bij the RESOURCE_HEAP_Q
        queue.
    """
    EXCHANGE = auto()
    QUEUE = auto()


class AbstractResourceResolver(ABC):
    """
    Abstract base clase for a AMQP resource resolver.
    """
    def __init__(self, resource, action):
        """
        :param resource: The provided resource.
        :type resource: pokeman._components.globals.*
        :param action: The action.
        :type action: Actions
        """
        self.resource = resource
        self.blueprint = {
            'type': None,
            'priority': None,
            'context': {},
            'action': action.name
        }
        self.resolve_resource()

    def resolve_resource(self):
        """
        This method executes all the resolve steps.
        """
        self.get_build_type()
        self.set_priority()
        self.check_configuration()
        self.get_context()

    def get_build_type(self):
        self.blueprint['type'] = self.resource.__class__.__name__

    @abstractmethod
    def set_priority(self):
        pass

    @abstractmethod
    def check_configuration(self):
        pass

    @abstractmethod
    def get_context(self):
        pass


class ExchangeResolver(AbstractResourceResolver):
    """
    Template resolver for Exchange.
    """
    def set_priority(self):
        """
        This method sets the priority value that is needed in the HeapQ,
        for the resource.
        """
        self.blueprint['priority'] = Priority.EXCHANGE.value

    def check_configuration(self):
        """
        This method does nothing yet.
        """
        pass

    def get_context(self):
        """
        This method lazy assigns all the resource attributes to
        the blueprint.

        ..note::
            Private '_' attributes are ignored.
        """
        for _attr, _value in self.resource.__dict__.items():
            if _attr.startswith('_'):
                continue
            self.blueprint['context'][_attr] = _value


class QueueResolver(AbstractResourceResolver):
    """
    Template resolver for Queue.
    """
    def set_priority(self):
        """
        This method sets the priority value that is needed in the HeapQ,
        for the resource.
        """
        self.blueprint['priority'] = Priority.QUEUE.value

    def check_configuration(self):
        """
        This method does nothing yet.
        """
        pass

    def get_context(self):
        """
        This method lazy assigns all the resource attributes to
        the blueprint.

        ..note::
            Private '_' attributes are ignored.
        """
        for _attr, _value in self.resource.__dict__.items():
            if _attr.startswith('_'):
                continue
            self.blueprint['context'][_attr] = _value


class ResourceResolver:
    """
    The resource resolver provides blueprints for the provided
    resource objects
    """
    _templates = {
        Exchange: {
            'blueprint': ExchangeResolver
        },
        Queue: {
            'blueprint': QueueResolver
        }
    }

    def __init__(self):
        """
        This method resets the ResourceResolver on initialization.
        """
        self.reset()

    def reset(self):
        """
        This method resets the blueprint object to None.
        """
        self._blueprint = None

    def resolve(self, resource, action):
        """
        This method takes the resource object and desired action,
        resolves it to a blueprint for builder objects.

        :param resource: The provided AMQP resource.
        :type resource: pokeman.amqp_resources.globals.*

        :param action: The provided action.
        :type action: Action

        :return: The resolved resource as blueprint.
        :rtype: dict

        ..note::
            This layer method calls the blueprint method from this class,
            as for future integration more checks and balances will be
            added.
        """
        blueprint = self.blueprint(resource=resource, action=action)
        return blueprint

    def blueprint(self, resource, action):
        """
        This method takes the resource object and desired action,
        resolves it to a blueprint for builder objects.

        :param resource: The provided AMQP resource.
        :type resource: pokeman.amqp_resources.globals.*

        :param action: The provided action.
        :type action: Action

        :return: The resolved resource as blueprint.
        :rtype: dict
        """
        if resource.__class__ in self._templates.keys():
            _resource = resource
        else:
            raise ValueError('No resource resolver implemented for {RESOURCE}'.format(
                RESOURCE=resource.__class__.__name__)
            )

        blueprint_resolver = self._templates[_resource.__class__]['blueprint'](resource=resource, action=action)
        self._blueprint = blueprint_resolver.blueprint
        blueprint = self._blueprint
        self.reset()
        return blueprint
