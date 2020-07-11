from abc import ABC, abstractmethod

import json
from pika.exceptions import ChannelClosedByBroker

from pokeman.amqp_resources.globals import AbstractExchange, AbstractQueue, Exchange, Queue
from . import __all__ as _c__all__

import logging

LOGGER = logging.getLogger(__name__)


class AbstractResourceBuilder(ABC):
    """
    Abstract base class for a resource builder.
    """
    @abstractmethod
    def digest_blueprint(self):
        pass

    @abstractmethod
    def create_resource(self):
        pass


class AbstractResourceDestroyer(ABC):
    """
    Abstract base class for a resource destroyer.
    """
    def __init__(self, channel, blueprint):
        self.channel = channel
        self.blueprint = blueprint
        self.blueprint_context = self.blueprint['context']
        self.delete_resource()

    @abstractmethod
    def delete_resource(self):
        pass


class AbstractExistenceChecker(ABC):
    """
    Abstract base class for an existence checker.
    """
    def __init__(self, channel, blueprint):
        self.reset()
        self.channel = channel
        self.blueprint = blueprint
        self.blueprint_context = self.blueprint['context']
        self.check_existence()

    def reset(self):
        self._exists = False

    @property
    def exists(self):
        existence = self._exists
        self.reset()
        return existence

    @abstractmethod
    def check_existence(self):
        pass


class ExchangeBuilder(AbstractResourceBuilder, AbstractExchange):
    """
    The Exchange builder is responsible for building Queue resources
    and is handled by the ResourceManager.
    """
    def __init__(self, channel, blueprint):
        """
        This method initializes the Exchange builder.

        :param channel: The provided AMQP channel.
        :type channel: pokeman.Pokeman.connection.channel

        :param blueprint: The provided Exchange blueprint
        :type blueprint: dict
        """
        self.reset()
        self.channel = channel
        self.blueprint = blueprint
        self.build_context = self.blueprint['context']

        # Abstract attributes
        self.exchange_name = None
        self.exchange_type = None
        self.durable = None
        self.auto_delete = None
        self.specific_poker = None

    def reset(self):
        """
        This method resets the Exchange builder instance.
        """
        self.exchange_name = None
        self.exchange_type = None
        self.durable = None
        self.auto_delete = None
        self.specific_poker = None

    def digest_blueprint(self):
        """
        This method checks whether the right blueprint was supplied to
        the builder and copies the build context parameters to its
        instance.
        """
        LOGGER.debug('{BUILDER} digesting blueprint'.format(BUILDER=self.__class__.__name__))
        if self.blueprint['type'] != self.__class__.__name__.replace('Builder', ''):
            raise ValueError('{BUILDER} received the wrong blueprint'.format(
                BUILDER=self.__class__.__name__
            ))
        self.exchange_name = self.build_context['exchange_name']
        self.exchange_type = self.build_context['exchange_type']
        self.durable = self.build_context['durable']
        self.auto_delete = self.build_context['auto_delete']
        self.specific_poker = self.build_context['specific_poker']
        LOGGER.debug('{BUILDER} digesting blueprint OK!'.format(BUILDER=self.__class__.__name__))

    def create_resource(self):
        """
        This method creates the Exchange resource on the AMQP broker.
        """
        LOGGER.debug('Creating new Exchange')
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=self.exchange_type,
            durable=self.durable,
            auto_delete=False
        )
        LOGGER.debug('Creating new Exchange OK!')
        self.reset()


class QueueBuilder(AbstractResourceBuilder, AbstractQueue):
    """
    The Queue builder is responsible for building Queue resources
    and is handled by the ResourceManager.
    """
    def __init__(self, channel, blueprint):
        """
        This method initializes the Queue builder.

        :param channel: The provided AMQP channel.
        :type channel: pokeman.Pokeman.connection.channel

        :param blueprint: The provided Queue blueprint
        :type blueprint: dict
        """
        self.reset()
        self.channel = channel
        self.blueprint = blueprint
        self.build_context = self.blueprint['context']

        # Abstract attributes
        self.queue_name = None
        self.exchange = None
        self.routing_key = None
        self.exclusive = None
        self.durable = None
        self.auto_delete = None
        self.specific_poker = None

    def reset(self):
        """
        This method resets the Queue builder instance.
        """
        self.queue_name = None
        self.exchange = None
        self.routing_key = None
        self.exclusive = None
        self.durable = None
        self.auto_delete = None
        self.specific_poker = None

    def digest_blueprint(self):
        """
        This method checks whether the right blueprint was supplied to
        the builder and copies the build context parameters to its
        instance.
        """
        LOGGER.debug('{BUILDER} digesting blueprint'.format(BUILDER=self.__class__.__name__))
        if self.blueprint['type'] != self.__class__.__name__.replace('Builder', ''):
            raise ValueError('{BUILDER} received the wrong blueprint'.format(
                BUILDER=self.__class__.__name__
            ))
        self.queue_name = self.build_context['queue_name']
        self.exchange = self.build_context['exchange']
        self.routing_key = self.build_context['routing_key']
        self.exclusive = self.build_context['exclusive']
        self.durable = self.build_context['durable']
        self.auto_delete = self.build_context['auto_delete']
        LOGGER.debug('{BUILDER} digesting blueprint OK!'.format(BUILDER=self.__class__.__name__))

    def create_resource(self):
        """
        This method creates the Queue resource on the AMQP broker.
        """
        LOGGER.debug('Creating new Queue')
        self.channel.queue_declare(
            queue=self.queue_name,
            durable=self.durable,
            exclusive=self.exclusive,
            auto_delete=self.auto_delete
        )
        self.channel.queue_bind(
            queue=self.queue_name,
            exchange=self.exchange,
            routing_key=self.routing_key
        )
        LOGGER.debug('Creating new Queue')
        self.reset()


class ExchangeExistenceChecker(AbstractExistenceChecker):
    """
    The Exchange existence checker.
    """
    def check_existence(self):
        """
        This method checks if the provided resource already exists.

        :return: The existence status
        :rtype: bool
        """
        try:
            self.channel.exchange_declare(
                exchange=self.blueprint_context['exchange_name'],
                exchange_type=self.blueprint_context['exchange_type'],
                passive=True
            )
            self._exists = True
        except ChannelClosedByBroker as exc:
            if 'NOT_FOUND' in exc.reply_text:
                self._exists = False


class QueueExistenceChecker(AbstractExistenceChecker):
    """
    The Queue existence checker.
    """
    def check_existence(self):
        """
        This method checks if the provided resource already exists.

        :return: The existence status
        :rtype: bool
        """
        try:
            self.channel.queue_declare(
                queue=self.blueprint_context['queue_name'],
                passive=True
            )
            self._exists = True
        except ChannelClosedByBroker as exc:
            if 'NOT_FOUND' in exc.reply_text:
                self._exists = False


class ExchangeDestroyer(AbstractResourceDestroyer):
    """
    The Exchange destroyer object.
    """
    def delete_resource(self):
        """
        This method deletes the Exchange.
        """
        LOGGER.debug('Deleting Exchange')
        try:
            self.channel.exchange_delete(exchange=self.blueprint_context['exchange_name'])
        except ChannelClosedByBroker as exc:
            if 'NOT_FOUND' in exc.reply_text:
                pass
        LOGGER.debug('Deleting Exchange OK!')


class QueueDestroyer(AbstractResourceDestroyer):
    """
    The Queue destroyer object.
    """
    def delete_resource(self):
        """
        This methods deletes the Queue.
        """
        LOGGER.debug('Deleting Queue')
        try:
            self.channel.queue_delete(queue=self.blueprint_context['queue_name'])
        except ChannelClosedByBroker as exc:
            if 'NOT_FOUND' in exc.reply_text:
                pass
        LOGGER.debug('Deleting Queue OK!')


class ResourceManager:
    """
    The ResourceManager handles the validation, building and deletion
    of AMQP resources.
    """
    _templates = {
        Exchange.__name__: {
            'builder': ExchangeBuilder,
            'checker': ExchangeExistenceChecker,
            'destroyer': ExchangeDestroyer
        },
        Queue.__name__: {
            'builder': QueueBuilder,
            'checker': QueueExistenceChecker,
            'destroyer': QueueDestroyer
        }
    }

    def __init__(self, connection):
        """
        This method initialized the AMQP ResourceManager with no attached builder.

        As there is a hight risk of losing the connection during the various validation
        and building steps, each job is provided with its own channel derived from
        the Pokeman connection.

        :param connection: The provided connection
        :type connection: pokeman.Pokeman.connection
        """
        self.builder = None
        self._connection = connection
        self.resource_exists = False

    def pick_builder(self, blueprint):
        """
        This method picks the right builder for the provided resource blueprint.
        The ResourceManager gives the call to check if the resource already exists
        and assigns the right builder for the job.

        :param blueprint: The provided blueprint.
        :type blueprint: dict
        """
        if self.builder is not None:
            raise SyntaxError('The {MANAGER} has already picked a builder. Assign some work to the builder first'
                              'or destroy the current builder with Foreman.destroy_builder()'.format(
                MANAGER=self.__class__.__name__
            )
            )
        if blueprint['type'] not in _c__all__:
            raise AttributeError('The provided resource is not valid')

        self.resource_exists = self._templates[blueprint['type']]['checker'](
            channel=self._connection.channel(),
            blueprint=blueprint
        ).exists

        if self.resource_exists is False:
            self.builder = self._templates[blueprint['type']]['builder'](
                channel=self._connection.channel(),
                blueprint=blueprint
            )
        else:
            LOGGER.debug('The provided resource already exists')

    def create_resource(self):
        """
        This method instructs the attached builder to create the resource.
        """
        if self.resource_exists is False:
            self.builder.digest_blueprint()
            self.builder.create_resource()
            self.destroy_builder()
        else:
            self.destroy_builder()
            pass

    def destroy_builder(self):
        """
        This method destroys the builder that is attached to the ResourceManager.
        """
        self.builder = None
        self.resource_exists = False

    def delete_attached_resources(self, poker_id):
        """
        This method deletes all the AMQP resources attached to the Pokeman.

        ..note::
            Warning! Use this method with caution, this will delete all
            the resources attached to the pokeman, included the default
            resources if called from the default Pokeman. This method
            will delete resources that are still in use as well, and
            also deletes resources if they are re-declared in other
            Pokeman's as well but still have the same name.

        :param poker_id: The provided poker id.
        :type poker_id: str
        """
        from pokeman.amqp_resources.heapq import ResourceHeapQ  # Avoid circular import
        LOGGER.debug('Deleting all attached resources for Pokeman {POKER_ID}'.format(
            POKER_ID=poker_id
        )
        )
        resources_heapq = ResourceHeapQ.get_heapq(
            poker_id=poker_id,
            reverse=True
        )
        for _ in range(0, resources_heapq.qsize()):
            heap_record = resources_heapq.get()
            resource_blueprint = json.loads(heap_record[1])
            LOGGER.debug('DELETING: ' + str(resource_blueprint))
            destroyer = self._templates[resource_blueprint['type']]['destroyer'](
                channel=self._connection.channel(),
                blueprint=resource_blueprint
            )
        LOGGER.debug('Deleting all attached resources for Pokeman {POKER_ID} OK!'.format(
            POKER_ID=poker_id
        )
        )
