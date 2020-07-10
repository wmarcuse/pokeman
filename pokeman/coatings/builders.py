from abc import ABC, abstractmethod
from uuid import uuid4
import time
import os
import pika
from pika.exceptions import AMQPConnectionError, UnroutableError
import json

from pokeman.utils.custom_abc import abstract_attribute
from pokeman.coatings.resolvers import Wingman
from pokeman.coatings.ptypes import Ptypes
from . import __all__ as _c__all__

import logging

LOGGER = logging.getLogger(__name__)


class AbstractSynchronousProducerBuilder(ABC):
    """
    Abstract base class for a synchronous producer builder.
    """

    @abstract_attribute
    def producer(self):
        pass

    @abstractmethod
    def digest_blueprint(self):
        pass

    @abstractmethod
    def set_current_context(self, channel_id, value):
        pass

    @abstractmethod
    def go_to_next_build_step(self):
        pass

    @abstractmethod
    def finalize_current_build_step(self):
        pass

    @abstractmethod
    def add_channel(self):
        pass

    @abstractmethod
    def apply_delivery_confirmations(self):
        pass

    @abstractmethod
    def setup_exchange(self):
        pass

    @abstractmethod
    def setup_routing_key(self):
        pass


class AbstractSynchronousConsumerBuilder(ABC):
    """
    Abstract base class for a synchronous consumer builder.
    """

    @abstract_attribute
    def consumer(self):
        pass

    @abstractmethod
    def digest_blueprint(self):
        pass

    @abstractmethod
    def set_current_context(self, channel_id, value):
        pass

    @abstractmethod
    def go_to_next_build_step(self):
        pass

    @abstractmethod
    def finalize_current_build_step(self):
        pass

    @abstractmethod
    def add_channel(self):
        pass

    @abstractmethod
    def apply_delivery_confirmations(self):
        pass

    @abstractmethod
    def setup_exchanges(self):
        pass

    @abstractmethod
    def setup_queues(self):
        pass


class SynchronousProducerBuilder(AbstractSynchronousProducerBuilder):
    BLUEPRINT = 0x0
    INSTANCE = 0x1

    def __init__(self, connection, blueprint):
        """
        This method initializes the producer builder and sets the
        connection and the by the Foreman already on compatibility
        checked build template.

        :param connection: The provided AMQP broker connection that is attached
        to the Pokeman.
        :type connection: pokeman.Pokeman.connection

        :param blueprint: The resolved template from the Wingman.
        :type blueprint: pokeman.coating._resolvers.Wingman._blueprint
        """
        self.reset()
        self.connection = connection
        self.blueprint = blueprint
        self.multistep = False
        self.channel_build_context = {}
        self.current_context_channel_id = None
        self.current_context_exchange_name = None

    def reset(self):
        """
        The production builder is reset on initialization and after a
        delivery of a producer.
        """
        self._producer = SynchronousProducer()

    @property
    def producer(self):
        """
        When the producer delivery is invoked, the builder resets itself for
        a new producing task.

        :return: The built producer object.
        :rtype: pokeman.coating._builders.Synchronousproducer
        """
        producer = self._producer
        self.reset()
        return producer

    def _channel_generator(self, location):
        """
        Channel generator.

        :param location: The desired location to iterate.

        :return: The generator object
        """
        if location == self.BLUEPRINT:
            for _channel_id, _context in self.blueprint['channel'].items():
                yield _channel_id, _context
        elif location == self.INSTANCE:
            for _channel_id, _context in self.channel_build_context.items():
                yield _channel_id, _context
        else:
            raise ValueError('Invalid location argument')

    def _exchange_generator(self, location, channel_id):
        """
        Exchange generator.

        :param location: The desired location to iterate.

        :param channel_id: The desired channel id to iterate.
        :type channel_id: str

        :return: The generator object
        """
        if location == self.BLUEPRINT:
            for _exchange_name, _context in self.blueprint['channel'][channel_id]['exchange'].items():
                yield _exchange_name, _context
        else:
            raise ValueError('Invalid location argument')

    def digest_blueprint(self):
        """
        This method envokes blueprint digestion by the builder.
        It detects if a multistep build process is necessary and
        sets the channel build context.
        """
        LOGGER.debug('{BUILDER} digesting blueprint'.format(BUILDER=self.__class__.__name__))
        if len(self.blueprint['channel']) > 1:
            self.multistep = True  # TODO: Not implemented yet
        channel_generator = self._channel_generator(location=self.BLUEPRINT)
        for _channel_id, _context in channel_generator:
            self.channel_build_context[_channel_id] = {'configured': False, 'current_context': False}
        LOGGER.debug('{BUILDER} digesting blueprint OK!'.format(BUILDER=self.__class__.__name__))

    def start_independent_build(self):
        """
        This method invokes the standard building process for
        the producer builder. The Foreman can invoke custom builds by
        instructing the builder on it's own.
        """
        channel_generator = self._channel_generator(location=self.INSTANCE)
        for _channel_id, _context in channel_generator:
            self.set_current_context(channel_id=_channel_id, value=True)
            self.add_channel()
            self.apply_delivery_confirmations()  # TODO: Make choice in composite
            self.setup_exchange()
            self.setup_routing_key()
            self.set_current_context(channel_id=_channel_id, value=False)

    def set_current_context(self, channel_id, value):
        """
        This method sets the current context for a build
        process started from a channel tree.

        :param channel_id: The provided channel id.
        :type channel_id: str

        :param value: Boolean value if the current context needs to be set or removed.
        :type value: bool
        """
        self.channel_build_context[channel_id]['current_context'] = value
        if value is True:
            self.current_context_channel_id = channel_id
        else:
            self.current_context_channel_id = None

    def go_to_next_build_step(self):
        """
        Invoke this method if a multistep build process is triggered.
        """
        pass

    def finalize_current_build_step(self):
        """
        Invoke this method if a multistep build process is triggered.
        """
        pass

    def set_app_id(self):
        """
        This method sets the app id for the producer.
        """
        self._producer.APP_ID = self.blueprint['app_id']

    def add_channel(self):
        """
        This method will open a new channel with AMQP broker and attach
        it to the producer.
        """
        LOGGER.debug('Creating a new channel')
        new_channel = self.connection.channel()
        self._producer.channel = new_channel
        self._producer.channel_id = self.current_context_channel_id
        LOGGER.debug('Creating a new channel OK!')

    def apply_delivery_confirmations(self):
        """
        This method will apply delivery confirmations on a BlockingConnection.

        .. note::
            When the message is not received correctly a pika.exceptions.
            UnroutableError will be raised.
        """
        LOGGER.debug('Applying delivery confirmations')
        self._producer.channel.confirm_delivery()
        LOGGER.debug('Applying delivery confirmations OK!')

    def setup_exchange(self):
        """
        Set up the exchange on the AMQP broker by declaring the exchange
        on the current context channel.
        """
        # TODO: Resolve new globals format!!!!
        # TODO: Add the logic here if multiple exchanges per producer are allowed for future releases
        exchange_generator = self._exchange_generator(
            location=self.BLUEPRINT,
            channel_id=self.current_context_channel_id
        )
        for _exchange_name, _context in exchange_generator:
            self.current_context_exchange_name = _exchange_name
            LOGGER.debug('Binding exchange {EXCHANGE_NAME} to {APP_ID} producer'.format(
                EXCHANGE_NAME=_exchange_name,
                APP_ID=self.blueprint['app_id']
            )
            )
            # TODO: Current configuration allows one exchange per producer
            self._producer.exchange = _exchange_name
            LOGGER.debug('Binding exchange {EXCHANGE_NAME} to {APP_ID} producer OK!'.format(
                EXCHANGE_NAME=_exchange_name,
                APP_ID=self.blueprint['app_id']
            )
            )
            self.current_context_exchange_name = None

    def setup_routing_key(self):
        channel_generator = self._channel_generator(location=self.BLUEPRINT)
        for _channel_id, _context in channel_generator:
            self._producer.routing_key = _context['routing_key']


class SynchronousProducer:
    """
    The synchronous producer unfinished intangible.
    A builder Ptypes allowed needs to configure and
    deliver this object.
    """
    APP_ID = None
    CONTENT_TYPE = 'application/json'
    CONTENT_ENCODING = None
    HEADERS = None
    DELIVERY_MODE = 2
    PRIORITY = None
    CORRELATION_ID = None
    REPLY_TO = None
    EXPIRATION = None
    MESSAGE_ID = None
    TIMESTAMP = None
    TYPE = None
    USER_ID = None
    CLUSTER_ID = None

    def __init__(self):
        self.channel = None
        self.channel_id = None
        self.exchange = None
        self.routing_key = None
        self._message_number = 0

    def publish(self, message,
                content_type=CONTENT_TYPE,
                content_encoding=CONTENT_ENCODING,
                headers=HEADERS,
                delivery_mode=DELIVERY_MODE,
                priority=PRIORITY,
                correlation_id=CORRELATION_ID,
                reply_to=REPLY_TO,
                expiration=EXPIRATION,
                message_id=MESSAGE_ID,
                timestamp=TIMESTAMP,
                type=TYPE,
                user_id=USER_ID,
                cluster_id=CLUSTER_ID
                ):
        """
        This method publishes a message to AMQP broker, incrementing
        the number of messages variable.

        :param message: The message
        :type message: str|unicode|dict|list|int|float

        :param headers: The headers, unicode key,values are allowed
        :type headers: dict

        .. note::
            The first if statement checks if the channel is not dead or
            already in use (open).
        """

        if self.channel is None or not self.channel.is_open:
            raise ConnectionError('This producer has no active AMQP broker connection')

        if not correlation_id:
            correlation_id = str(uuid4())

        # if not timestamp:
        #     timestamp = str(int(time.time()))

        properties = pika.BasicProperties(
            content_type=content_type,
            content_encoding=content_encoding,
            headers=headers,
            delivery_mode=delivery_mode,
            priority=priority,
            correlation_id=correlation_id,
            reply_to=reply_to,
            expiration=expiration,
            message_id=message_id,
            timestamp=timestamp,
            type=type,
            user_id=user_id,
            app_id=self.APP_ID,
            cluster_id=cluster_id
        )

        message = message
        try:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=json.dumps(message, ensure_ascii=False),
                properties=properties
            )
        except UnroutableError as e:
            """There was nog delivery confirmation from the broker"""
            LOGGER.exception(e)
            raise
        except AMQPConnectionError as e:
            """The connection could not be established"""
            LOGGER.exception(e)
            raise
        except Exception as e:
            """Any other exception"""
            LOGGER.exception(e)
            raise
        self._message_number += 1
        LOGGER.debug("Published message # {MESSAGE_NUMBER}".format(MESSAGE_NUMBER=self._message_number))
        return {
            "correlation_id": correlation_id,
            "timestamp": timestamp
        }


# TODO: Add Template design pattern integration for Asynchonous Producer integration
class Foreman:
    """
    The Foreman gets it's instructions from the Pokeman and uses the
    Wingman for blueprint-builder validation, it can return builders
    after picking and assigning the right builders for the right blueprint
    and Ptypes.
    """
    def __init__(self):
        """
        This method initializes the Foreman with no attached builder and
        sets the Wingman as its validation servant.

        .. note::
            Pick and assign builders in chronological order, as the Foreman
            can only do the job allocation with one builder at a time.

            You can off-course deploy and operate multiple builders at the
            same time, as they handle the heavy lifting.
        """
        self.builder = None
        self.wingman = Wingman()

    def pick_builder(self, connection, coating, ptype):
        """
        This method picks the right builder for the provided coating
        and Ptypes. The Wingman resolves and validates the provided
        coating and supplies the blueprint.

        :param connection: The provided AMQP broker connection.
        :type connection: pokeman.Pokeman.connection

        :param coating: The provided EIP coating
        :type coating: pokeman.coating.BasicMessage

        :param ptype: The provided Ptypes
        :type ptype: pokeman.coating.ptypes.Ptypes
        """
        if self.builder is not None:
            raise SyntaxError('The {MANAGER} has already picked a builder. Assign some work to the builder first'
                              'or destroy the current builder with Foreman.destroy_builder()'.format(
                MANAGER=self.__class__.__name__
            )
            )
        if coating.__class__.__name__ not in _c__all__:
            raise AttributeError('The provided coating is not valid')

        blueprint = self.wingman.resolve(coating=coating)

        # with open('template_{UUID}.json'.format(UUID=str(uuid4())), 'w', encoding='utf-8') as f:
        #         #     json.dump(blueprint, f, ensure_ascii=False, indent=4)

        ptype_check = Ptypes._map(eip=blueprint["type"], ptype=ptype)
        if ptype_check["map"] == Ptypes.SYNC_PRODUCER:
            self.builder = SynchronousProducerBuilder(connection=connection, blueprint=blueprint)
        else:
            raise NotImplementedError('The provided configuration parameters are not implemented yet')

    def deliver_producer(self):
        """
        This method invokes the building of a producer by the builder
        and delivers it to the Pokeman.

        :return: The producer object
        """
        self.builder.digest_blueprint()
        self.builder.start_independent_build()
        return self.builder.producer

    def destroy_builder(self):
        """
        This method destroys the current builder attached to the Foreman.
        Lunch-break for that guy.
        """
        self.builder = None
