from abc import ABC, abstractmethod
from uuid import uuid4
import functools
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
    def open_channel(self):
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


class AbstractAsynchronousConsumerBuilder(ABC):
    """
    Abstract base class for a asynchronous consumer builder.
    """

    @abstract_attribute
    def consumer(self):
        pass

    @abstractmethod
    def digest_blueprint(self):
        pass

    @abstractmethod
    def setup_connection(self):
        pass

    @abstractmethod
    def setup_exchange(self):
        pass

    @abstractmethod
    def setup_queue(self):
        pass

    @abstractmethod
    def setup_callback_method(self):
        pass

    @abstractmethod
    def setup_qos(self):
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
    def open_channel(self):
        pass

    @abstractmethod
    def setup_exchange(self):
        pass

    @abstractmethod
    def setup_queue(self):
        pass

    @abstractmethod
    def setup_callback_method(self):
        pass

    @abstractmethod
    def setup_qos(self):
        pass

    @abstractmethod
    def elevate_setup(self):
        pass


class SynchronousProducerBuilder(AbstractSynchronousProducerBuilder):

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
        :rtype: pokeman.coating._builders.SynchronousProducer
        """
        producer = self._producer
        self.reset()
        return producer

    def digest_blueprint(self):
        """
        This method envokes blueprint digestion by the builder.
        It detects if a multistep build process is necessary and
        sets the channel build context.
        """
        LOGGER.debug('{BUILDER} digesting blueprint'.format(BUILDER=self.__class__.__name__))
        LOGGER.debug('{BUILDER} digesting blueprint OK!'.format(BUILDER=self.__class__.__name__))

    def start_independent_build(self):
        """
        This method invokes the standard building process for
        the producer builder. The Foreman can invoke custom builds by
        instructing the builder on it's own.
        """
        self.open_channel()
        self.apply_delivery_confirmations()  # TODO: Make choice in composite
        self.setup_exchange()
        self.setup_routing_key()

    def set_app_id(self):
        """
        This method sets the app id for the producer.
        """
        self._producer.APP_ID = self.blueprint['app_id']

    def open_channel(self):
        """
        This method will open a new channel with AMQP broker and attach
        it to the producer.
        """
        LOGGER.debug('Opening a new channel')
        new_channel = self.connection.channel()
        self._producer.channel = new_channel
        LOGGER.debug('Opening a new channel OK!')

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
        _exchange_name = self.blueprint['exchange']
        LOGGER.debug('Binding exchange {EXCHANGE_NAME} to {APP_ID} producer'.format(
            EXCHANGE_NAME=_exchange_name,
            APP_ID=self.blueprint['app_id']
        )
        )
        self._producer.exchange = _exchange_name
        LOGGER.debug('Binding exchange {EXCHANGE_NAME} to {APP_ID} producer OK!'.format(
            EXCHANGE_NAME=_exchange_name,
            APP_ID=self.blueprint['app_id']
        )
        )

    def setup_routing_key(self):
        self._producer.routing_key = self.blueprint['routing_key']


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
            "correlation_id": correlation_id
        }


class AsynchronousConsumerBuilder(AbstractAsynchronousConsumerBuilder):

    def __init__(self, connection, blueprint):
        """
        This method initializes the consumer builder and sets the
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

    def reset(self):
        """
        The production builder is reset on initialization and after a
        delivery of a consumer.
        """
        self._consumer = AsynchronousConsumer()

    @property
    def consumer(self):
        """
        When the consumer delivery is invoked, the builder resets itself for
        a new producing task.

        :return: The built consumer object.
        :rtype: pokeman.coating._builders.SynchronousConsumer
        """
        consumer = self._consumer
        self.reset()
        return consumer

    def digest_blueprint(self):
        """
        This method envokes blueprint digestion by the builder.
        It detects if a multistep build process is necessary and
        sets the channel build context.
        """
        LOGGER.debug('{BUILDER} digesting blueprint'.format(BUILDER=self.__class__.__name__))
        LOGGER.debug('{BUILDER} digesting blueprint OK!'.format(BUILDER=self.__class__.__name__))

    def start_independent_build(self):
        """
        This method invokes the standard building process for
        the consumer builder. The Foreman can invoke custom builds by
        instructing the builder on it's own.
        """
        self.setup_connection()
        self.setup_exchange()
        self.setup_queue()
        self.setup_callback_method()
        self.setup_qos()

    def setup_connection(self):
        """
        Set up the connection object for the consumer.
        """
        self._consumer.connection_ob = self.connection

    def setup_exchange(self):
        """
        Set up the exchange on the AMQP broker by declaring the exchange
        on the current context channel.
        """
        _exchange_name = self.blueprint['exchange']
        LOGGER.debug('Binding exchange {EXCHANGE_NAME} to producer'.format(
            EXCHANGE_NAME=_exchange_name
        )
        )
        self._consumer.exchange = _exchange_name
        LOGGER.debug('Binding exchange {EXCHANGE_NAME} to producer OK!'.format(
            EXCHANGE_NAME=_exchange_name
        )
        )

    def setup_queue(self):
        self._consumer.queue = self.blueprint['queue']

    def setup_callback_method(self):
        self._consumer.callback_method = self.blueprint['callback_method']

    def setup_qos(self):
        self._consumer.qos = self.blueprint['qos']


class AsynchronousConsumer:
    def __init__(self):
        self.connection_ob = None
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None
        self.callback_method = lambda body, properties: None
        self.qos = 1
        self.was_consuming = False
        self._consuming = False
        self._consumer_tag = None
        self._closing = False

    def connect(self):
        try:
            self.connection_ob.connect(
                on_open_callback=self.on_connection_open,
                on_open_error=self.on_connection_open_error,
                on_close_callback=self.on_connection_closed
            )
            self.connection = self.connection_ob.connection
        except Exception as e:
            LOGGER.exception("Trying to set connection attempt failed")
            pass

    def close_connection(self):
        self._consuming = False
        if self.connection.is_closing or self.connection.is_closed:
            LOGGER.debug("Connection is closing or already closed")
        else:
            LOGGER.debug("Closing connection")
            self.connection.close()

    def on_connection_open(self, _unused_connection):
        """
        This method is called by pika once the connection to the AMQP broker has
        been established. It passes the handle to the connection object in
        _unused_connection, now it unused but in this method mutations on
        the handle are possible.
        The method then calls the self.open_channel() method.
        :param _unused_connection: The connection.
        :type _unused_connection: pika.SelectConnection
        """
        LOGGER.debug("Connection opened")
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """
        This method is called by pika if the connection to the AMQP broker
        can"t be established. The self.reconnect() method is called.
        :param _unused_connection: The connection handle where error occurred.
        :type _unused_connection: pika.SelectConnection
        :param err: The error.
        :type err: Exception
        """
        LOGGER.error("Connection open failed, reconnecting: {ERROR}".format(ERROR=err))
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """
        This method is invoked by pika when the connection to the AMQP broker is
        closed unexpectedly. Since it is unexpected, this method will
        reconnect to the AMQP broker if it disconnects.
        :param _unused_connection: The closed connection handle.
        :type _unused_connection: pika.SelectConnection|Any
        :param reason: The exception representing the reason for loss of
            connection.
        :type reason: Exception
        """
        self.channel = None
        if self._closing:
            self.connection.ioloop.stop()
        else:
            LOGGER.warning("Connection closed, reconnect necessary: {REASON}".format(REASON=reason))
            self.reconnect()

    def reconnect(self):
        """
        This method will be invoked if the connection can"t be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """
        This method will open a new channel with the AMQP broker by issuing the
        Channel. Open RPC command. When the AMQP broker confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.
        """
        LOGGER.debug("Creating a new channel")
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        This method is invoked by pika when the channel has been opened.
        The channel object is passed in so this method can make use of it.
        Since the channel is now open, the exchange to use is declared.
        :param channel: The channel object.
        :type channel: pika.channel.Channel
        """
        LOGGER.debug("Channel opened")
        self.channel = channel
        self.add_on_channel_close_callback()
        self.set_qos()

    def add_on_channel_close_callback(self):
        """
        This method tells pika to call the on_channel_closed method if
        the AMQP broker unexpectedly closes the channel.
        """
        LOGGER.debug("Adding channel close callback")
        self.channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """
        Invoked by pika when the AMQP broker unexpectedly closes the channel.
        Channels are usually closed if there is an attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, this method will close the
        connection to shutdown the object.
        :param channel: The closed channel.
        :type channel: pika.channel.Channel
        :param reason: The reason why the channel was closed.
        :type reason: Exception
        """
        LOGGER.warning("Channel {CHANNEL} was closed: {REASON}".format(CHANNEL=channel,
                                                                       REASON=reason))
        self.close_connection()

    def set_qos(self):
        """
        This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before the AMQP broker will deliver another one. Do experiment
        with different prefetch values to achieve desired performance.
        """
        self.channel.basic_qos(
            prefetch_count=self.qos, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        """
        This method is invoked by pika when the Basic.QoS method has completed.
        At this point the consumer will start consuming messages by calling the
        self.start_consuming() method which will invoke the needed RPC commands
        to start the process.
        :param _unused_frame: The Basic.QosOk response frame
        :type _unused_frame: pika.frame.Method
        """
        LOGGER.debug("QOS set to: {PREFETCH_COUNT}".format(PREFETCH_COUNT=self.qos))
        self.start_consuming()

    def start_consuming(self):
        """
        This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if the AMQP broker
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with the AMQP broker. The method keeps the value globally to use
        it when we want to cancel consuming. The on_message method is passed
        in as a callback pika will invoke when a message is fully received.
        """
        LOGGER.debug("Issuing consumer related RPC commands")
        self.add_on_cancel_callback()
        self._consumer_tag = self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self.on_message
        )
        self.was_consuming = True
        self._consuming = True

    def on_message(self, channel, method, properties, body):
        """
        Invoked by pika when a message is delivered from the AMQP broker. The
        channel is passed for convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param channel: The channel object.
        :type channel: pika.channel.Channel

        :param method: basic_deliver method.
        :type method: pika.Spec.Basic.Deliver

        :param properties: The properties.
        :type properties: pika.Spec.BasicProperties

        :param body: The message body.
        :type body: bytes
        """
        try:
            self.callback_method(json.loads(body), properties)
            self.acknowledge_message(method.delivery_tag)
        except Exception:
            LOGGER.exception("Asynchronous callback method exception:")

    # TODO: Integrate this as a choice
    def acknowledge_message(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def add_on_cancel_callback(self):
        """
        Add a callback that will be invoked if the AMQP broker cancels the consumer
        for some reason. If the AMQP broker does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        LOGGER.debug("Adding consumer cancellation callback")
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """
        This method is invoked by pika when the AMQP broker sends a Basic.Cancel
        for a consumer receiving messages.
        :param method_frame: The Basic.Cancel frame
        :type method_frame: pika.frame.Method
        """
        LOGGER.debug("Consumer was cancelled remotely, shutting down: {METHOD_FRAME}".format(METHOD_FRAME=method_frame))
        if self.channel:
            self.channel.close()

    def stop_consuming(self):
        """
        Tell the AMQP broker that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self.channel:
            LOGGER.debug("Sending a Basic.Cancel RPC command to the AMQP broker")
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self.channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        """
        This method is invoked by pika when the AMQP broker acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.
        :param _unused_frame: The Basic.CancelOk frame
        :type _unused_frame: pika.frame.Method
        :param userdata: Extra user data (consumer tag)
        :type userdata: str|unicode
        """
        self._consuming = False
        LOGGER.debug(
            "the AMQP broker acknowledged the cancellation of the consumer: {USERDATA}".format(USERDATA=userdata))
        self.close_channel()

    def close_channel(self):
        """
        Call to close the channel with the AMQP broker cleanly by issuing the
        Channel.Close RPC command.
        """
        LOGGER.debug("Closing the channel")
        self.channel.close()

    def start(self):
        """
        Start the consumer by connecting to the AMQP broker and then starting
        the IOLoop to block and allow the SelectConnection to operate.
        """
        self.connect()
        self.connection.ioloop.start()

    def stop(self):
        """
        Cleanly shutdown the connection to the AMQP broker by stopping the consumer
        with the AMQP broker. When the AMQP broker confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when raising a PoisonPill exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with the AMQP broker. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            LOGGER.debug("Stopping")
            if self._consuming:
                self.stop_consuming()
                self.connection.ioloop.start()
            else:
                self.connection.ioloop.stop()
            LOGGER.debug("Stopped")


class SynchronousConsumerBuilder(AbstractSynchronousConsumerBuilder):

    def __init__(self, connection, blueprint):
        """
        This method initializes the consumer builder and sets the
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

    def reset(self):
        """
        The production builder is reset on initialization and after a
        delivery of a consumer.
        """
        self._consumer = SynchronousConsumer()

    @property
    def consumer(self):
        """
        When the consumer delivery is invoked, the builder resets itself for
        a new producing task.

        :return: The built consumer object.
        :rtype: pokeman.coating._builders.SynchronousConsumer
        """
        consumer = self._consumer
        self.reset()
        return consumer

    def digest_blueprint(self):
        """
        This method envokes blueprint digestion by the builder.
        It detects if a multistep build process is necessary and
        sets the channel build context.
        """
        LOGGER.debug('{BUILDER} digesting blueprint'.format(BUILDER=self.__class__.__name__))
        LOGGER.debug('{BUILDER} digesting blueprint OK!'.format(BUILDER=self.__class__.__name__))

    def start_independent_build(self):
        """
        This method invokes the standard building process for
        the consumer builder. The Foreman can invoke custom builds by
        instructing the builder on it's own.
        """
        self.open_channel()
        self.setup_exchange()
        self.setup_queue()
        self.setup_callback_method()
        self.setup_qos()
        self.elevate_setup()
        self.setup_variation()

    def open_channel(self):
        """
        This method will open a new channel with AMQP broker and attach
        it to the producer.
        """
        LOGGER.debug('Opening a new channel')
        new_channel = self.connection.channel()
        self._consumer.channel = new_channel
        LOGGER.debug('Opening a new channel OK!')

    def setup_exchange(self):
        """
        Set up the exchange on the AMQP broker by declaring the exchange
        on the current context channel.
        """
        _exchange_name = self.blueprint['exchange']
        LOGGER.debug('Binding exchange {EXCHANGE_NAME} to producer'.format(
            EXCHANGE_NAME=_exchange_name
        )
        )
        self._consumer.exchange = _exchange_name
        LOGGER.debug('Binding exchange {EXCHANGE_NAME} to producer OK!'.format(
            EXCHANGE_NAME=_exchange_name
        )
        )

    def setup_queue(self):
        self._consumer.queue = self.blueprint['queue']

    def setup_callback_method(self):
        self._consumer.callback_method = self.blueprint['callback_method']

    def setup_qos(self):
        self._consumer.qos = self.blueprint['qos']

    def setup_variation(self):
        if 'reference' in self.blueprint:
            if 'correlation_id' in self.blueprint['reference']:
                print("SVRC")
                modified_consumer = self._consumer
                modified_consumer.correlation_id_reference = self.blueprint['reference']['correlation_id']['value']
                modified_consumer.start = self.blueprint['reference']['correlation_id']['start_method'].__get__(modified_consumer, type(modified_consumer))
                modified_consumer.on_message = self.blueprint['reference']['correlation_id']['on_message_method'].__get__(modified_consumer, type(modified_consumer))
                self._consumer = modified_consumer
            else:
                pass
        else:
            pass

    def elevate_setup(self):
        self._consumer.set_qos()


class SynchronousConsumer:
    def __init__(self):
        self.channel = None
        self.exchange = None
        self.queue = None
        self.callback_method = lambda body, properties: None
        self.qos = 1

    def set_qos(self):
        """
        This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before the AMQP broker will deliver another one. Do experiment
        with different prefetch values to achieve desired performance.
        """
        self.channel.basic_qos(prefetch_count=self.qos)

    def start(self, count=-1):
        current_count = 0
        while current_count != count:
            current_count += 1
            self.start_consuming()

    def start_consuming(self):
        """
        This method issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with the AMQP broker. The method keeps the value globally to use
        it when we want to cancel consuming. The on_message method is passed
        in as a callback pika will invoke when a message is fully received.
        """
        self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self.on_message
        )
        self.channel.start_consuming()

    def on_message(self, channel, method, properties, body):
        """
        Invoked by pika when a message is delivered from the AMQP broker. The
        channel is passed for convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param channel: The channel object.
        :type channel: pika.channel.Channel

        :param method: basic_deliver method.
        :type method: pika.Spec.Basic.Deliver

        :param properties: The properties.
        :type properties: pika.Spec.BasicProperties

        :param body: The message body.
        :type body: bytes
        """
        try:
            self.callback_method(json.loads(body), properties)
            self.acknowledge_message(method.delivery_tag)
        except Exception:
            LOGGER.exception("Synchronous callback method exception:")
        self.channel.stop_consuming()

    # TODO: Integrate this as a choice
    def acknowledge_message(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)


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
                              'or destroy the current builder with {MANAGER}.destroy_builder()'.format(
                MANAGER=self.__class__.__name__
            )
            )
        if coating.__class__.__name__ not in _c__all__:
            raise AttributeError('The provided coating is not valid')
        blueprint = self.wingman.resolve(coating=coating)
        ptype_check = Ptypes._map(eip=blueprint["type"], ptype=ptype)
        if ptype_check["map"] == Ptypes.SYNC_PRODUCER:
            self.builder = SynchronousProducerBuilder(connection=connection, blueprint=blueprint)
        elif ptype_check["map"] == Ptypes.SYNC_CONSUMER:
            self.builder = SynchronousConsumerBuilder(connection=connection, blueprint=blueprint)
        elif ptype_check["map"] == Ptypes.ASYNC_CONSUMER:
            self.builder = AsynchronousConsumerBuilder(connection=connection, blueprint=blueprint)
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

    def deliver_consumer(self):
        """
        This method invokes the building of a consumer by the builder
        and delivers it to the Pokeman.

        :return: The consumer object
        """
        self.builder.digest_blueprint()
        self.builder.start_independent_build()
        return self.builder.consumer

    def destroy_builder(self):
        """
        This method destroys the current builder attached to the Foreman.
        Lunch-break for that guy.
        """
        self.builder = None
