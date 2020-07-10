from inspect import currentframe
import pika
from pika.exceptions import AMQPConnectionError, UnroutableError
import json
import time
import uuid

# from pokeman._composite.connection import ConnectionType
# from pokeman._components.coating.abstract_coating import CoatingFactory
# from pokeman._components.coating.request_reply import RequestReply

import logging

LOGGER = logging.getLogger(__name__)


class SynchronousProducer:
    """
    This is an synchronous producer.
    """

    def __init__(self, connection, coating):

        # Producer parameters
        self._connection = None
        self._channel = None
        self._stopping = False
        self._started_publishing = False
        self._connection = connection
        self._async = False
        self._message_number = None
        self.correlations = []
        self.correlation_data = {}

        # Coating parameters
        self._coating = coating
        self._resolved_coating = None
        self._additional_channels = []
        self._additional_handlers = {}
        self._callback_queue = None
        self._response_queue = None
        self._callback_exchange = None
        self._callback_routing_key = None
        self._callback_method = None
        self._auto_acknowledge = None
        self._response = None
        self._response_body = None

    def open_additional_channel(self):
        """
        This method will open an additional channel with AMQP broker by issuing the
        Channel.
        """
        LOGGER.debug("Creating an additional channel")
        return self._connection.channel()

    def open_channel(self):
        """
        This method will open a new channel with AMQP broker by issuing the
        Channel.
        """
        LOGGER.debug("Creating a new channel")
        self._channel = self._connection.channel()

    def apply_delivery_confirmations(self):
        """
        This method will apply delivery confirmations on a BlockingConnection.

        .. note::
            When the message is not received correctly a pika.exceptions.
            UnroutableError will be raised.
        """
        self._channel.confirm_delivery()
        LOGGER.debug("Delivery confirmations set in channel")

    def setup_exchange(self):
        """
        Setup the exchange on AMQP broker by invoking the Exchange.
        """
        LOGGER.debug("Declaring exchange {EXCHANGE_NAME}".format(EXCHANGE_NAME=self.config.EXCHANGE_NAME))
        self._channel.exchange_declare(
            exchange=self.config.EXCHANGE_NAME,
            exchange_type=self.config._EXCHANGE_TYPE
        )

    def setup_queue(self):
        """
        Setup the queue on AMQP broker by invoking the Queue.
        """
        LOGGER.debug("Declaring queue {QUEUE_NAME}".format(QUEUE_NAME=self.config.QUEUE))
        self._channel.queue_declare(queue=self.config.QUEUE,
                                    durable=self.config.QUEUE_DURABLE)
        self._channel.queue_bind(queue=self.config.QUEUE,
                                 exchange=self.config.EXCHANGE_NAME,
                                 routing_key=self.config.ROUTING_KEY)
        LOGGER.debug("Binding queue {QUEUE_NAME} to exchange {EXCHANGE_NAME} and routing key {ROUTING_KEY}".format(
            QUEUE_NAME=self.config.QUEUE,
            EXCHANGE_NAME=self.config.EXCHANGE_NAME,
            ROUTING_KEY=self.config.ROUTING_KEY
        ))

    def on_response_body(self, channel, method, properties, body):
        LOGGER.debug("PRE: " + str(body))
        for correlation in self.correlations:
            if correlation == properties.correlation_id:
                self._response = body
                LOGGER.debug("BODY: " + str(body))
                self.stop()
                return

    def callback_wrapper(self, callback_method, channel, method, properties, body):
        body = callback_method(channel, method, properties, body)
        channel.stop_consuming()
        self._response_body = body

    def get_response(self):
        if self._response_queue is not None:
            _channel = self._additional_handlers[self._response_queue]["channel"]
            _channel.basic_consume(
                queue=self._response_queue,
                on_message_callback=lambda channel, method, properties, body: self.callback_wrapper(
                    callback_method=self._callback_method,
                    channel=channel,
                    method=method,
                    properties=properties,
                    body=body
                )
            )
            _channel.start_consuming()
            return self._response_body

        else:
            return None

    def setup_coating(self):
        # TODO: Split in single purpose methods
        if self._coating:
            LOGGER.debug("Coating detected, starting to resolve")
            self._resolved_coating = CoatingFactory(self._coating)
            if self._resolved_coating:
                resolved_coating = self._resolved_coating.get_coating()
                LOGGER.debug("Coating resolved: {PATTERN}".format(PATTERN=resolved_coating))

                # resolved_additional_channels = self._resolved_coating.get_additional_channels()
                # for _ in resolved_additional_channels:
                #     _channel = self._connection.channel()
                #     self._additional_channels.append(_channel)

                resolved_callback_exchange = self._resolved_coating.get_callback_exchange()
                if resolved_callback_exchange is None:
                    self._callback_exchange = self.config.EXCHANGE_NAME
                else:
                    self._callback_exchange = resolved_callback_exchange

                resolved_callback_routing_key = self._resolved_coating.get_callback_routing_key()
                if resolved_callback_routing_key is None:
                    self._callback_routing_key = "{ROUTING_KEY}.response".format(ROUTING_KEY=self.config.ROUTING_KEY)
                else:
                    self._callback_routing_key = resolved_callback_routing_key

                resolved_prefetch_count = self._resolved_coating.get_prefetch_count()

                resolved_additional_queues = self._resolved_coating.get_additional_queues()
                for _q in resolved_additional_queues:
                    _channel = self._connection.channel()
                    _channel.queue_declare(queue=_q, exclusive=True, auto_delete=True)
                    _channel.queue_bind(
                        queue=_q,
                        exchange=self._callback_exchange,
                        routing_key=self._callback_routing_key
                    )
                    _channel.basic_qos(prefetch_count=resolved_prefetch_count)
                    self._additional_handlers[_q] = {
                        "exchange": self._callback_exchange,
                        "routing_key": self._callback_routing_key,
                        "channel": _channel
                    }

                self._response_queue = self._resolved_coating.get_response_queue()

                resolved_callback_method = self._resolved_coating.get_on_message_callback()
                if resolved_coating == RequestReply.__name__:
                    resolved_callback_method = resolved_callback_method  # bijwerken
                else:
                    LOGGER.warning("The provided coating could not be resolved")
                    resolved_callback_method = self.on_response_body  # bijwerken

                self._callback_method = resolved_callback_method

                # resolved_callback_queue = self._resolved_coating.get_callback_queue()
                # if resolved_callback_queue is None:
                #     self._callback_queue = "{QUEUE}.response".format(QUEUE=self.config.QUEUE)
                # else:
                #     self._callback_queue = resolved_callback_queue

                self._auto_acknowledge = self._resolved_coating.get_auto_acknowledge()
            else:
                LOGGER.warning("The provided coating could not be resolved")
        else:
            LOGGER.warning("No coating provided")
            pass

    def ready_for_publishing(self):
        """
        This does nothing yet.
        """
        LOGGER.debug("Ready for publishing")

    def publish(self, message,
                content_type=None,
                content_encoding=None,
                headers=None,
                delivery_mode=2,
                priority=None,
                correlation_id=None,
                reply_to=None,
                expiration=None,
                message_id=None,
                timestamp=None,
                type=None,
                user_id=None,
                cluster_id=None
                ):
        """
        If the class is not stopping, publish a message to AMQP broker, incrementing
        the number of messages variable.

        :param message: The message
        :type message: str|unicode|dict|list|int|float

        :param headers: The headers, unicode key,values are allowed
        :type headers: dict

        .. note::
            The first if statement checks if the channel is not dead or
            already in use (open).
        """


        if self._channel is None or not self._channel.is_open:
            return

        if not correlation_id:
            correlation_id = str(uuid.uuid4())

        if not timestamp:
            timestamp = int(time.time())

        if self._resolved_coating:
            reply_to = self._callback_queue
        LOGGER.debug("CALLBACK: " + str(reply_to))

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
            app_id=self.config._APP_ID,
            cluster_id=cluster_id
        )

        message = message
        try:
            self._channel.basic_publish(
                exchange=self.config.EXCHANGE_NAME,
                routing_key=self.config.ROUTING_KEY,
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
        # if self._started_publishing is False:
        #     self._started_publishing = True
        #     self.setup_coating()
        return {
            "correlation_id": correlation_id,
            "timestamp": timestamp
        }

    def run(self):
        """
        Run the producer synchronous by connecting, setting up the desired
        behaviour between the producer and the broker.
        """
        if not self._stopping:
            self._connection = None
            self._message_number = 0
            self._async = False
            self.connect()
            self.open_channel()
            self.apply_delivery_confirmations()
            self.setup_exchange()
            self.setup_queue()
            self.setup_coating()
            self.ready_for_publishing()

    def stop(self):
        """
        Stop the producer by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published.
        """
        LOGGER.debug("Stopping")
        self._stopping = True
        self.close_channel()
        self.close_connection()

        if self._connection is not None and not self._connection.is_closed:
            LOGGER.error("Stopping went wrong")

        LOGGER.debug("Stopped")

    def close_channel(self):
        """
        Invoke this command to close the channel with AMQP broker by sending
        the Channel.Close RPC command.
        """
        if self._channel is not None:
            LOGGER.debug("Closing the channel")
            self._channel.close()

    def close_connection(self):
        """
        This method closes the connection with AMQP broker.
        """
        if self._connection is not None:
            LOGGER.debug("Closing connection")
            self._connection.close()
