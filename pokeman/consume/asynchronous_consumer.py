from inspect import currentframe
import functools
import time
import pika

from pokeman.utils.locator import topic_locator
from pokeman.composite.connection import ConnectionType
from pokeman.composite.config.asynchronous_consumer_topic_exchange_config import AsyncConsumerTopicExchangeConfig
from pokeman.utils.task import TaskStatus

import logging

LOGGER = logging.getLogger(__name__)


class AsyncConsumer(object):
    """
    This is an AMQP broker asynchronous consumer.
    """

    _frame = currentframe().f_back
    TOPIC, EVENT = topic_locator(frame=_frame)
    _config = AsyncConsumerTopicExchangeConfig(
        topic=TOPIC,
        event=EVENT
    )

    def __init__(self, connstr, method=ConnectionType.URL, username="guest", password="guest",
                 task_db_uri="", config=_config):
        """
        Setup the asynchronous consumer object, passing in the connection
        string and the connection method to connect to the AMQP broker.

        :param connstr: The string for connecting to the AMQP broker, depends on the method.
        :type connstr: str

        :param method: The connection method as defined in the ConnectionType class.
        :type method: ConnectionType

        :param username: The username if the connection method is not URL.
        :type username: str

        :param password: The password if the connection method is not URL.
        :type password: str

        :param task_db_uri: The database location of the tasks
        :type task_db_uri: str

        .. note::
            If the connection string is an url it should look something like this:
            *amqp://guest:guest@localhost:5672/*.

        .. note::
            The self.config variable is declared global, in this way the broker/consumer
            settings can be changed during loops and runtime. The config settings are
            defined global/not-global by their "_" prefix in the key.
        """
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

        self._connection_string = connstr
        self._method = method
        self._username = username
        self._password = password
        self._async = True

        self.config = config

        self._func = None
        self._kwarg_dict = {}

    def append_url_parameters(self, url):
        """
        Appends the consumer connection parameters to the connection url.

        :param url: The string for connecting to the AMQP broker.
        :type url: str

        :return: The connection string with parameters.
        :rtype: str

        .. note::
            the url should look something like this:
            *amqp://guest:guest@localhost:5672/*.
        """
        parameters = "?"
        config = {
            "connection_attempts": self.config.CP_CONNECTION_ATTEMPTS,
            "heartbeat": self.config.CP_HEARTBEAT
        }
        for param, value in config.items():
            if parameters != "?":
                parameters += "&"
            parameters += ("{PARAM}={VALUE}".format(PARAM=param,
                                                    VALUE=value))
        connection_url = url + parameters
        return connection_url

    def connect(self):
        """
        This method connects to the AMQP broker, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        .. note::
            The connection parameters are set depending on the connection type:
                * pika.URLParameters(url) when the ConnectionType == URL, useful
                  when connecting to remote AMQP server.
                * pika.ConnectionParameters(host) when the ConnectionType == HOST,
                  useful when connecting to Docker container in same cluster.

        .. note::
            The connection related while loop does not handle any connection
            problems with the broker, the on_open_error_callback method handles
            that. But the while loop handles any problems related to pika while
            trying to set up the connection parameters.

        :return: The pika connection handle.
        :rtype: pika.SelectConnection
        """
        LOGGER.debug("Connecting to AMQP broker")
        parameters = None
        connected = False
        if self._method == ConnectionType.URL:
            parameters = pika.URLParameters(
                url=self.append_url_parameters(self._connection_string)
            )
        elif self._method == ConnectionType.HOST:
            parameters = pika.ConnectionParameters(
                host=self._connection_string,
                credentials=pika.credentials.PlainCredentials(
                    username=self._username,
                    password=self._password,
                ),
                connection_attempts=self.config.CP_CONNECTION_ATTEMPTS,
                heartbeat=self.config.CP_HEARTBEAT,
                blocked_connection_timeout=self.config.CP_BLOCKED_CONNECTION_TIMEOUT
            )

        retries = 0
        connection = None
        while not connected and retries < 6:
            time.sleep(2)
            retries += 1
            try:
                connection = pika.SelectConnection(
                    parameters=parameters,
                    on_open_callback=self.on_connection_open,
                    on_open_error_callback=self.on_connection_open_error,
                    on_close_callback=self.on_connection_closed)
                connected = True
                LOGGER.debug("Trying to set connection")
            except Exception as e:
                LOGGER.exception("Trying to set connection attempt failed, retries: {TRIES}".format(TRIES=retries))
                pass

        return connection

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.debug("Connection is closing or already closed")
        else:
            LOGGER.debug("Closing connection")
            self._connection.close()

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
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
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
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        This method is invoked by pika when the channel has been opened.
        The channel object is passed in so this method can make use of it.
        Since the channel is now open, the exchange to use is declared.

        :param channel: The channel object.
        :type channel: pika.channel.Channel
        """
        LOGGER.debug("Channel opened")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(exchange_name=self.config.EXCHANGE_NAME)

    def add_on_channel_close_callback(self):
        """
        This method tells pika to call the on_channel_closed method if
        the AMQP broker unexpectedly closes the channel.
        """
        LOGGER.debug("Adding channel close callback")
        self._channel.add_on_close_callback(self.on_channel_closed)

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

    def setup_exchange(self, exchange_name):
        """
        Setup the exchange on the AMQP broker by invoking the Exchange. Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param exchange_name: The name of the exchange to declare.
        :type exchange_name: str|unicode
        """
        LOGGER.debug("Declaring exchange {EXCHANGE_NAME}".format(EXCHANGE_NAME=exchange_name))
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.config._EXCHANGE_TYPE,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        """
        Invoked by pika when the AMQP broker has finished the Exchange. Declare RPC
        command. Call the queue setup method.

        :param _unused_frame: Exchange.DeclareOk response frame.
        :type _unused_frame: pika.Frame.Method

        :param userdata: Extra user data (exchange name).
        :type userdata: str|unicode
        """
        LOGGER.debug("Exchange declared: {USERDATA}".format(USERDATA=userdata))
        self.setup_queue(queue_name=self.config.QUEUE)

    def setup_queue(self, queue_name):
        """
        Setup the queue on the AMQP broker by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param queue_name: The name of the queue to declare.
        :type queue_name: str|unicode
        """
        LOGGER.debug("Declaring queue {QUEUE_NAME}".format(QUEUE_NAME=queue_name))
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, durable=self.config.QUEUE_DURABLE, callback=cb)

    def on_queue_declareok(self, _unused_frame, userdata):
        """
        Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method the method will bind the queue
        and exchange together with the routing key by issuing the Queue. Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param _unused_frame: The Queue.DeclareOk frame.
        :type _unused_frame: pika.frame.Method

        :param userdata: Extra user data (queue name).
        :type userdata: str|unicode
        """
        queue_name = userdata
        LOGGER.debug("Binding {EXCHANGE} to {QUEUE} with {ROUTING_KEY}".format(EXCHANGE=self.config.EXCHANGE_NAME,
                                                                              QUEUE=queue_name,
                                                                              ROUTING_KEY=self.config.ROUTING_KEY))
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue=queue_name,
            exchange=self.config.EXCHANGE_NAME,
            routing_key=self.config.ROUTING_KEY,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        """
        This method is invoked by pika when it receives the Queue.BindOk
        response from the AMQP broker. At this point we will set the prefetch
        count for the channel.

        :param _unused_frame: The Queue.BindOk frame.
        :type _unused_frame: pika.frame.Method

        :param userdata: Extra user data (queue name).
        :type userdata: str|unicode
        """
        LOGGER.debug("Queue bound: {QUEUE}".format(QUEUE=userdata))
        self.set_qos()

    def set_qos(self):
        """
        This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before the AMQP broker will deliver another one. Do experiment
        with different prefetch values to achieve desired performance.
        """
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        """
        This method is invoked by pika when the Basic.QoS method has completed.
        At this point the consumer will start consuming messages by calling the
        self.start_consuming() method which will invoke the needed RPC commands
        to start the process.

        :param _unused_frame: The Basic.QosOk response frame
        :type _unused_frame: pika.frame.Method
        """
        LOGGER.debug("QOS set to: {PREFETCH_COUNT}".format(PREFETCH_COUNT=self._prefetch_count))
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
        self._consumer_tag = self._channel.basic_consume(
            queue=self.config.QUEUE, on_message_callback=self.on_message)

        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """
        Add a callback that will be invoked if the AMQP broker cancels the consumer
        for some reason. If the AMQP broker does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        LOGGER.debug("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """
        This method is invoked by pika when the AMQP broker sends a Basic.Cancel
        for a consumer receiving messages.

        :param method_frame: The Basic.Cancel frame
        :type method_frame: pika.frame.Method
        """
        LOGGER.debug("Consumer was cancelled remotely, shutting down: {METHOD_FRAME}".format(METHOD_FRAME=method_frame))
        if self._channel:
            self._channel.close()

    def on_message(self, channel, basic_deliver, properties, body):
        """
        Invoked by pika when a message is delivered from the AMQP broker. The
        channel is passed for convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param channel: The channel object.
        :type channel: pika.channel.Channel

        :param basic_deliver: basic_deliver method.
        :type basic_deliver: pika.Spec.Basic.Deliver

        :param properties: The properties.
        :type properties: pika.Spec.BasicProperties

        :param body: The message body.
        :type body: bytes

        .. note::
            The return statement in the except statement is for avoiding
            a dead loop when the parsed function fails.
        """
        LOGGER.debug("Received message # {DELIVERY_TAG} from {APP_ID}".format(DELIVERY_TAG=basic_deliver.delivery_tag,
                                                                             APP_ID=properties.app_id))
        self.acknowledge_message(basic_deliver.delivery_tag)
        headers = properties.headers
        try:
            self._func(**self._kwarg_dict)
            LOGGER.info(str(body))
        except Exception as e:
            # CriticalHandler.mail_admin(e)  # TODO: Add some critical handler here
            LOGGER.exception(e)
            channel.basic_publish(exchange="search_engine",
                                  routing_key="search_engine.request.response",
                                  properties=pika.BasicProperties(correlation_id=properties.correlation_id,),
                                  body="Status: {STATUS} | Message: {MESSAGE}".format(STATUS=TaskStatus.FAILED,
                                                                                      MESSAGE="Exception occured"))
            return
        time.sleep(10)
        LOGGER.debug("Calback published 1")
        self._channel.basic_publish(exchange="search_engine",
                              routing_key="search_engine.request.response",
                              properties=pika.BasicProperties(correlation_id=properties.correlation_id, ),
                              body="Status: {STATUS} | Message: {MESSAGE}".format(STATUS=TaskStatus.SUCCESS,
                                                                                  MESSAGE="Message handled perfectly!"))
        LOGGER.debug("Calback published")

    def acknowledge_message(self, delivery_tag):
        """
        Acknowledge the message delivery from the AMQP broker by sending a
        Basic.Ack RPC method for the delivery tag.

        :param delivery_tag: The delivery tag from the Basic.Deliver frame.
        :type delivery_tag: int
        """
        LOGGER.debug("Acknowledging message {DELIVERY_TAG}".format(DELIVERY_TAG=delivery_tag))
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """
        Tell the AMQP broker that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            LOGGER.debug("Sending a Basic.Cancel RPC command to the AMQP broker")
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

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
        self._channel.close()

    def run(self, func, **kwargs):
        """
        Run the consumer by connecting to the AMQP broker and then starting
        the IOLoop to block and allow the SelectConnection to operate.
        """
        self._func = func
        for key, value in kwargs.items():
            self._kwarg_dict[key] = value
        self._connection = self.connect()
        self._connection.ioloop.start()

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
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.debug("Stopped")


class ReconnectingAsyncConsumer(object):
    """
    This is an consumer that will reconnect if the nested
    AsyncConsumer indicates that a reconnect is necessary.
    """

    def __init__(self, connstr, config, method=ConnectionType.URL, username="guest", password="guest", task_db_uri=""):
        self._reconnect_delay = 0
        self._connstr = connstr
        self._config = config
        self._method = method
        self._username = username
        self._password = password
        self._task_db_uri = task_db_uri
        self._consumer = AsyncConsumer(connstr=self._connstr,
                                       config=self._config,
                                       method=self._method,
                                       username=self._username,
                                       password=self._password,
                                       task_db_uri=self._task_db_uri)

    def run(self, func, **kwargs):
        while True:
            try:
                self._consumer.run(func, **kwargs)
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.debug("Reconnecting after {SECONDS} seconds".format(SECONDS=reconnect_delay))
            time.sleep(reconnect_delay)
            self._consumer = AsyncConsumer(connstr=self._connstr,
                                           method=self._method,
                                           username=self._username,
                                           password=self._password,
                                           task_db_uri=self._task_db_uri)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
