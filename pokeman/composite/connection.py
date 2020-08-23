from enum import Enum
from abc import ABC, abstractmethod

import pika
import time
from uuid import uuid4

import logging
from pokeman.composite.config.configs import BasicConfig

LOGGER = logging.getLogger(__name__)


class ConnectionType(Enum):
    URL = {'type': 'URL', 'format': 'amqp://<user>:<password>@<host>:<port>'}
    HOST = {'type': 'HOST', 'format': "<resource-dns>"}


class ChannelStatus(Enum):
    INACTIVE = 0x0
    ACTIVE = 0x1
    CLOSED = 0x2


class AbstractConnectionParameters(ABC):
    """
    Abstract connection parameters class
    """

    def __init__(self, connstr, method=ConnectionType.URL, username='guest', password='guest', config=BasicConfig()):
        """
        Abstract initialization method for the connection object.

        :param connstr: The provided connection string.
        :type connstr: str

        :param method: The provided connection method.
        :type method: ConnectionType

        :param username: The provided AMQP broker username.
        :type username: str

        :param password: The provided AMQP broker password.
        :type password: str

        :param config: The provided connection configuration object.
        :type config: BasicConfig
        """
        self.connection_string = connstr
        self.method = method
        self.username = username
        self.password = password
        self.config = config

    def _append_url_parameters(self, uri):
        """
        Appends the connection configuration parameters to the connection
        url.

        :param uri: The string for connecting to AMQP broker.
        :type uri: str

        :return: The connection string with parameters.
        :rtype: str

        .. note::
            the uri should look something like this:
            *amqp://guest:guest@localhost:5672*
        """
        parameters = '?'
        config = {
            'connection_attempts': self.config.CONNECTION_ATTEMPTS,
            'heartbeat': self.config.HEARTBEAT,
            'retry_delay': self.config.RETRY_DELAY
        }
        for param, value in config.items():
            if parameters != '?':
                parameters += '&'
            parameters += ('{PARAM}={VALUE}'.format(PARAM=param,
                                                    VALUE=value))
        connection_url = uri + parameters
        return connection_url

    def resolve_uri_parameters(self, uri):
        """
        This method resolves the uri parameters from the provided connection
        string.

        :param uri: The provided AMQP broker connection string.
        :type uri: str

        :return: The resolved connection uri.
        :rtype: str
        """
        LOGGER.debug('Resolving {CONNECTION} uri parameters'.format(CONNECTION=self.__class__.__name__))
        resolved_uri = self._append_url_parameters(uri)
        LOGGER.debug('Resolving {CONNECTION} uri parameters OK!'.format(CONNECTION=self.__class__.__name__))
        return resolved_uri


class AbstractConnection(ABC):
    """
    Abstract connection composite structure.
    """

    def __init__(self, parameters):
        """
        Abstract initialization method for the connection object.

        :param parameters: The provided connection parameters.
        :type parameters: ConnectionParameters

        """
        self.cp = parameters
        self.connection = None
        self.channels = {}

    def set_channel_status(self, channel_id, status):
        """
        This method sets the channel status.

        :param channel_id: The channel ID
        :type channel_id: str

        :param status: The channel status
        :type status: ChannelStatus
        """
        self.channels[channel_id]['status'] = status

    def open_channel(self):
        """
        This method opens a new channel for the connection.

        :return: The channel ID
        :rtype: str
        """
        channel_id = str(uuid4())
        self.channels[channel_id]['channel'] = self.connection.channel()
        self.set_channel_status(channel_id=channel_id, status=ChannelStatus.INACTIVE)
        return channel_id

    def close_channel(self, channel_id):
        """
        This method closes a new channel for the connection.
        """
        self.channels[channel_id]['channel'].close()
        self.set_channel_status(channel_id=channel_id, status=ChannelStatus.CLOSED)

    def close_all_channels(self):
        """
        This method closes all the open channels for the connection.
        """
        for channel, context in self.channels.items():
            if context['status'] == ChannelStatus.INACTIVE or context['status'] == ChannelStatus.ACTIVE:
                context['channel'].close()

    @abstractmethod
    def connect(self, poker_id):
        """
        Abstract connect method.
        """
        pass  # pragma:

    @abstractmethod
    def disconnect(self):
        """
        Abstract disconnect method.
        """
        pass


class ConnectionParameters(AbstractConnectionParameters):
    pass


class Connection(AbstractConnection):
    """
    The Blocking composite connection with the AMQP broker.
    """

    def connect(self, poker_id):
        """
        This method connects to AMQP broker, and sets the object connection variable.

        :return: The pika connection handle.
        :rtype: pika.BlockingConnection

        .. note::
            The connection parameters are set depending on the connection type:
                * pika.URLParameters(url) when the ConnectionType == URL, useful
                  when connecting to remote AMQP server.
                * pika.ConnectionParameters(host) when the ConnectionType == HOST,
                  useful when connecting to Docker container in same cluster.

        .. note::
            The while loop assures that the producer will fail to start due tot an
            AMQPConnectionError that is the result of AMQP broker still in starting up
            phase.
        """
        LOGGER.debug('Pokeman {POKER_ID} connecting to AMQP broker'.format(
            POKER_ID=poker_id
        )
        )
        parameters = None
        connected = False
        if self.cp.method == ConnectionType.URL:
            parameters = pika.URLParameters(
                url=self.cp.resolve_uri_parameters(uri=self.cp.connection_string)
            )
        elif self.cp.method == ConnectionType.HOST:
            parameters = pika.ConnectionParameters(
                host=self.cp.connection_string,
                credentials=pika.credentials.PlainCredentials(
                    username=self.cp.username,
                    password=self.cp.password,
                ),
                connection_attempts=self.cp.config.CONNECTION_ATTEMPTS,
                heartbeat=self.cp.config.HEARTBEAT,
                retry_delay=self.cp.config.RETRY_DELAY
            )
        retries = 0
        _connection = None
        while not connected and retries < 6:
            time.sleep(2)
            retries += 1
            try:
                _connection = pika.BlockingConnection(parameters=parameters)
            # TODO: Too broad exception handling here, zoom-in
            except AttributeError:
                raise AttributeError('The connection object has no attribute close')
            else:
                LOGGER.debug('Pokeman connecting to AMQP broker OK!')
                self.connection = _connection
        # TODO MAYBE: _connection.add_on_connection_blocked_callback(self.on_connection_blocked)
        #   def on_connection_blocked(self, _unused_connection, _unused_fram):
        #       """
        #       This method will be called if the blocked_connection_timeout limit is overdue.
        #       :return:
        #       """
        #       pass

    def disconnect(self):
        """
        This method disconnects the connection with the AMQP broker.
        """
        if self.connection is not None:
            LOGGER.debug('Closing Pokeman connection')
            try:
                self.connection.close()
            except Exception:
                LOGGER.exception('Closing Pokeman connection FAILED!')
                raise ConnectionError('Closing Pokeman connection FAILED!')
            else:
                if self.connection is not None and not self.connection.is_closed:
                    raise ConnectionError('Closing Pokeman connection FAILED!')
                LOGGER.debug('Closing Pokeman connection OK!')


class SelectConnection(AbstractConnection):
    """
    The Selecting composite connection with the AMQP broker.
    """
    def connect(self, poker_id):
        """
        This method connects to AMQP broker, and sets the object connection variable.

        .. note::
            The connection parameters are set depending on the connection type:
                * pika.URLParameters(url) when the ConnectionType == URL, useful
                  when connecting to remote AMQP server.
                * pika.ConnectionParameters(host) when the ConnectionType == HOST,
                  useful when connecting to Docker container in same cluster.

        .. note::
            The while loop assures that the producer will fail to start due tot an
            AMQPConnectionError that is the result of AMQP broker still in starting up
            phase.
        """
        LOGGER.debug('Pokeman {POKER_ID} connecting to AMQP broker'.format(
            POKER_ID=poker_id
        )
        )
        parameters = None
        connected = False
        if self.cp.method == ConnectionType.URL:
            parameters = pika.URLParameters(
                url=self.cp.resolve_uri_parameters(uri=self.cp.connection_string)
            )
        elif self.cp.method == ConnectionType.HOST:
            parameters = pika.ConnectionParameters(
                host=self.cp.connection_string,
                credentials=pika.credentials.PlainCredentials(
                    username=self.cp.username,
                    password=self.cp.password,
                ),
                connection_attempts=self.cp.config.CONNECTION_ATTEMPTS,
                heartbeat=self.cp.config.HEARTBEAT,
                retry_delay=self.cp.config.RETRY_DELAY
            )
        retries = 0
        _connection = None
        while not connected and retries < 6:
            time.sleep(2)
            retries += 1
            try:
                _connection = pika.SelectConnection(
                    parameters=parameters
                )
            # TODO: Too broad exception handling here, zoom-in
            except AttributeError:
                raise AttributeError('The connection object has no attribute close')
            else:
                LOGGER.debug('Pokeman connecting to AMQP broker OK!')
                self.connection = _connection

    def disconnect(self):
        """
        This method disconnects the connection with the AMQP broker.
        """
        if self.connection is not None and not self.connection.is_closing and not self.connection.is_closed:
            LOGGER.debug('Closing Pokeman connection')
            try:
                self.connection.close()
            except Exception:
                LOGGER.exception('Closing Pokeman connection FAILED!')
                raise ConnectionError('Closing Pokeman connection FAILED!')
            else:
                if self.connection is not None and not self.connection.is_closed:
                    raise ConnectionError('Closing Pokeman connection FAILED!')
                LOGGER.debug('Closing Pokeman connection OK!')
