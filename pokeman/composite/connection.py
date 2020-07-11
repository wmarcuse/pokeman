from enum import Enum
from abc import ABC, abstractmethod

import pika
import time

import logging
from pokeman.composite.config.configs import BasicConfig

LOGGER = logging.getLogger(__name__)


class ConnectionType(Enum):
    URL = {'type': 'URL', 'format': 'amqp://<user>:<password>@<host>:<port>'}
    HOST = {'type': 'HOST', 'format': "<resource-dns>"}


class AbstractConnection(ABC):
    """
    Abstract connection composite structure.
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
        self._connection_string = connstr
        self._method = method
        self._username = username
        self._password = password
        self._config = config

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
            'connection_attempts': self._config.CONNECTION_ATTEMPTS,
            'heartbeat': self._config.HEARTBEAT,
            'retry_delay': self._config.RETRY_DELAY
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

    @abstractmethod
    def connect(self, poker_id):
        """
        Abstract connect method.
        """
        pass  # pragma:

    @abstractmethod
    def disconnect(self, connection):
        """
        Abstract disconnect method.

        :param connection: The provided connection.
        :type connection: pokeman.Pokeman.connection
        """
        pass


class BasicConnection(AbstractConnection):
    """
    The Blocking composite connection with the AMQP broker.
    """

    # def __init__(self, connstr, method=ConnectionType.URL, username='guest', password='guest', config=None):
    #     """
    #     Setup the connection object, passing in the connection
    #     string and the connection method to connect to AMQP broker.
    #
    #     Note that the connection itself is not instantiated, this is
    #     done by a Pokeman instance. Therefore this connection configuration
    #     object can be passed to multiple Pokeman's.
    #
    #     :param connstr: The string for connecting to AMQP broker, depends on the method.
    #     :type connstr: str
    #
    #     :param method: The connection method as defined in the ConnectionType class.
    #     :type method: ConnectionType
    #
    #     :param username: The username if the connection method is not URL.
    #     :type username: str
    #
    #     :param password: The password if the connection method is not URL.
    #     :type password: str
    #
    #     :param config: The configuration for the AMQP broker connection.
    #     :type config: pokeman.composite.config.*
    #
    #     .. note::
    #         If the connection string is an url it should look something like this:
    #         *amqp://guest:guest@localhost:5672*
    #
    #     .. note::
    #         The self.config variable is declared global, in this way the broker/producer
    #         settings can be changed during loops and runtime. The config settings are
    #         defined global/not-global by their '_' prefix in the key.
    #     """
    #     super().__init__(connstr=connstr, method=method, username=username, password=password, config=config)

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
        if self._method == ConnectionType.URL:
            parameters = pika.URLParameters(
                url=self.resolve_uri_parameters(uri=self._connection_string)
            )
        elif self._method == ConnectionType.HOST:
            parameters = pika.ConnectionParameters(
                host=self._connection_string,
                credentials=pika.credentials.PlainCredentials(
                    username=self._username,
                    password=self._password,
                ),
                connection_attempts=self._config.CONNECTION_ATTEMPTS,
                heartbeat=self._config.HEARTBEAT,
                retry_delay=self._config.RETRY_DELAY
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
                return _connection
        # TODO MAYBE: _connection.add_on_connection_blocked_callback(self.on_connection_blocked)
        #   def on_connection_blocked(self, _unused_connection, _unused_fram):
        #       """
        #       This method will be called if the blocked_connection_timeout limit is overdue.
        #       :return:
        #       """
        #       pass

    def disconnect(self, connection):
        """
        This method disconnects the connection with the AMQP broker.

        :param connection: The provided connection.
        :type connection: pokeman.Pokeman.connection
        """
        if connection is not None:
            LOGGER.debug('Closing Pokeman connection')
            try:
                connection.close()
            except Exception:
                LOGGER.exception('Closing Pokeman connection FAILED!')
                raise ConnectionError('Closing Pokeman connection FAILED!')
            else:
                if connection is not None and not connection.is_closed:
                    raise ConnectionError('Closing Pokeman connection FAILED!')
                LOGGER.debug('Closing Pokeman connection OK!')



