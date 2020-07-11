import unittest
from unittest import mock
import pika

from tests import TestAttributes as TA
from pokeman.composite import connection
from pokeman.composite.config import configs


class ConnectionTypetests(unittest.TestCase):
    def setUp(self):
        self.ConnectionType = connection.ConnectionType

    def test_base_name(self):
        self.assertEqual(self.ConnectionType.__name__, 'ConnectionType')

    def test_enums(self):
        self.assertEqual(self.ConnectionType.URL.name, 'URL')
        self.assertEqual(self.ConnectionType.HOST.name, 'HOST')


class AbstractConnectionTests(unittest.TestCase):
    def test_base_name(self):
        self.assertEqual(connection.AbstractConnection.__name__, 'AbstractConnection')

    @mock.patch.object(connection.AbstractConnection, '__abstractmethods__', set())
    def test_base_initialization(self):
        self.basic_config = configs.BasicConfig()
        self.abc_connection = connection.AbstractConnection(
            connstr='amqp://username:password@localhost:5672',
            method=connection.ConnectionType.URL,
            config=self.basic_config
        )
        self.assertEqual(self.abc_connection._connection_string, 'amqp://username:password@localhost:5672')
        self.assertEqual(self.abc_connection._method, connection.ConnectionType.URL)
        self.assertIsInstance(self.abc_connection._config, configs.BasicConfig)


class BasicConnectionTests(unittest.TestCase):
    class ConnectionMock:
        def __init__(self, status):
            self.status = status

        def close(self):
            if self.status == 'success':
                self.is_closed = True
            else:
                self.is_closed = False

    def setUp(self):
        self.basic_config = configs.BasicConfig()
        self.basic_connection = connection.BasicConnection(
            connstr='amqp://username:password@localhost:5672',
            method=connection.ConnectionType.URL,
            config=self.basic_config
        )
        self.basic_connection_host = connection.BasicConnection(
            connstr='localhost',
            method=connection.ConnectionType.HOST,
            username='username',
            password='password',
            config=self.basic_config
        )

    def test_base_name(self):
        self.assertEqual(connection.BasicConnection.__name__, 'BasicConnection')

    def test_base_initialization_url(self):
        self.assertEqual(self.basic_connection._connection_string, 'amqp://username:password@localhost:5672')
        self.assertEqual(self.basic_connection._method, connection.ConnectionType.URL)
        self.assertIsInstance(self.basic_connection._config, configs.BasicConfig)

    def test_base_initialization_host(self):
        self.basic_config = configs.BasicConfig()
        self.assertEqual(self.basic_connection_host._connection_string, 'localhost')
        self.assertEqual(self.basic_connection_host._method, connection.ConnectionType.HOST)
        self.assertEqual(self.basic_connection_host._username, 'username')
        self.assertEqual(self.basic_connection_host._password, 'password')
        self.assertIsInstance(self.basic_connection_host._config, configs.BasicConfig)

    def test__append_url_parameters(self):
        self.assertEqual(
            self.basic_connection._append_url_parameters(uri=self.basic_connection._connection_string),
            'amqp://username:password@localhost:5672?connection_attempts=3&heartbeat=3600&retry_delay=1'
        )

    def test_resolve_uri_parameters(self):
        self.assertEqual(
            self.basic_connection.resolve_uri_parameters(uri=self.basic_connection._connection_string),
            'amqp://username:password@localhost:5672?connection_attempts=3&heartbeat=3600&retry_delay=1'
        )

    def test_connect_parameters_and_nested_credentials_success(self):
        def mock_parameters(parameters):
            return parameters
        for connection_method in [
            {
                self.basic_connection: pika.URLParameters
            },
            {
                self.basic_connection_host: pika.ConnectionParameters
            }
        ]:
            with TA.patch(pika, 'BlockingConnection', mock_parameters):
                connection_parameters = [k for k, v in connection_method.items()][0].connect(poker_id='abc')
                self.assertIsInstance(connection_parameters, [v for k, v in connection_method.items()][0])
                self.assertEqual(connection_parameters._host, 'localhost')
                self.assertEqual(connection_parameters._credentials.username, 'username')
                self.assertEqual(connection_parameters._credentials.password, 'password')
                self.assertEqual(connection_parameters._connection_attempts, 3)
                self.assertEqual(connection_parameters._heartbeat, 3600)
                self.assertEqual(connection_parameters._retry_delay, 1)

    def test_connect_parameters_and_nested_credentials_failure(self):
        def mock_connection_error(parameters):
            raise Exception('Pokeman connecting to AMQP broker FAILED!')

        with TA.patch(pika, 'BlockingConnection', mock_connection_error):
            with self.assertRaisesRegex(Exception, 'Pokeman connecting to AMQP broker FAILED!'):
                self.basic_connection.connect(poker_id='abc')

    def test_disconnect_success(self):
        self.assertEqual(self.basic_connection.disconnect(connection=self.ConnectionMock(
            status='success'
        )), None)

    def test_disconnect_failure_connection_could_not_be_closed(self):
        def mock_connection_close_exception():
            raise Exception('Closing Pokeman connection FAILED!')

        with self.assertRaisesRegex(Exception, 'FAILED'):
            self.basic_connection.disconnect(connection=self.ConnectionMock(
                status='failure'
            )
            )

    def test_disconnect_failure_connection_not_is_closed(self):
        with self.assertRaisesRegex(ConnectionError, 'FAILED'):
            self.basic_connection.disconnect(connection=self.ConnectionMock(
                status='failure'
            )
            )


if __name__ == '__main__':
    unittest.main()
