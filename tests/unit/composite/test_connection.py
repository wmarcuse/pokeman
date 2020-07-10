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
            connstr='amqp://guest:guest@localhost:5672',
            method=connection.ConnectionType.URL,
            username='guest',
            password='guest',
            config=self.basic_config
        )
        self.assertEqual(self.abc_connection._connection_string, 'amqp://guest:guest@localhost:5672')
        self.assertEqual(self.abc_connection._method, connection.ConnectionType.URL)
        self.assertEqual(self.abc_connection._username, 'guest')
        self.assertEqual(self.abc_connection._password, 'guest')
        self.assertIsInstance(self.abc_connection._config, configs.BasicConfig)


class BasicConnectionTests(unittest.TestCase):
    def setUp(self):
        self.basic_config = configs.BasicConfig()
        self.basic_connection = connection.BasicConnection(
            connstr='amqp://guest:guest@localhost:5672',
            method=connection.ConnectionType.URL,
            username='guest',
            password='guest',
            config=self.basic_config
        )

    def test_base_name(self):
        self.assertEqual(connection.BasicConnection.__name__, 'BasicConnection')

    def test_base_initialization(self):
        self.assertEqual(self.basic_connection._connection_string, 'amqp://guest:guest@localhost:5672')
        self.assertEqual(self.basic_connection._method, connection.ConnectionType.URL)
        self.assertEqual(self.basic_connection._username, 'guest')
        self.assertEqual(self.basic_connection._password, 'guest')
        self.assertIsInstance(self.basic_connection._config, configs.BasicConfig)

    def test__append_url_parameters(self):
        self.assertEqual(
            self.basic_connection._append_url_parameters(uri=self.basic_connection._connection_string),
            'amqp://guest:guest@localhost:5672?connection_attempts=3&heartbeat=3600&retry_delay=1'
        )

    def test_resolve_uri_parameters(self):
        self.assertEqual(
            self.basic_connection.resolve_uri_parameters(uri=self.basic_connection._connection_string),
            'amqp://guest:guest@localhost:5672?connection_attempts=3&heartbeat=3600&retry_delay=1'
        )

    def test_connect_parameters_and_nested_credentials(self):
        def mock_parameters(parameters):
            return parameters

        with TA.patch(pika, 'BlockingConnection', mock_parameters):
            connection_parameters = self.basic_connection.connect(poker_id='abc')
            self.assertIsInstance(connection_parameters, pika.URLParameters)
            self.assertEqual(connection_parameters._host, 'localhost')
            self.assertEqual(connection_parameters._credentials.username, 'guest')
            self.assertEqual(connection_parameters._credentials.password, 'guest')
            self.assertEqual(connection_parameters._connection_attempts, 3)
            self.assertEqual(connection_parameters._heartbeat, 3600)
            self.assertEqual(connection_parameters._retry_delay, 1)

    def test_disconnect(self):
        class ConnectionMock:
            def __init__(self, status):
                self.status = status

            def close(self):
                if self.status == 'success':
                    self.is_closed = True
                else:
                    self.is_closed = False
        self.assertEqual(self.basic_connection.disconnect(connection=ConnectionMock(
            status='success'
        )), None)
        with self.assertRaisesRegex(ConnectionError, 'FAILED'):
            self.basic_connection.disconnect(connection=ConnectionMock(
                status='failure'
            )
            )

if __name__ == '__main__':
    unittest.main()
