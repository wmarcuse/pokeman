import unittest

from tests import TestAttributes as TA

from pokeman.amqp_resources import globals


class AbstractExchangeTests(unittest.TestCase):
    def setUp(self):
        self.AbstractExchange = globals.AbstractExchange

    def test_base_name(self):
        self.assertEqual(self.AbstractExchange.__name__, 'AbstractExchange')

    def test_base_abstract_attributes(self):
        assert self.AbstractExchange.exchange_name
        self.AbstractExchange.exchange_name = None
        assert self.AbstractExchange.exchange_type
        self.AbstractExchange.exchange_type = None
        assert self.AbstractExchange.durable
        self.AbstractExchange.durable = None
        assert self.AbstractExchange.auto_delete
        self.AbstractExchange.auto_delete = None
        assert self.AbstractExchange.specific_poker
        self.AbstractExchange.specific_poker = None
        self.assertTrue(isinstance(self.AbstractExchange(), globals.AbstractExchange))


class AbstractRoutingKeyTests(unittest.TestCase):
    def setUp(self):
        self.AbstractRoutingKey = globals.AbstractRoutingKey

    def test_base_name(self):
        self.assertEqual(self.AbstractRoutingKey.__name__, 'AbstractRoutingKey')

    def test_base_abstract_attributes(self):
        assert self.AbstractRoutingKey.key
        self.AbstractRoutingKey.key = None
        self.assertTrue(isinstance(self.AbstractRoutingKey(), globals.AbstractRoutingKey))


class AbstractQueueTests(unittest.TestCase):
    def setUp(self):
        self.AbstractQueue = globals.AbstractQueue

    def test_base_name(self):
        self.assertEqual(self.AbstractQueue.__name__, 'AbstractQueue')

    def test_base_abstract_attributes(self):
        assert self.AbstractQueue.queue_name
        self.AbstractQueue.queue_name = None
        assert self.AbstractQueue.exchange
        self.AbstractQueue.exchange = None
        assert self.AbstractQueue.routing_key
        self.AbstractQueue.routing_key = None
        assert self.AbstractQueue.exclusive
        self.AbstractQueue.exclusive = None
        assert self.AbstractQueue.durable
        self.AbstractQueue.durable = None
        assert self.AbstractQueue.auto_delete
        self.AbstractQueue.auto_delete = None
        assert self.AbstractQueue.specific_poker
        self.AbstractQueue.specific_poker = None
        self.assertTrue(isinstance(self.AbstractQueue(), globals.AbstractQueue))


class ExchangeTests(unittest.TestCase):
    class PokemanMock:
        def __init__(self):
            self.poker_id = 'xyz'

    def setUp(self):
        from pokeman.amqp_resources.heapq import ResourceHeapQ
        with TA.patch(ResourceHeapQ, 'add_resource', lambda resource, specific_poker: None):
            self.exchange_set = globals.Exchange(
                exchange_name='test_name',
                exchange_type='test_type',
                durable=False,
                auto_delete=True,
                specific_poker=self.PokemanMock()
            )
            self.exchange_unset = globals.Exchange()

    def test_base_name(self):
        self.assertEqual(self.exchange_set.__class__.__name__, 'Exchange')

    def test__exchange_name_setter(self):
        self.assertEqual(self.exchange_set.exchange_name, 'test_name')
        self.assertEqual(self.exchange_unset.exchange_name, 'pokeman_exchange')

    def test__exchange_type_setter(self):
        self.assertEqual(self.exchange_set.exchange_type, 'test_type')
        self.assertEqual(self.exchange_unset.exchange_type, 'direct')

    def test__durable_setter(self):
        self.assertEqual(self.exchange_set.durable, False)
        self.assertEqual(self.exchange_unset.durable, True)

    def test__auto_delete_setter(self):
        self.assertEqual(self.exchange_set.auto_delete, True)
        self.assertEqual(self.exchange_unset.auto_delete, False)

    def test__specific_poker_setter(self):
        from pokeman import Queue
        self.assertEqual(self.exchange_set.specific_poker, 'xyz')
        self.assertEqual(self.exchange_unset.specific_poker, None)

class RoutingKeyTests(unittest.TestCase):
    def setUp(self):
        self.routingkey_set = globals.RoutingKey(key='xyz')
        self.routingkey_unset = globals.RoutingKey()

    def test_base_name(self):
        self.assertEqual(self.routingkey_set.__class__.__name__, 'RoutingKey')

    def test__routing_key_setter(self):
        self.assertEqual(self.routingkey_set.key, 'xyz')
        self.assertEqual(self.routingkey_unset.key, None)


class QueueTests(unittest.TestCase):
    class PokemanMock:
        def __init__(self):
            self.poker_id = 'xyz'

    class ExchangeMock:
        def __init__(self, state='set'):
            if state == 'set':
                self.exchange_name = 'mock_test_name'
            else:
                self.exchange_name = 'lol'

    class ExchangeConverterMock:
        def __init__(self, specific_poker):
            self.specific_poker = specific_poker
            self.exchange_name = 'converted_mock_test_name'

    class RoutingKeyMock:
        def __init__(self):
            self.key = 'mock_routing_key'

    class RoutingKeyConverterMock:
        def __init__(self):
            self.key = 'converted_mock_routing_key'

    def setUp(self):
        from pokeman.amqp_resources.heapq import ResourceHeapQ
        with TA.patch(ResourceHeapQ, 'add_resource', lambda resource, specific_poker: None):
            self.queue_set = globals.Queue(
                queue_name='test_name',
                exchange=self.ExchangeMock(state='set'),
                routing_key=self.RoutingKeyMock(),
                exclusive=True,
                durable=False,
                auto_delete=True,
                specific_poker=self.PokemanMock()
            )
            self.queue_unset = globals.Queue()

    def test_base_name(self):
        self.assertEqual(self.queue_set.__class__.__name__, 'Queue')

    def test__exchange_converter_lambda(self):
        with TA.patch(globals, 'Exchange', self.ExchangeConverterMock):
            self.assertEqual(self.queue_set._exchange_converter(), 'mock_test_name')
            self.assertEqual(self.queue_unset._exchange_converter(), 'converted_mock_test_name')

    def test__routing_key_converter_lambda(self):
        with TA.patch(globals, 'RoutingKey', self.RoutingKeyConverterMock):
            self.assertEqual(self.queue_set._routing_key_converter(), 'mock_routing_key')
            self.assertEqual(self.queue_unset._routing_key_converter(), 'converted_mock_routing_key')

    def test__queue_name_setter(self):
        self.assertEqual(self.queue_set.queue_name, 'test_name')
        self.assertEqual(self.queue_unset.queue_name, 'pokeman_queue')

    def test__exchange_setter(self):
        """
        Checked with above lambda test.
        """
        pass

    def test__routing_key_setter(self):
        """
        Checked with above lambda test.
        """
        pass

    def test__exclusive_setter(self):
        self.assertEqual(self.queue_set.exclusive, True)
        self.assertEqual(self.queue_unset.exclusive, False)

    def test__durable_setter(self):
        self.assertEqual(self.queue_set.durable, False)
        self.assertEqual(self.queue_unset.durable, True)

    def test__auto_delete_setter(self):
        self.assertEqual(self.queue_set.auto_delete, True)
        self.assertEqual(self.queue_unset.auto_delete, False)

    def test__specific_setter(self):
        self.assertEqual(self.queue_set.specific_poker, 'xyz')
        self.assertEqual(self.queue_unset.specific_poker, None)
        self.assertEqual(self.queue_set._specific_poker_setter(
            specific_poker=self.PokemanMock(),
            lambda_call=True
        ).__class__, self.PokemanMock().__class__
        )
        self.assertEqual(self.queue_set._specific_poker_setter(
            specific_poker=self.queue_set.specific_poker,
            lambda_call=True
        ), None
        )
        with self.assertRaisesRegex(ValueError, 'Unresolvable specific poker parameter'):
            self.queue_set._specific_poker_setter(
                specific_poker=list(),
                lambda_call=0
            )


if __name__ == '__main__':
    unittest.main()
