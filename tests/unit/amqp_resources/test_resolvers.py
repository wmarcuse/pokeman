import unittest
from unittest import mock

from tests import TestAttributes as TA

from pokeman.amqp_resources import resolvers

class ActionTests(unittest.TestCase):
    def setUp(self):
        self.Action = resolvers.Action

    def test_base_name(self):
        self.assertEqual(self.Action.__name__, 'Action')

    def test_enums(self):
        self.assertEqual(self.Action.CREATE.name, 'CREATE')
        self.assertEqual(self.Action.DELETE.name, 'DELETE')


class PriorityTests(unittest.TestCase):
    def setUp(self):
        self.Priority = resolvers.Priority

    def test_base_name(self):
        self.assertEqual(self.Priority.__name__, 'Priority')

    def test_flags(self):
        self.assertEqual(self.Priority.EXCHANGE.name, 'EXCHANGE')
        self.assertEqual(self.Priority.QUEUE.name, 'QUEUE')


class AbstractResourceResolverTests(unittest.TestCase):
    def test_base_name(self):
        self.assertEqual(resolvers.AbstractResourceResolver.__name__, 'AbstractResourceResolver')

    @mock.patch.object(resolvers.AbstractResourceResolver, '__abstractmethods__', set())
    def test_base_initialization(self):
        self.abc_resolver = resolvers.AbstractResourceResolver(
            resource=None,
            action=resolvers.Action.CREATE
        )
        blueprint_fields = ['type', 'priority', 'context', 'action']
        assert [field in self.abc_resolver.blueprint.keys() for field in blueprint_fields]


class ExchangeResolverTests(unittest.TestCase):
    def setUp(self):
        from pokeman.amqp_resources.heapq import ResourceHeapQ
        from pokeman.amqp_resources.globals import Exchange
        with TA.patch(ResourceHeapQ, 'add_resource', lambda resource, specific_poker: None):
            self.ExchangeResolver = resolvers.ExchangeResolver(
                resource=Exchange(),
                action=resolvers.Action.CREATE
            )

    def test_base_name(self):
        self.assertEqual(self.ExchangeResolver.__class__.__name__, 'ExchangeResolver')

    def test_resolver(self):
        from pokeman.amqp_resources.globals import Exchange
        context = {
            'exchange_name': 'pokeman_exchange',
            'exchange_type': 'direct',
            'durable': True,
            'auto_delete': False,
            'specific_poker': None
        }
        self.assertTrue(isinstance(self.ExchangeResolver.resource, Exchange))
        self.assertEqual(self.ExchangeResolver.blueprint['type'], 'Exchange')
        self.assertEqual(self.ExchangeResolver.blueprint['priority'], resolvers.Priority.EXCHANGE)
        self.assertEqual(self.ExchangeResolver.blueprint['action'], resolvers.Action.CREATE.name)
        self.assertEqual(self.ExchangeResolver.blueprint['context'], context)


class QueueResolverTests(unittest.TestCase):
    def setUp(self):
        from pokeman.amqp_resources.heapq import ResourceHeapQ
        from pokeman.amqp_resources.globals import Queue
        with TA.patch(ResourceHeapQ, 'add_resource', lambda resource, specific_poker: None):
            self.QueueResolver = resolvers.QueueResolver(
                resource=Queue(),
                action=resolvers.Action.CREATE
            )

    def test_base_name(self):
        self.assertEqual(self.QueueResolver.__class__.__name__, 'QueueResolver')

    def test_resolver(self):
        from pokeman.amqp_resources.globals import Queue
        context = {
            'queue_name': 'pokeman_queue',
            'exchange': 'pokeman_exchange',
            'routing_key': None,
            'exclusive': False,
            'durable': True,
            'auto_delete': False,
            'specific_poker': None
        }
        self.assertTrue(isinstance(self.QueueResolver.resource, Queue))
        self.assertEqual(self.QueueResolver.blueprint['type'], 'Queue')
        self.assertEqual(self.QueueResolver.blueprint['priority'], resolvers.Priority.QUEUE)
        self.assertEqual(self.QueueResolver.blueprint['action'], resolvers.Action.CREATE.name)
        self.assertEqual(self.QueueResolver.blueprint['context'], context)


class ResourceResolverTests(unittest.TestCase):
    class InvalidResourceMock:
        pass

    def setUp(self):
        self.ResourceResolver = resolvers.ResourceResolver()

    def test_base_name(self):
        self.assertEqual(self.ResourceResolver.__class__.__name__, 'ResourceResolver')

    def test_base_initialization(self):
        self.assertEqual(self.ResourceResolver._blueprint, None)

    def test__template_mapping(self):
        for resource, mapping in self.ResourceResolver._templates.items():
            assert resource.__name__ == mapping['blueprint'].__name__.replace('Resolver', '')

    def test_resolve(self):
        from pokeman.amqp_resources.heapq import ResourceHeapQ
        from pokeman.amqp_resources.globals import Queue
        with TA.patch(ResourceHeapQ, 'add_resource', lambda resource, specific_poker: None):
            blueprint = {
            'type': 'Queue',
            'priority': 2,
            'context': {
                'auto_delete': False,
                'durable': True,
                'exchange': 'pokeman_exchange',
                'exclusive': False,
                'queue_name': 'pokeman_queue',
                'routing_key': None,
                'specific_poker': None
            },
            'action': 'CREATE'
            }
            self.assertEqual(self.ResourceResolver.resolve(
                resource=Queue(),
                action=resolvers.Action.CREATE
            ), blueprint)

    def test_blueprint_valid_resource(self):
        from pokeman.amqp_resources.heapq import ResourceHeapQ
        from pokeman.amqp_resources.globals import Exchange
        with TA.patch(ResourceHeapQ, 'add_resource', lambda resource, specific_poker: None):
            blueprint = {
            'type': 'Exchange',
            'priority': 1,
            'context': {
                'exchange_name': 'pokeman_exchange',
                'exchange_type': 'direct',
                'durable': True,
                'auto_delete': False,
                'specific_poker': None
            },
            'action': 'CREATE'
            }
            self.assertEqual(self.ResourceResolver.blueprint(
                resource=Exchange(),
                action=resolvers.Action.CREATE
            ), blueprint)

    def test_blueprint_invalid_resource(self):
        with self.assertRaisesRegex(ValueError, 'No resource resolver implemented'):
            self.ResourceResolver.blueprint(
                resource=self.InvalidResourceMock(),
                action=resolvers.Action.CREATE
            )



if __name__ == '__main__':
    unittest.main()
