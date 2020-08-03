import unittest
from unittest import mock

from tests import TestAttributes as TA

from pokeman.amqp_resources import builders


class AbstractResourceBuilderTests(unittest.TestCase):
    def test_base_name(self):
        self.assertEqual(builders.AbstractResourceBuilder.__name__, 'AbstractResourceBuilder')

    @mock.patch.object(builders.AbstractResourceBuilder, '__abstractmethods__', set())
    def test_base_initialization(self):
        self.abc_builder = builders.AbstractResourceBuilder()
        assert self.abc_builder.digest_blueprint
        assert self.abc_builder.create_resource
        self.assertIsInstance(self.abc_builder, builders.AbstractResourceBuilder)


class AbstractResourceDestroyerTests(unittest.TestCase):
    def test_base_name(self):
        self.assertEqual(builders.AbstractResourceDestroyer.__name__, 'AbstractResourceDestroyer')

    @mock.patch.object(builders.AbstractResourceDestroyer, '__abstractmethods__', set())
    def test_base_initialization(self):
        blueprint = {
            'context': None
        }
        self.abc_destroyer = builders.AbstractResourceDestroyer(
            channel=None,
            blueprint=blueprint
        )
        self.assertEqual(self.abc_destroyer.channel, None)
        self.assertEqual(self.abc_destroyer.blueprint, blueprint)
        self.assertEqual(self.abc_destroyer.blueprint_context, blueprint['context'])
        assert self.abc_destroyer.delete_resource
        self.assertIsInstance(self.abc_destroyer, builders.AbstractResourceDestroyer)


class AbstractExistenceChecker(unittest.TestCase):
    def test_base_name(self):
        self.assertEqual(builders.AbstractExistenceChecker.__name__, 'AbstractExistenceChecker')

    @mock.patch.object(builders.AbstractExistenceChecker, '__abstractmethods__', set())
    def test_base_initialization(self):
        blueprint = {
            'context': None
        }
        self.abc_existence_checker = builders.AbstractExistenceChecker(
            channel=None,
            blueprint=blueprint
        )
        self.assertEqual(self.abc_existence_checker.channel, None)
        self.assertEqual(self.abc_existence_checker.blueprint, blueprint)
        self.assertEqual(self.abc_existence_checker.blueprint_context, blueprint['context'])
        self.assertEqual(self.abc_existence_checker._exists, False)
        self.assertIsInstance(self.abc_existence_checker, builders.AbstractExistenceChecker)
        assert not self.abc_existence_checker.exists
        assert self.abc_existence_checker.check_existence

    @mock.patch.object(builders.AbstractExistenceChecker, '__abstractmethods__', set())
    def test_reset(self):
        blueprint = {
            'context': None
        }
        self.abc_existence_checker = builders.AbstractExistenceChecker(
            channel=None,
            blueprint=blueprint
        )
        self.abc_existence_checker._exists = True
        self.abc_existence_checker.reset()
        self.assertEqual(self.abc_existence_checker._exists, False)

    @mock.patch.object(builders.AbstractExistenceChecker, '__abstractmethods__', set())
    def test_exists(self):
        blueprint = {
            'context': None
        }
        self.abc_existence_checker = builders.AbstractExistenceChecker(
            channel=None,
            blueprint=blueprint
        )
        self.abc_existence_checker._exists = True
        self.assertEqual(self.abc_existence_checker.exists, True)
        self.assertEqual(self.abc_existence_checker._exists, False)


class ExchangeBuilderTests(unittest.TestCase):
    class ChannelMock:
        @classmethod
        def exchange_declare(cls, exchange, exchange_type, durable, auto_delete):
            pass

    def setUp(self):
        self.blueprint = {
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
        self.blueprint_context = self.blueprint['context']
        self.ExchangeBuilder = builders.ExchangeBuilder(
            channel=self.ChannelMock(),
            blueprint=self.blueprint
        )

    def test_base_name(self):
        self.assertEqual(self.ExchangeBuilder.__class__.__name__, 'ExchangeBuilder')

    def test_digest_blueprint_success(self):
        self.ExchangeBuilder.digest_blueprint()
        self.assertEqual(self.ExchangeBuilder.exchange_name, self.blueprint_context['exchange_name'])
        self.assertEqual(self.ExchangeBuilder.exchange_type, self.blueprint_context['exchange_type'])
        self.assertEqual(self.ExchangeBuilder.durable, self.blueprint_context['durable'])
        self.assertEqual(self.ExchangeBuilder.auto_delete, self.blueprint_context['auto_delete'])
        self.assertEqual(self.ExchangeBuilder.specific_poker, self.blueprint_context['specific_poker'])

    def test_digest_blueprint_failure(self):
        mock_blueprint = self.blueprint
        mock_blueprint['type'] = 'Undefined'
        with self.assertRaisesRegex(ValueError, 'received the wrong blueprint'):
            self.ExchangeBuilder.blueprint = mock_blueprint
            self.ExchangeBuilder.digest_blueprint()

    def test_reset(self):
        self.ExchangeBuilder.digest_blueprint()
        self.ExchangeBuilder.reset()
        attributes = [
            self.ExchangeBuilder.exchange_name,
            self.ExchangeBuilder.exchange_type,
            self.ExchangeBuilder.durable,
            self.ExchangeBuilder.auto_delete,
            self.ExchangeBuilder.specific_poker
        ]
        for attr in attributes:
            self.assertEqual(attr, None)

    def test_create_resource(self):
        self.assertEqual(self.ExchangeBuilder.create_resource(), None)


class QueueBuilderTests(unittest.TestCase):
    class ChannelMock:
        @classmethod
        def queue_declare(cls, queue, durable, exclusive, auto_delete):
            pass

        @classmethod
        def queue_bind(cls, queue, exchange, routing_key):
            pass

    def setUp(self):
        self.blueprint = {
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
        self.blueprint_context = self.blueprint['context']
        self.QueueBuilder = builders.QueueBuilder(
            channel=self.ChannelMock(),
            blueprint=self.blueprint
        )

    def test_base_name(self):
        self.assertEqual(self.QueueBuilder.__class__.__name__, 'QueueBuilder')

    def test_digest_blueprint_success(self):
        self.QueueBuilder.digest_blueprint()
        self.assertEqual(self.QueueBuilder.queue_name, self.blueprint_context['queue_name'])
        self.assertEqual(self.QueueBuilder.exchange, self.blueprint_context['exchange'])
        self.assertEqual(self.QueueBuilder.routing_key, self.blueprint_context['routing_key'])
        self.assertEqual(self.QueueBuilder.exclusive, self.blueprint_context['exclusive'])
        self.assertEqual(self.QueueBuilder.durable, self.blueprint_context['durable'])
        self.assertEqual(self.QueueBuilder.auto_delete, self.blueprint_context['auto_delete'])
        self.assertEqual(self.QueueBuilder.specific_poker, self.blueprint_context['specific_poker'])

    def test_digest_blueprint_failure(self):
        mock_blueprint = self.blueprint
        mock_blueprint['type'] = 'Undefined'
        with self.assertRaisesRegex(ValueError, 'received the wrong blueprint'):
            self.QueueBuilder.blueprint = mock_blueprint
            self.QueueBuilder.digest_blueprint()

    def test_reset(self):
        self.QueueBuilder.digest_blueprint()
        self.QueueBuilder.reset()
        attributes = [
            self.QueueBuilder.queue_name,
            self.QueueBuilder.exchange,
            self.QueueBuilder.routing_key,
            self.QueueBuilder.exclusive,
            self.QueueBuilder.durable,
            self.QueueBuilder.auto_delete,
            self.QueueBuilder.specific_poker
        ]
        for attr in attributes:
            self.assertEqual(attr, None)

    def test_create_resource(self):
        self.assertEqual(self.QueueBuilder.create_resource(), None)


class ExchangeExistenceCheckerTests(unittest.TestCase):
    class ChannelMock:
        def __init__(self, exists=False):
            self.exists = exists

        def exchange_declare(self, exchange, exchange_type, passive):
            from pika.exceptions import ChannelClosedByBroker
            if self.exists is True:
                return None
            else:
                raise ChannelClosedByBroker(406, reply_text='... NOT FOUND ...')

    def setUp(self):
        self.blueprint = {
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
        self.ExchangeExistenceChecker = builders.ExchangeExistenceChecker

    def test_base_name(self):
        self.assertEqual(self.ExchangeExistenceChecker.__name__, 'ExchangeExistenceChecker')

    def test_check_existence_true(self):
        self.ExchangeExistenceChecker = self.ExchangeExistenceChecker(
            channel=self.ChannelMock(
                exists=True
            ),
            blueprint=self.blueprint
        )
        self.ExchangeExistenceChecker.check_existence()
        self.assertEqual(self.ExchangeExistenceChecker._exists, True)

    def test_check_existence_false(self):
        self.ExchangeExistenceChecker = self.ExchangeExistenceChecker(
            channel=self.ChannelMock(
                exists=False
            ),
            blueprint=self.blueprint
        )
        self.ExchangeExistenceChecker.check_existence()
        self.assertEqual(self.ExchangeExistenceChecker._exists, False)


class QueueExistenceCheckerTests(unittest.TestCase):
    class ChannelMock:
        def __init__(self, exists=False):
            self.exists = exists

        def queue_declare(self, queue, passive):
            from pika.exceptions import ChannelClosedByBroker
            if self.exists is True:
                return None
            else:
                raise ChannelClosedByBroker(406, reply_text='... NOT FOUND ...')

    def setUp(self):
        self.blueprint = {
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
        self.QueueExistenceChecker = builders.QueueExistenceChecker

    def test_base_name(self):
        self.assertEqual(self.QueueExistenceChecker.__name__, 'QueueExistenceChecker')

    def test_check_existence_true(self):
        self.QueueExistenceChecker = self.QueueExistenceChecker(
            channel=self.ChannelMock(
                exists=True
            ),
            blueprint=self.blueprint
        )
        self.QueueExistenceChecker.check_existence()
        self.assertEqual(self.QueueExistenceChecker._exists, True)

    def test_check_existence_false(self):
        self.QueueExistenceChecker = self.QueueExistenceChecker(
            channel=self.ChannelMock(
                exists=False
            ),
            blueprint=self.blueprint
        )
        self.QueueExistenceChecker.check_existence()
        self.assertEqual(self.QueueExistenceChecker._exists, False)


class ExchangeDestroyerTests(unittest.TestCase):
    class ChannelMock:
        def __init__(self, deletable=False, reply=None):
            self.deletable = deletable
            self.reply = reply

        def exchange_delete(self, exchange):
            from pika.exceptions import ChannelClosedByBroker
            if self.deletable is True:
                return None
            else:
                raise ChannelClosedByBroker(406, reply_text=self.reply)

    def setUp(self):
        self.blueprint = {
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
        self.ExchangeDestroyer = builders.ExchangeDestroyer

    def test_base_name(self):
        self.assertEqual(self.ExchangeDestroyer.__name__, 'ExchangeDestroyer')

    def test_delete_resource_deletable_true(self):
        self.ExchangeDestroyer = self.ExchangeDestroyer(
            channel=self.ChannelMock(
                deletable=True
            ),
            blueprint=self.blueprint
        )
        self.assertEqual(self.ExchangeDestroyer.delete_resource(), None)

    def test_delete_resource_deletable_false_resource_does_not_exists(self):
        self.ExchangeDestroyer = self.ExchangeDestroyer(channel=self.ChannelMock(
                deletable=False,
                reply='... NOT_FOUND ...'
            ), blueprint=self.blueprint)
        self.ExchangeDestroyer.delete_resource()
        self.assertEqual(self.ExchangeDestroyer.delete_resource(), None)


class QueueDestroyerTests(unittest.TestCase):
    class ChannelMock:
        def __init__(self, deletable=False, reply=None):
            self.deletable = deletable
            self.reply = reply

        def queue_delete(self, queue):
            from pika.exceptions import ChannelClosedByBroker
            if self.deletable is True:
                return None
            else:
                raise ChannelClosedByBroker(406, reply_text=self.reply)

    def setUp(self):
        self.blueprint = {
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

        self.QueueDestroyer = builders.QueueDestroyer

    def test_base_name(self):
        self.assertEqual(self.QueueDestroyer.__name__, 'QueueDestroyer')

    def test_delete_resource_deletable_true(self):
        self.QueueDestroyer = self.QueueDestroyer(
            channel=self.ChannelMock(
                deletable=True
            ),
            blueprint=self.blueprint
        )
        self.assertEqual(self.QueueDestroyer.delete_resource(), None)

    def test_delete_resource_deletable_false_resource_does_not_exists(self):
        self.QueueDestroyer = self.QueueDestroyer(channel=self.ChannelMock(
                deletable=False,
                reply='... NOT_FOUND ...'
            ), blueprint=self.blueprint)
        self.QueueDestroyer.delete_resource()
        self.assertEqual(self.QueueDestroyer.delete_resource(), None)


class ResourceManagerTests(unittest.TestCase):

    class ConnectionMock:
        def __init__(self, resource, exists=False, scope='existence_check'):
            self._exists = exists
            self.scope = scope

            class Sub:
                @property
                def exists(self):
                    if self._exists is True:
                        return True
                    else:
                        return False

            setattr(Sub, '_exists', self._exists)
            self.channel = Sub
            if resource == 'Exchange':
                setattr(self.channel, 'exchange_declare', ExchangeExistenceCheckerTests.ChannelMock.exchange_declare)
            elif resource == 'Queue':
                self.channel.queue_declare = QueueExistenceCheckerTests.ChannelMock.queue_declare

    class ResourceHeapQMock:
        _HEAPQ = [
            (
                1,
                {
                    "type": "Exchange",
                    "priority": 1,
                    "context": {
                        "exchange_name": "pokeman_exchange",
                        "exchange_type": "direct",
                        "durable": True,
                        "auto_delete": False,
                        "specific_poker": "4a06075e-25b0-49e8-b000-7feca505efb5"
                    },
                    "action": "CREATE"
                }
            ),
            (
                2,
                {
                    "type": "Queue",
                    "priority": 2,
                    "context": {
                        "queue_name": "pokeman_queue",
                        "exchange": "pokeman_exchange",
                        "routing_key": None,
                        "exclusive": False,
                        "durable": False,
                        "auto_delete": False,
                        "specific_poker": None
                    },
                    "action": "CREATE"
                }
            )
        ]

        @classmethod
        def get_heapq(cls, poker_id, reverse):
            import json
            from queue import PriorityQueue
            resource_heapq = PriorityQueue()
            for _resource in cls._HEAPQ:
                resource_heapq.put((_resource[0], json.dumps(_resource[1])))
            return resource_heapq

    def setUp(self):
        self.exchange_blueprint = {
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
        self.queue_blueprint = {
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
        self.ExchangeResourceManagerResourceExists = builders.ResourceManager(
            connection=self.ConnectionMock(
                resource=self.exchange_blueprint['type'],
                exists=True,
                scope='existence_check'
            )
        )
        self.ExchangeResourceManagerResourceNotExists = builders.ResourceManager(
            connection=self.ConnectionMock(
                resource=self.exchange_blueprint['type'],
                exists=False,
                scope='existence_check'
            )
        )
        self.QueueResourceManagerResourceExists = builders.ResourceManager(
            connection=self.ConnectionMock(
                resource=self.queue_blueprint['type'],
                exists=True,
                scope='existence_check'
            )
        )
        self.QueueResourceManagerResourceNotExists = builders.ResourceManager(
            connection=self.ConnectionMock(
                resource=self.queue_blueprint['type'],
                exists=False,
                scope='existence_check'
            )
        )

    def test_base_name(self):
        self.assertEqual(self.ExchangeResourceManagerResourceExists.__class__.__name__, 'ResourceManager')

    def test_templates_mapping(self):
        for resource, mapping in self.ExchangeResourceManagerResourceExists._templates.items():
            self.assertIn('builder', mapping.keys())
            self.assertIn('checker', mapping.keys())
            self.assertIn('destroyer', mapping.keys())

    def test_templates_mapped_resources(self):
        from pokeman import Exchange, Queue
        self.assertIn(Exchange.__name__, self.QueueResourceManagerResourceExists._templates.keys())
        self.assertIn(Queue.__name__, self.ExchangeResourceManagerResourceExists._templates.keys())

    def test_initialization(self):
        self.assertEqual(self.ExchangeResourceManagerResourceExists.builder, None)
        self.assertTrue(isinstance(self.ExchangeResourceManagerResourceExists._connection, self.ConnectionMock))
        self.assertEqual(self.ExchangeResourceManagerResourceExists.resource_exists, False)

    def test_pick_builder_failure_already_assigned(self):
        self.ExchangeResourceManagerResourceExists.builder = 'xyz'
        with self.assertRaisesRegex(SyntaxError, 'already picked a builder'):
            self.ExchangeResourceManagerResourceExists.pick_builder(
                blueprint=self.exchange_blueprint
            )

    def test_pick_builder_failure_invalid_resource_type(self):
        self.exchange_blueprint['type'] = 'Invalid'
        with self.assertRaisesRegex(ValueError, 'The provided resource is not valid'):
            self.ExchangeResourceManagerResourceExists.pick_builder(
                blueprint=self.exchange_blueprint
            )

    def test_pick_builder_exchange_success_resource_exists(self):
        self.ExchangeResourceManagerResourceExists.pick_builder(
            blueprint=self.exchange_blueprint
        )
        self.assertEqual(self.ExchangeResourceManagerResourceExists.resource_exists, True)
        self.assertEqual(self.ExchangeResourceManagerResourceExists.builder, None)

    def test_pick_builder_exchange_success_resource_not_exists(self):
        from pokeman.amqp_resources.builders import ExchangeBuilder
        self.ExchangeResourceManagerResourceNotExists.pick_builder(
            blueprint=self.exchange_blueprint
        )
        self.assertEqual(self.ExchangeResourceManagerResourceNotExists.resource_exists, False)
        self.assertTrue(isinstance(self.ExchangeResourceManagerResourceNotExists.builder, ExchangeBuilder))

    def test_pick_builder_queue_success_resource_exists(self):
        self.QueueResourceManagerResourceExists.pick_builder(
            blueprint=self.queue_blueprint
        )
        self.assertEqual(self.QueueResourceManagerResourceExists.resource_exists, True)
        self.assertEqual(self.QueueResourceManagerResourceExists.builder, None)

    def test_pick_builder_queue_success_resource_not_exists(self):
        from pokeman.amqp_resources.builders import QueueBuilder
        self.QueueResourceManagerResourceNotExists.pick_builder(
            blueprint=self.queue_blueprint
        )
        self.assertEqual(self.QueueResourceManagerResourceNotExists.resource_exists, False)
        self.assertTrue(isinstance(self.QueueResourceManagerResourceNotExists.builder, QueueBuilder))

    def test_create_exchange_resource_resource_exists(self):
        exchange_test_builder_object = ExchangeBuilderTests()
        exchange_test_builder_object.setUp()
        exchange_builder = exchange_test_builder_object.ExchangeBuilder
        self.ExchangeResourceManagerResourceExists.builder = exchange_builder
        self.ExchangeResourceManagerResourceExists.resource_exists = True
        self.assertEqual(self.ExchangeResourceManagerResourceExists.resource_exists, True)
        self.ExchangeResourceManagerResourceExists.create_resource()
        self.assertEqual(self.ExchangeResourceManagerResourceExists.builder, None)

    def test_create_exchange_resource_resource_not_exists(self):
        exchange_test_builder_object = ExchangeBuilderTests()
        exchange_test_builder_object.setUp()
        exchange_builder = exchange_test_builder_object.ExchangeBuilder
        self.ExchangeResourceManagerResourceNotExists.builder = exchange_builder
        self.assertEqual(self.ExchangeResourceManagerResourceNotExists.resource_exists, False)
        self.ExchangeResourceManagerResourceNotExists.create_resource()
        self.assertEqual(self.ExchangeResourceManagerResourceNotExists.builder, None)

    def test_create_queue_resource_resource_exists(self):
        queue_test_builder_object = QueueBuilderTests()
        queue_test_builder_object.setUp()
        queue_builder = queue_test_builder_object.QueueBuilder
        self.QueueResourceManagerResourceExists.builder = queue_builder
        self.QueueResourceManagerResourceExists.resource_exists = True
        self.assertEqual(self.QueueResourceManagerResourceExists.resource_exists, True)
        self.QueueResourceManagerResourceExists.create_resource()
        self.assertEqual(self.QueueResourceManagerResourceExists.builder, None)

    def test_create_queue_resource_resource_not_exists(self):
        queue_test_builder_object = QueueBuilderTests()
        queue_test_builder_object.setUp()
        queue_builder = queue_test_builder_object.QueueBuilder
        self.QueueResourceManagerResourceNotExists.builder = queue_builder
        self.assertEqual(self.QueueResourceManagerResourceNotExists.resource_exists, False)
        self.QueueResourceManagerResourceNotExists.create_resource()
        self.assertEqual(self.QueueResourceManagerResourceNotExists.builder, None)

    def test_destroy_builder(self):
        self.ExchangeResourceManagerResourceExists.builder = 'xyz'
        self.ExchangeResourceManagerResourceExists.resource_exists = True
        self.ExchangeResourceManagerResourceExists.destroy_builder()
        self.assertEqual(self.ExchangeResourceManagerResourceExists.builder, None)
        self.assertEqual(self.ExchangeResourceManagerResourceExists.resource_exists, False)

    def test_delete_attached_resources(self):
        from pokeman.amqp_resources import heapq
        template_mock = {
            'Exchange': {
                'destroyer': lambda channel, blueprint: None
            },
            'Queue': {
                'destroyer': lambda channel, blueprint: None
            }
        }
        with TA.patch(heapq, 'ResourceHeapQ', self.ResourceHeapQMock), \
             TA.patch(builders.ResourceManager, '_templates', template_mock):

            self.assertEqual(self.ExchangeResourceManagerResourceExists.delete_attached_resources(
                poker_id='xyz'
            ), None)
