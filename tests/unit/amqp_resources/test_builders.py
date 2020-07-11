import unittest
from unittest import mock

from tests import TestAttributes as TA

from pokeman.amqp_resources import builders

EXCHANGE_BLUEPRINT = {
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

QUEUE_BLUEPRINT = {
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

#
# class ExchangeDestroyerTests(unittest.TestCase):
#     class ChannelMock:
#         def __init__(self, deletable=False, reply=None):
#             self.deletable = deletable
#             self.reply = reply
#
#         def exchange_delete(self, exchange):
#             from pika.exceptions import ChannelClosedByBroker
#             if self.deletable is False:
#                 return None
#             else:
#                 raise ChannelClosedByBroker(406, reply_text=self.reply)
#
#     def setUp(self):
#         self.blueprint = {
#     'type': 'Exchange',
#     'priority': 1,
#     'context': {
#         'exchange_name': 'pokeman_exchange',
#         'exchange_type': 'direct',
#         'durable': True,
#         'auto_delete': False,
#         'specific_poker': None
#     },
#     'action': 'CREATE'
# }
#         self.ExchangeDestroyer = builders.ExchangeDestroyer
#
#     def test_base_name(self):
#         self.assertEqual(self.ExchangeDestroyer.__name__, 'ExchangeDestroyer')
#
#     def test_delete_resource_deletable_true(self):
#         self.ExchangeDestroyer = self.ExchangeDestroyer(
#             channel=self.ChannelMock(
#                 deletable=True
#             ),
#             blueprint=self.blueprint
#         )
#         self.assertEqual(self.ExchangeDestroyer.delete_resource(), None)
#
#     def test_delete_resource_deletable_false_resource_does_not_exists(self):
#         from pika.exceptions import ChannelClosedByBroker
#         with self.assertRaisesRegex(ChannelClosedByBroker, 'NOT FOUND'):
#             # self.ExchangeDestroyer(
#             #     channel=self.ChannelMock(
#             #         deletable=False,
#             #         reply='... NOT FOUND ...'
#             #     ),
#             #     blueprint=self.blueprint
#             # )
#             gg = self.ExchangeDestroyer(channel=self.ChannelMock(
#                     deletable=False,
#                     reply='... NOT FOUND ...'
#                 ), blueprint=self.blueprint)
#             # setattr(self.ExchangeDestroyer, 'channel', self.ChannelMock(
#             #         deletable=False,
#             #         reply='... NOT FOUND ...'
#             #     ))
#             gg.delete_resource()


# class QueueDestroyerTests(unittest.TestCase):
#     class ChannelMock:
#         def __init__(self, deletable=False, reply=None):
#             self.deletable = deletable
#             self.reply = reply
#
#         def queue_delete(self, queue):
#             from pika.exceptions import ChannelClosedByBroker
#             if self.deletable is True:
#                 return None
#             else:
#                 raise ChannelClosedByBroker(406, reply_text=self.reply)
#
#     def setUp(self):
#         self.blueprint = {
#     'type': 'Queue',
#     'priority': 2,
#     'context': {
#         'auto_delete': False,
#         'durable': True,
#         'exchange': 'pokeman_exchange',
#         'exclusive': False,
#         'queue_name': 'pokeman_queue',
#         'routing_key': None,
#         'specific_poker': None
#     },
#     'action': 'CREATE'
# }
#
#         self.QueueDestroyer = builders.QueueDestroyer
#
#     def test_base_name(self):
#         self.assertEqual(self.QueueDestroyer.__name__, 'QueueDestroyer')
#
#     def test_delete_resource_deletable_true(self):
#         self.QueueDestroyer = self.QueueDestroyer(
#             channel=self.ChannelMock(
#                 deletable=True
#             ),
#             blueprint=self.blueprint
#         )
#         self.assertEqual(self.QueueDestroyer.delete_resource(), None)
#
#     def test_delete_resource_deletable_false_resource_does_not_exists(self):
#         from pika.exceptions import ChannelClosedByBroker
#         with self.assertRaisesRegex(ChannelClosedByBroker, 'NOT FOUND'):
#             self.QueueDestroyer = self.QueueDestroyer(
#                 channel=self.ChannelMock(
#                     deletable=False,
#                     reply='... NOT FOUND ...'
#                 ),
#                 blueprint=self.blueprint
#             ).delete_resource()
#


# class ResourceManagerTests(unittest.TestCase):
#
#     class ConnectionMock:
#         def __init__(self, resource, exists=False, scope='existence_check'):
#             self._exists = exists
#             self.scope = scope
#
#             class Some:
#                 @property
#                 def exists(self):
#                     if self._exists is True:
#                         return True
#                     else:
#                         return False
#
#             setattr(Some, '_exists', self._exists)
#             self.channel = Some
#             if resource == 'Exchange':
#                 setattr(self.channel, 'exchange_declare', ExchangeExistenceCheckerTests.ChannelMock.exchange_declare)
#             elif resource == 'Queue':
#                 self.channel.queue_declare = QueueExistenceCheckerTests.ChannelMock.queue_declare
#
#     def setUp(self):
#         self.exchange_blueprint = EXCHANGE_BLUEPRINT
#         self.queue_blueprint = QUEUE_BLUEPRINT
#         self.ExchangeResourceManagerExistsExistenceCheck = builders.ResourceManager(
#             connection=self.ConnectionMock(
#                 resource=self.exchange_blueprint['type'],
#                 exists=True,
#                 scope='existence_check'
#             )
#         )
#         self.ExchangeResourceManagerNotExistsExistenceCheck = builders.ResourceManager(
#             connection=self.ConnectionMock(
#                 resource=self.exchange_blueprint['type'],
#                 exists=False,
#                 scope='existence_check'
#             )
#         )
#         self.QueueResourceManager = builders.ResourceManager(
#             connection=self.ConnectionMock(
#                 resource=self.queue_blueprint['type']
#             )
#         )
#
#     def test_base_name(self):
#         self.assertEqual(self.ExchangeResourceManagerExistsExistenceCheck.__class__.__name__, 'ResourceManager')
#
#     def test_templates_mapping(self):
#         for resource, mapping in self.ExchangeResourceManagerExistsExistenceCheck._templates.items():
#             self.assertIn('builder', mapping.keys())
#             self.assertIn('checker', mapping.keys())
#             self.assertIn('destroyer', mapping.keys())
#
#     def test_templates_mapped_resources(self):
#         from pokeman import Exchange, Queue
#         self.assertIn(Exchange.__name__, self.QueueResourceManager._templates.keys())
#         self.assertIn(Queue.__name__, self.ExchangeResourceManagerExistsExistenceCheck._templates.keys())
#
#     def test_initialization(self):
#         self.assertEqual(self.ExchangeResourceManagerExistsExistenceCheck.builder, None)
#         self.assertTrue(isinstance(self.ExchangeResourceManagerExistsExistenceCheck._connection, self.ConnectionMock))
#         self.assertEqual(self.ExchangeResourceManagerExistsExistenceCheck.resource_exists, False)
#
#     # def test_pick_builder_exchange_success_resource_exists(self):
#     #     print(self.exchange_blueprint)
#     #     self.ExchangeResourceManagerExistsExistenceCheck.pick_builder(
#     #         blueprint=self.exchange_blueprint
#     #     )
#     #     self.assertEqual(self.ExchangeResourceManagerExistsExistenceCheck.resource_exists, True)
#     #
#     # # def test_pick_builder_exchange_success_resource_does_not_exists(self):
#     # #     self.ExchangeResourceManagerExistsExistenceCheck.pick_builder(
#     # #         blueprint=self.exchange_blueprint
#     # #     )
#     # #     self.assertEqual(self.ExchangeResourceManagerNotExistsExistenceCheck.resource_exists, False)
#     # #
