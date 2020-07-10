import unittest
from unittest import mock

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
    pass