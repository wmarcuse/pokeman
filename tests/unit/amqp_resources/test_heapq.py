import unittest
import os
import sqlite3

from pokeman.amqp_resources import heapq


class ResourceHeapQTests(unittest.TestCase):
    POKER_ID_MOCK = '4ff44fd6-1d08-49a3-a98b-27073871e85c'
    HEAPQ_ID_MOCK = '{PREFIX}{POKER_ID}'.format(
        PREFIX=heapq._HEAPQ_PREFIX,
        POKER_ID=POKER_ID_MOCK
    )

    def setUp(self):
        self.ResourceHeapQ = heapq.ResourceHeapQ

    def tearDown(self):
        os.remove(self.ResourceHeapQ._set_heapq_location(heapq_id=self.HEAPQ_ID_MOCK))

    def test_base_name(self):
        self.assertEqual(self.ResourceHeapQ.__name__, 'ResourceHeapQ')

    def test__set_heapq_id(self):
        poker_id = self.POKER_ID_MOCK
        desired_heapq_id = '{PREFIX}{POKER_ID}'.format(
            PREFIX=heapq._HEAPQ_PREFIX,
            POKER_ID=poker_id
        )
        heapq_id = self.ResourceHeapQ._set_heapq_id(
            poker_id=poker_id
        )
        self.assertEqual(heapq_id, desired_heapq_id)

    def test__set_heapq_location(self):
        heapq_location = self.ResourceHeapQ._set_heapq_location(heapq_id=self.HEAPQ_ID_MOCK)
        directory_path = heapq_location.split('heapq_')[0]
        self.assertTrue(os.path.isdir(directory_path))

    def test__set_heapq_environment_variable_default_heapq(self):
        default_heapq = self.ResourceHeapQ._set_heapq_environment_variable(self.HEAPQ_ID_MOCK)
        self.assertEqual(os.environ[heapq._POKEMAN_DEFAULT_HEAPQ_ENVIRON], self.HEAPQ_ID_MOCK)
        self.assertTrue(default_heapq)

    def test__set_heapq_environment_variable_non_default_heapq(self):
        """Depends on previous test"""
        default_heapq = self.ResourceHeapQ._set_heapq_environment_variable(self.HEAPQ_ID_MOCK)
        self.assertFalse(default_heapq)

    def test_heapq_connect(self):
        heapq_location = self.ResourceHeapQ._set_heapq_location(heapq_id=self.HEAPQ_ID_MOCK)
        heapq_connection = self.ResourceHeapQ._heapq_connect(heapq_location=heapq_location)
        self.assertTrue(isinstance(heapq_connection, sqlite3.Connection))

    def test_create_database(self):
        self.assertEqual(self.ResourceHeapQ.create_database(poker_id=self.POKER_ID_MOCK), None)


