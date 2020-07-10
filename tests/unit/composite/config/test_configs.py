import unittest

from pokeman import BasicConfig

class BasicConfigTests(unittest.TestCase):
    def setUp(self):
        self.basic_config = BasicConfig(
            connection_attempts=5,
            heartbeat=7200,
            retry_delay=2
        )

    def test_base_name(self):
        self.assertEqual(self.basic_config.__class__.__name__, 'BasicConfig')

    def test_base_initialization(self):
        self.assertEqual(self.basic_config.CONNECTION_ATTEMPTS, 5)
        self.assertEqual(self.basic_config.HEARTBEAT, 7200)
        self.assertEqual(self.basic_config.RETRY_DELAY, 2)

if __name__ == '__main__':
    unittest.main()
