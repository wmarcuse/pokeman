import unittest

from pokeman.utils import task


class TaskStatusTests(unittest.TestCase):
    def test_base_name(self):
        self.assertEqual(task.TaskStatus.__name__, 'TaskStatus')

    def test_enums(self):
        self.assertEqual(task.TaskStatus.PENDING.name, 'PENDING')
        self.assertEqual(task.TaskStatus.SUCCESS.name, 'SUCCESS')
        self.assertEqual(task.TaskStatus.ON_HOLD.name, 'ON_HOLD')
        self.assertEqual(task.TaskStatus.FAILED.name, 'FAILED')

if __name__ == '__main__':
    unittest.main()
