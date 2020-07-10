import unittest
import abc

from pokeman.utils import custom_abc

class DummyAttributeTests(unittest.TestCase):
    def test_base_name(self):
        self.assertEqual(custom_abc.DummyAttribute.__name__, 'DummyAttribute')


class AbstractAttributeTests(unittest.TestCase):
    def test_abstract_attribute(self):
        self.assertIsInstance(custom_abc.abstract_attribute(), custom_abc.DummyAttribute)
        self.assertEqual(custom_abc.abstract_attribute().__is_abstract_attribute__, True)


class ABCMetaTests(unittest.TestCase):
    class AbstractClass(metaclass=custom_abc.ABCMeta):
        @custom_abc.abstract_attribute
        def test_attribute(self):
            pass

    class AbstractImplementation(AbstractClass):
        def __init__(self, attr=False):
            if attr is True:
                self.test_attribute = True
            else:
                pass

    def test_base_name(self):
        self.assertEqual(custom_abc.ABCMeta.__name__, 'ABCMeta')

    def test_equality(self):
        self.assertTrue(issubclass(custom_abc.ABCMeta, abc.ABCMeta))

    def test_metaclass_abstract_attribute_success(self):
        self.assertEqual(self.AbstractImplementation(attr=True).test_attribute, True)

    def test_metaclass_abstract_attribute_failure(self):
        with self.assertRaisesRegex(NotImplementedError, "Can't instantiate abstract class"):
            self.AbstractImplementation(attr=False)

if __name__ == '__main__':
    unittest.main()