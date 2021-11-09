import unittest
from tests.test_dataset_operations import DatasetOperations

suite = unittest.TestSuite()
suite.addTest(DatasetOperations('test_dataset_exists'))
unittest.TextTestRunner(verbosity=2).run(suite)
