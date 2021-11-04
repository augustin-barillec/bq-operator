import unittest
from tests.test_dataset_operations import DatasetOperations

suite = unittest.TestSuite()
suite.addTest(DatasetOperations('test_instantiate_dataset'))
unittest.TextTestRunner(verbosity=2).run(suite)
