import unittest
from tests.test_table_operations import TableOperations

suite = unittest.TestSuite()
suite.addTest(TableOperations('test_build_table_id'))
unittest.TextTestRunner(verbosity=2).run(suite)
