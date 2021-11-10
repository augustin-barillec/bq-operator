import unittest
from tests import utils


class BaseClassTest(unittest.TestCase):
    def setUp(self):
        utils.create_dataset()

    def tearDown(self):
        utils.delete_dataset()
