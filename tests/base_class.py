import unittest
from tests.context.resources import bq_client, dataset_id


def delete_dataset():
    bq_client.delete_dataset(dataset_id, not_found_ok=False)


def create_dataset():
    bq_client.create_dataset(dataset_id, exists_ok=False)


class BaseClassTest(unittest.TestCase):
    def setUp(self):
        create_dataset()

    def tearDown(self):
        delete_dataset()
