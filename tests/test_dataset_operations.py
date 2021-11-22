from tests.context.resources import bq_client, project_id, dataset_id
from tests.context.operators import operator
from tests import utils
from tests.base_class import BaseClassTest


class DatasetOperations(BaseClassTest):

    def test_get_dataset(self):
        expected = dataset_id
        computed = operator.get_dataset().full_dataset_id
        self.assertEqual(expected, computed)

    def test_dataset_exists(self):
        self.assertTrue(operator.dataset_exists())
        utils.delete_dataset()
        self.assertFalse(operator.dataset_exists())
        utils.create_dataset()

    def test_create_dataset(self):
        bq_client.delete_dataset(dataset_id, not_found_ok=False)
        location = 'southamerica-east1'
        operator.create_dataset(location=location)
        self.assertTrue(utils.dataset_exists())
        self.assertEqual(location, utils.get_dataset().location)

    def test_delete_dataset(self):
        self.assertTrue(utils.dataset_exists())
        operator.delete_dataset()
        self.assertFalse(utils.dataset_exists())
        operator.delete_dataset()
        self.assertFalse(utils.dataset_exists())
        utils.create_dataset()
