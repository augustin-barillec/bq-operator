from google.cloud import bigquery
from tests.context.resources import bq_client, dataset_id
from tests.context.operators import operator
from tests.base_class import BaseClassTest


class DatasetOperations(BaseClassTest):

    def test_get_dataset(self):
        expected = bq_client.get_dataset(dataset_id)
        computed = operator.get_dataset()
        self.assertEqual(
            expected.full_dataset_id,
            computed.full_dataset_id)

    def test_dataset_exists(self):
        self.assertTrue(operator.dataset_exists())
        bq_client.delete_dataset(dataset_id, not_found_ok=False)
        self.assertFalse(operator.dataset_exists())
        bq_client.create_dataset(dataset_id, exists_ok=False)

