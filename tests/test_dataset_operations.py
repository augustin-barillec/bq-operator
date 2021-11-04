from google.cloud import bigquery
from tests.context.resources import bq_client, dataset_id
from tests.context.operators import operator
from tests.base_class import BaseClassTest


class DatasetOperations(BaseClassTest):

    def test_instantiate_dataset(self):
        expected = bigquery.Dataset(dataset_id)
        computed = operator.instantiate_dataset()
        self.assertEqual(
            expected.project,
            computed.project)
        self.assertEqual(
            expected.dataset_id,
            computed.dataset_id)

    def test_get_dataset(self):

