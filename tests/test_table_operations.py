from google.cloud import bigquery
from tests.context.resources import bq_client, dataset_id
from tests.context.operators import operator
from tests import utils
from tests.populate import populate
from tests.base_class import BaseClassTest


class TableOperations(BaseClassTest):

    def test_build_table_id(self):
        table_name = 'table_name_1'
        expected = f'{dataset_id}.{table_name}'
        computed = operator.build_table_id(table_name)
        self.assertEqual(expected, computed)

    def test_get_table(self):
        populate()
        table_name = 'a0'
        expected = f'{dataset_id}.{table_name}'
        computed = operator.get_table(table_name).full_table_id
        self.assertEqual(expected, computed)

    def test_table_exists(self):
        populate()
        self.assertTrue(operator.table_exists('a0'))
        self.assertTrue(operator.table_exists('a1'))
        utils.delete_table('a0')
        self.assertFalse(operator.table_exists('a0'))
        self.assertTrue(operator.table_exists('a1'))

    def test_get_schema(self):
        populate()
        expected = [bigquery.SchemaField('data_a0', 'STRING')]
        computed = operator.get_schema('a0')
        self.assertEqual(expected, computed)
