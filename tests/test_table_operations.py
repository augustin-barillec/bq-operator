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

    def get_table(self):
        populate()


