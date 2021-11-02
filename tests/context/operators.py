from bq_operator import Operator, OperatorQuickSetup
from tests.context.resources import project_id, dataset_name, dataset_id, \
    bq_client

operator = Operator(bq_client=bq_client, dataset_id=dataset_id)
operator_quick_setup = OperatorQuickSetup(
    project_id=project_id, dataset_name=dataset_name)
