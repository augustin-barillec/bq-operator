from google.cloud import bigquery
from bq_operator.operator import Operator


class OperatorQuickSetup(Operator):

    def __init__(self, project_id, dataset_name, credentials=None):
        self._project_id = project_id
        self._dataset_name = dataset_name

        bq_client = bigquery.Client(
            project=self._project_id,
            credentials=credentials)

        dataset_id = f'{self._project_id}.{self._dataset_name}'

        super().__init__(bq_client, dataset_id)

    @property
    def project_id(self):
        """str: The project_id given in the argument."""
        return self._project_id

    @property
    def dataset_name(self):
        """str: The dataset_name given in the argument."""
        return self._dataset_name
