from google.cloud import bigquery
from bq_operator.operator import Operator


class OperatorQuickSetup(Operator):

    def __init__(self, project_id, dataset_name, credentials=None):
        self._project_id = project_id
        client = bigquery.Client(
            project=self._project_id,
            credentials=credentials)
        dataset_id = f'{self._project_id}.{dataset_name}'
        super().__init__(client, dataset_id)

    @property
    def project_id(self):
        """The project_id."""
        return self._project_id
