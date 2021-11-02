from google.cloud import bigquery
from tests.context.resource_names import PROJECT_ID, DATASET_NAME

project_id = PROJECT_ID
dataset_name = DATASET_NAME
dataset_id = f'{project_id}.{dataset_name}'
bq_client = bigquery.Client(project=project_id)
