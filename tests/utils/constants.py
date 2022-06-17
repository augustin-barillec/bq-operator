from google.cloud import bigquery
from google.cloud import storage


project_id = 'dmp-y-tests'
dataset_name = 'test_bq_operator'
dataset_id = f'{project_id}.{dataset_name}'
dataset_location = 'EU'
bucket_name = 'bucket_bq_operator'
bq_client = bigquery.Client(project=project_id)
gs_client = storage.Client(project=project_id)
bucket = gs_client.bucket(bucket_name)
bucket_location = 'EU'
credentials = None
field_delimiter = '|'
