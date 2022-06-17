from google.cloud import bigquery
from tests.utils import constants


def create_dataset():
    dataset = bigquery.Dataset(constants.dataset_id)
    dataset.location = constants.dataset_location
    constants.bq_client.create_dataset(dataset=dataset, exists_ok=False)


def create_bucket():
    constants.gs_client.create_bucket(
            bucket_or_name=constants.bucket_name,
            location=constants.bucket_location)
