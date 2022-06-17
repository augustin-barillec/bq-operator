from google.cloud import bigquery
from tests.utils import constants


def delete_dataset():
    constants.bq_client.delete_dataset(
        constants.dataset_id,
        delete_contents=False,
        not_found_ok=False)


def delete_bucket():
    constants.bucket.delete()
