from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from tests.context.resources import bq_client, dataset_id


def delete_dataset():
    bq_client.delete_dataset(dataset_id, not_found_ok=False)


def create_dataset():
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'EU'
    bq_client.create_dataset(dataset, exists_ok=False)


def get_dataset():
    return bq_client.get_dataset(dataset_id)


def dataset_exists():
    try:
        get_dataset()
        return True
    except NotFound:
        return False
