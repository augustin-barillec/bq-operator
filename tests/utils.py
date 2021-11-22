from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from tests.context.resources import bq_client, dataset_id


def delete_dataset():
    bq_client.delete_dataset(
        dataset_id, not_found_ok=False, delete_contents=True)


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


def build_table_id(table_name):
    return f'{dataset_id}.{table_name}'


def get_table(table_name):
    table_id = build_table_id(table_name)
    return bq_client.get_table(table_id)


def delete_table(table_name):
    table_id = build_table_id(table_name)
    bq_client.delete_table(table_id, not_found_ok=False)


