from google.cloud import bigquery
from tests.context.resources import bq_client, dataset_id

table_names_default = tuple(f'a{i}' for i in range(10))


def wait_for_jobs(jobs):
    for job in jobs:
        job.result()


def populate(table_names=table_names_default):
    jobs = []
    for n in table_names:
        table_id = f'{dataset_id}.{n}'
        job_config = bigquery.QueryJobConfig()
        job_config.destination = table_id
        job = bq_client.query(
            query=f"select 'data_{n}' as x",
            job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs)
