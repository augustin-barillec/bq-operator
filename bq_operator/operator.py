import logging
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(name=__name__)


class Operator:

    def __init__(self, bq_client, dataset_id):
        self._bq_client = bq_client
        self._dataset_id = dataset_id

    @property
    def bq_client(self):
        return self._bq_client

    @property
    def dataset_id(self):
        return self._dataset_id

    @staticmethod
    def log(msg):
        logger.debug(msg)

    def instantiate_dataset(self):
        return bigquery.Dataset(self._dataset_id)

    def get_dataset(self):
        return self._bq_client.get_dataset(self._dataset_id)

    def dataset_exists(self):
        try:
            self.get_dataset()
            return True
        except NotFound:
            return False

    def create_dataset(self, location=None):
        dataset = self.instantiate_dataset()
        dataset.location = location
        self.log(f'Trying to create dataset {self._dataset_id}')
        if self.dataset_exists():
            location = self.get_dataset().location
            msg = f'Dataset {self._dataset_id} ' \
                  f'already exists in location {location}'
            self.log(msg)
            return
        self._bq_client.create_dataset(dataset)
        location = self.get_dataset().location
        self.log(f'Created dataset {self._dataset_id} in location {location}')

    def delete_dataset(self):
        self.log(f'Trying to delete dataset {self._dataset_id}')
        if self.dataset_exists():
            self._bq_client.delete_dataset(self._dataset_id)
            self.log(f'Deleted dataset {self._dataset_id}')
        else:
            self.log(f'Dataset {self._dataset_id} not found')

    def build_table_id(self, table_name):
        return f'{self._dataset_id}.{table_name}'

    def instantiate_table(self, table_name):
        table_id = self.build_table_id(table_name)
        return bigquery.Table(table_id)

    def get_table(self, table_name):
        table_id = self.build_table_id(table_name)
        return self._bq_client.get_table(table_id)

    def table_exists(self, table_name):
        try:
            self.get_table(table_name)
            self.log(f'exists: {table_name}')
            return True
        except NotFound:
            self.log(f'not found: {table_name}')
            return False

    def get_attribute(self, table_name, attribute_name):
        table = self.get_table(table_name)
        return getattr(table, attribute_name)

    def get_schema(self, table_name):
        return self.get_attribute(table_name, 'schema')

    def get_time_partitioning(self, table_name):
        return self.get_attribute(table_name, 'time_partitioning')

    def get_range_partitioning(self, table_name):
        return self.get_attribute(table_name, 'range_partitioning')

    def get_require_partition_filter(self, table_name):
        return self.get_attribute(table_name, 'require_partition_filter')

    def get_clustering_fields(self, table_name):
        return self.get_attribute(table_name, 'clustering_fields')

    def get_format_attributes(self, table_name):
        n = table_name
        return {
            'schema': self.get_schema(n),
            'time_partitioning': self.get_time_partitioning(n),
            'range_partitioning': self.get_range_partitioning(n),
            'require_partition_filter': self.get_require_partition_filter(n),
            'clustering_fields': self.get_clustering_fields(n)}

    def get_col_names(self, table_name):
        schema = self.get_schema(table_name)
        return [f.name for f in schema]

    def get_num_rows(self, table_name):
        table = self.get_table(table_name)
        return table.num_rows

    def is_empty(self, table_name):
        num_rows = self.get_num_rows(table_name)
        return num_rows == 0

    def set_expiration_time(self, table_name, nb_days):
        table = self.get_table(table_name)
        expiration_time = (datetime.now(timezone.utc) + timedelta(days=nb_days))
        table.expires = expiration_time
        self._bq_client.update_table(table, ['expires'])

    def list_tables(self):
        tables = list(self._bq_client.list_tables(self._dataset_id))
        table_names = [t.table_id for t in tables]
        for table_name in table_names:
            self.log(f'listed: {table_name}')
        return table_names

    def delete_table(self, table_name):
        table_id = self.build_table_id(table_name)
        if self.table_exists(table_name):
            self._bq_client.delete_table(table_id)
            self.log(f'deleted: {table_name}')

    def delete_tables(self, table_names):
        for table_name in table_names:
            self.delete_table(table_name)

    def clean_dataset(self):
        self.log(f'Cleaning dataset {self._dataset_id}...')
        self.delete_tables(self.list_tables())
        self.log(f'Cleaned dataset {self._dataset_id}')

    def tables_same_format(self, left, right):
        res = True
        left_format_attributes = self.get_format_attributes(left)
        right_format_attributes = self.get_format_attributes(right)
        assert left_format_attributes.keys() == right_format_attributes.keys()
        for k in left_format_attributes:
            is_equal = (left_format_attributes[k] ==
                        right_format_attributes[k])
            if is_equal:
                msg = f'{k}: SAME'
            else:
                msg = f'{k}: DIFFERENT'
            self.log(msg)
            res = min(res, is_equal)
        return res

    def delete_if_mismatch(self, reference, to_compare):
        c1 = self.table_exists(table_name=reference)
        c2 = self.table_exists(table_name=to_compare)
        if not c1 or not c2:
            return
        c3 = self.tables_same_format(reference, to_compare)
        if c3:
            self.log('matching => doing nothing')
        else:
            self.log(f'mismatching => deleting {to_compare}')
            self.delete_table(
                table_name=to_compare)

    def create_empty_table(
            self,
            table_name,
            schema=None,
            time_partitioning=None,
            range_partitioning=None,
            require_partition_filter=None,
            clustering_fields=None):
        self.delete_table(table_name)
        table = self.instantiate_table(table_name)
        table.schema = schema
        table.time_partitioning = time_partitioning
        table.range_partitioning = range_partitioning
        table.require_partition_filter = require_partition_filter
        table.clustering_fields = clustering_fields
        self._bq_client.create_table(table)

    def _launch_copy_jobs(
            self, source_table_names, destination_table_names,
            write_disposition):
        nb_source_table_names = len(source_table_names)
        nb_destination_table_names = len(destination_table_names)
        assert nb_source_table_names == nb_destination_table_names
        assert nb_source_table_names >= 1
        jobs = []
        for s, d in zip(source_table_names, destination_table_names):
            s_id = self.build_table_id(s)
            d_id = self.build_table_id(d)
            job_config = bigquery.CopyJobConfig()
            job_config.write_disposition = write_disposition
            job = self._bq_client.copy_table(
                sources=s_id,
                destination=d_id,
                job_config=job_config)
            jobs.append(job)
        return jobs

    def _launch_sql_statement_jobs(self, sql_statements, job_configs):
        nb_sql_statements = len(sql_statements)
        if job_configs is None:
            job_configs = [None] * nb_sql_statements
        nb_job_configs = len(job_configs)
        assert nb_sql_statements == nb_job_configs
        assert nb_sql_statements >= 1
        jobs = []
        for sql_statement, job_config in zip(sql_statements, job_configs):
            job = self._bq_client.query(
                query=sql_statement, job_config=job_config)
            jobs.append(job)
        return jobs

    @staticmethod
    def wait_for_jobs(jobs):
        for job in jobs:
            job.result()

    def copy_tables(
            self,
            source_table_names,
            destination_table_names,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        jobs = self._launch_copy_jobs(
            source_table_names, destination_table_names, write_disposition)
        self.wait_for_jobs(jobs)

    def copy_table(
            self,
            source_table_name,
            destination_table_name,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        self.copy_tables(
            [source_table_name], [destination_table_name], write_disposition)

    @staticmethod
    def _zero_if_none(x):
        if x is None:
            return 0
        return x

    def run_sql_statements(self, sql_statements, job_configs=None):
        start_timestamp = datetime.now(timezone.utc)
        nb_sql_statements = len(sql_statements)
        plural = ''
        if nb_sql_statements > 1:
            plural = 's'
        self.log(f'Running sql statement{plural}...')
        jobs = self._launch_sql_statement_jobs(sql_statements, job_configs)
        self.wait_for_jobs(jobs)
        end_timestamp = datetime.now(timezone.utc)
        duration = (end_timestamp - start_timestamp).seconds
        total_bytes_billed_list = [
            self._zero_if_none(j.total_bytes_billed)
            for j in jobs]
        costs = [round(tbb / 10 ** 12 * 5, 5)
                 for tbb in total_bytes_billed_list]
        cost = sum(costs)
        monitoring = {'duration': duration, 'cost': cost}
        if nb_sql_statements > 1:
            monitoring['costs'] = costs
        self.log(
            f'Ran sql statement{plural} [{duration}s, {cost}$]')
        return monitoring

    def run_sql_statement(self, sql_statement, job_config=None):
        return self.run_sql_statements([sql_statement], [job_config])

    @staticmethod
    def sample_query(query, size=100000):
        return f"""
        select * from ({query}) 
        where rand() < {size}/(select count(*) from ({query}))
        """

    def run_queries(
            self,
            queries,
            destination_table_names,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        job_configs = []
        for d in destination_table_names:
            d_id = self.build_table_id(d)
            job_config = bigquery.QueryJobConfig()
            job_config.destination = d_id
            job_config.write_disposition = write_disposition
            job_configs.append(job_config)
        return self.run_sql_statements(queries, job_configs)

    def run_query(
            self,
            query,
            destination_table_name,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        return self.run_queries(
            [query], [destination_table_name], write_disposition)

    def create_view(self, query, destination_table_name):
        self.delete_table(destination_table_name)
        view = self.instantiate_table(destination_table_name)
        view.view_query = query
        self._bq_client.create_table(view)

    def create_views(self, queries, destination_table_names):
        for q, d in zip(queries, destination_table_names):
            self.create_view(q, d)
