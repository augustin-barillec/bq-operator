from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class Operator:

    def __init__(self, client, dataset_id):
        self._client = client
        self._dataset_id = dataset_id
        self._dataset_name = self._dataset_id.split('.')[-1]

    @property
    def client(self):
        """google.cloud.bigquery.client.Client: The client."""
        return self._client

    @property
    def dataset_id(self):
        """str: The dataset id."""
        return self._dataset_id

    @property
    def dataset_name(self):
        """str: The dataset name."""
        return self._dataset_name

    @staticmethod
    def _wait_for_jobs(jobs):
        for job in jobs:
            job.result()

    @staticmethod
    def _zero_if_none(x):
        if x is None:
            return 0
        return x

    @staticmethod
    def sample_query(query, size):
        """Sample randomly a query."""
        return (f'select * from ({query}) '
                f'where rand() < {size}/(select count(*) from ({query}))')

    def _instantiate_dataset(self):
        return bigquery.Dataset(self._dataset_id)

    def get_dataset(self):
        """Get the dataset."""
        return self._client.get_dataset(self._dataset_id)

    def dataset_exists(self):
        """Return True if the dataset exists."""
        try:
            self.get_dataset()
            return True
        except NotFound:
            return False

    def create_dataset(self, location=None):
        """Create the dataset."""
        dataset = self._instantiate_dataset()
        dataset.location = location
        self._client.create_dataset(dataset=dataset, exists_ok=False)
        location = self.get_dataset().location
        return location

    def delete_dataset(self):
        """Delete the dataset."""
        self._client.delete_dataset(self._dataset_id, not_found_ok=True)

    def build_table_id(self, table_name, dataset_id=None):
        """Return the table id."""
        if dataset_id is None:
            dataset_id = self._dataset_id
        return f'{dataset_id}.{table_name}'

    def _instantiate_table(self, table_name):
        table_id = self.build_table_id(table_name)
        return bigquery.Table(table_id)

    def get_table(self, table_name):
        """Get a table."""
        table_id = self.build_table_id(table_name)
        return self._client.get_table(table_id)

    def table_exists(self, table_name):
        """Return True if the table exists."""
        try:
            self.get_table(table_name)
            return True
        except NotFound:
            return False

    def get_format_attributes(self, table_name):
        """Return the following table's attributes:
        schema, time_partitioning, range_partitioning,
        require_partition_filter, clustering_fields.
        """
        n = table_name
        res = dict()
        for a in ['schema', 'time_partitioning', 'range_partitioning',
                  'require_partition_filter', 'clustering_fields']:
            res[a] = getattr(self.get_table(n), a)
        return res

    def get_col_names(self, table_name):
        """Return the column names of a table."""
        schema = self.get_table(table_name).schema
        return [f.name for f in schema]

    def is_empty(self, table_name):
        """Return True if the table is empty."""
        num_rows = self.get_table(table_name).num_rows
        return num_rows == 0

    def set_time_to_live(self, table_name, nb_days):
        """Set the time to live of a table in days."""
        expiration_time = (datetime.now(timezone.utc) +
                           timedelta(days=nb_days))
        table = self.get_table(table_name)
        table.expires = expiration_time
        self._client.update_table(table, ['expires'])

    def list_tables(self):
        """List table names."""
        tables = list(self._client.list_tables(self._dataset_id))
        table_names = sorted([t.table_id for t in tables])
        return table_names

    def delete_table(self, table_name):
        """Delete a table."""
        table_id = self.build_table_id(table_name)
        self._client.delete_table(table_id, not_found_ok=True)

    def delete_tables(self, table_names):
        """Delete tables."""
        for table_name in table_names:
            self.delete_table(table_name)

    def clean_dataset(self):
        """Delete all the tables from the dataset."""
        self.delete_tables(self.list_tables())

    def tables_same_format(self, left, right):
        """Return True if the two tables have the same following attributes:
        schema, time_partitioning, range_partitioning,
        require_partition_filter and clustering_fields.
        """
        res = True
        left_format_attributes = self.get_format_attributes(left)
        right_format_attributes = self.get_format_attributes(right)
        assert left_format_attributes.keys() == right_format_attributes.keys()
        for k in left_format_attributes:
            is_equal = (left_format_attributes[k] ==
                        right_format_attributes[k])
            res = min(res, is_equal)
        return res

    def delete_table_if_mismatch(self, reference, table_name):
        """Delete the table (whose name is table_name) if
        :meth:`bq_operator.operator.Operator.tables_same_format`
        returns False when applied to reference and itself.
        """
        c1 = self.table_exists(table_name=reference)
        c2 = self.table_exists(table_name=table_name)
        if not c1 or not c2:
            return
        c3 = self.tables_same_format(reference, table_name)
        if not c3:
            self.delete_table(table_name=table_name)

    def delete_tables_if_mismatch(self, references, table_names):
        """Apply :meth:`bq_operator.operator.Operator.delete_table_if_mismatch`
        to each couple references[i], table_names[i].
        """
        len_references = len(references)
        len_table_names = len(table_names)
        assert len_references >= 1
        assert len_references == len_table_names
        for r, t in zip(references, table_names):
            self.delete_table_if_mismatch(r, t)

    def create_empty_table(
            self,
            table_name,
            schema=None,
            time_partitioning=None,
            range_partitioning=None,
            require_partition_filter=None,
            clustering_fields=None):
        """Create an empty table."""
        table = self._instantiate_table(table_name)
        table.schema = schema
        table.time_partitioning = time_partitioning
        table.range_partitioning = range_partitioning
        table.require_partition_filter = require_partition_filter
        table.clustering_fields = clustering_fields
        self._client.create_table(table, exists_ok=True)

    def create_view(self, query, destination_table_name):
        """Create a view."""
        self.delete_table(destination_table_name)
        view = self._instantiate_table(destination_table_name)
        view.view_query = query
        self._client.create_table(view)

    def create_views(self, queries, destination_table_names):
        """Create views."""
        len_queries = len(queries)
        len_destination_table_names = len(destination_table_names)
        assert len_queries >= 1
        assert len_queries == len_destination_table_names
        for q, d in zip(queries, destination_table_names):
            self.create_view(q, d)

    def _query_job(
            self,
            query,
            destination_table_name,
            write_disposition):
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.build_table_id(destination_table_name)
        job_config.write_disposition = write_disposition
        job = self._client.query(
            query=query, job_config=job_config)
        return job

    def _extract_job(
            self,
            source_table_name,
            destination_uri,
            field_delimiter):
        assert destination_uri.endswith('.csv.gz')
        source = self.build_table_id(source_table_name)
        job_config = bigquery.ExtractJobConfig()
        job_config.field_delimiter = field_delimiter
        job_config.destination_format = 'CSV'
        job_config.compression = 'GZIP'
        job = self._client.extract_table(
            source=source,
            destination_uris=destination_uri,
            job_config=job_config)
        return job

    def _load_job(
            self,
            source_uri,
            destination_table_name,
            schema,
            field_delimiter,
            write_disposition):
        destination = self.build_table_id(destination_table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.field_delimiter = field_delimiter
        if schema is None:
            job_config.autodetect = True
        else:
            job_config.schema = schema
            job_config.skip_leading_rows = 1
        job_config.write_disposition = write_disposition
        job = self._client.load_table_from_uri(
            source_uris=source_uri,
            destination=destination,
            job_config=job_config)
        return job

    def _copy_job(
            self,
            source_table_name,
            destination_table_name,
            source_dataset_id,
            write_disposition):
        source_table_id = self.build_table_id(
            source_table_name, dataset_id=source_dataset_id)
        destination_table_id = self.build_table_id(
            destination_table_name)
        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = write_disposition
        job = self._client.copy_table(
            sources=source_table_id,
            destination=destination_table_id,
            job_config=job_config)
        return job

    def _query_jobs(
            self,
            queries,
            destination_table_names,
            write_disposition):
        len_queries = len(queries)
        len_destination_table_names = len(destination_table_names)
        assert len_queries >= 1
        assert len_queries == len_destination_table_names
        return [self._query_job(q, d, write_disposition)
                for q, d in zip(queries, destination_table_names)]

    def _extract_jobs(
            self,
            source_table_names,
            destination_uris,
            field_delimiter):
        len_source_table_names = len(source_table_names)
        len_destination_uris = len(destination_uris)
        assert len_source_table_names >= 1
        assert len_source_table_names == len_destination_uris
        return [self._extract_job(s, d, field_delimiter)
                for s, d in zip(source_table_names, destination_uris)]

    def _load_jobs(
            self,
            source_uris,
            destination_table_names,
            schemas,
            field_delimiter,
            write_disposition):
        len_source_uris = len(source_uris)
        len_destination_table_names = len(destination_table_names)
        assert len_source_uris >= 1
        assert len_source_uris == len_destination_table_names
        return [
            self._load_job(s, d, sch, field_delimiter, write_disposition)
            for s, d, sch in
            zip(source_uris, destination_table_names, schemas)]

    def _copy_jobs(
            self,
            source_table_names,
            destination_table_names,
            source_dataset_id,
            write_disposition):
        len_source_table_names = len(source_table_names)
        len_destination_table_names = len(destination_table_names)
        assert len_source_table_names >= 1
        assert len_source_table_names == len_destination_table_names
        return [
            self._copy_job(s, d, source_dataset_id, write_disposition)
            for s, d in zip(source_table_names, destination_table_names)]

    def run_queries(
            self,
            queries,
            destination_table_names,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        """Run queries."""
        start_timestamp = datetime.now(timezone.utc)
        jobs = self._query_jobs(
            queries, destination_table_names, write_disposition)
        self._wait_for_jobs(jobs)
        end_timestamp = datetime.now(timezone.utc)
        duration = (end_timestamp - start_timestamp).total_seconds()
        total_bytes_billed_list = [
            self._zero_if_none(j.total_bytes_billed)
            for j in jobs]
        costs = [round(tbb / 10 ** 12 * 5, 5)
                 for tbb in total_bytes_billed_list]
        cost = sum(costs)
        monitoring = {'duration': duration, 'cost': cost}
        return monitoring

    def extract_tables(
            self,
            source_table_names,
            destination_uris,
            field_delimiter='|'):
        """Extract tables."""
        self._wait_for_jobs(self._extract_jobs(
            source_table_names, destination_uris, field_delimiter))

    def load_tables(
            self,
            source_uris,
            destination_table_names,
            schemas=None,
            field_delimiter='|',
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        """Load tables."""
        if schemas is None:
            schemas = [None]*len(source_uris)
        self._wait_for_jobs(self._load_jobs(
            source_uris, destination_table_names, schemas,
            field_delimiter, write_disposition))

    def copy_tables(
            self,
            source_table_names,
            destination_table_names,
            source_dataset_id=None,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        """Copy tables."""
        if source_dataset_id is None:
            source_dataset_id = self._dataset_id
        self._wait_for_jobs(self._copy_jobs(
            source_table_names, destination_table_names,
            source_dataset_id, write_disposition))

    def run_query(
            self,
            query,
            destination_table_name,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        """Run a query."""
        return self.run_queries(
            [query], [destination_table_name], write_disposition)

    def extract_table(
            self,
            source_table_name,
            destination_uri,
            field_delimiter='|'):
        """Extract a table."""
        self.extract_tables(
            [source_table_name], [destination_uri], field_delimiter)

    def load_table(
            self,
            source_uri,
            destination_table_name,
            schema=None,
            field_delimiter='|',
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        """Load a table."""
        self.load_tables(
            [source_uri], [destination_table_name], [schema],
            field_delimiter, write_disposition)

    def copy_table(
            self,
            source_table_name,
            destination_table_name,
            source_dataset_id=None,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        """Copy a table."""
        self.copy_tables(
            [source_table_name], [destination_table_name],
            source_dataset_id, write_disposition)
