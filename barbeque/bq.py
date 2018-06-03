import sys, time
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

class BQClient:
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def create_table(self, dataset_name, table_name, schema, \
            partitioning_type, partitioning_field):
        dataset = self.client.dataset(dataset_name)
        table_ref = dataset.table(table_name)
        table = None
        try:
            table = self.client.get_table(table_ref)
        except NotFound:
            print("Table is not exist, continue creating table.")
        if table is not None:
            raise RuntimeError("Table is already exist")
        table = bigquery.Table(table_ref)
        table.schema = schema
        if partitioning_type:
            time_partitioning = bigquery.TimePartitioning(type_='DAY',\
                    field=partitioning_field)
            table.time_partitioning = time_partitioning
        result = self.client.create_table(table)        
        print(result)

    def query_and_load(self, sql, project_dest, dataset_dest, table_dest,timeout=30*60):
        client_dest = bigquery.Client(project=project_dest)
        dataset = client_dest.dataset(dataset_dest)
        dest_table_ref = dataset.table(table_dest)
        dest_table_ref.partitioning_type = 'DAY'

        conf = bigquery.QueryJobConfig()
        conf.use_legacy_sql = False
        conf.destination = dest_table_ref
        conf.write_disposition = 'WRITE_TRUNCATE'
        print('Executing sql statement to ' + project_dest + '.' +
              dataset_dest + '.' + table_dest + ': \n' + sql)
        start = time.time()
        query_job = self.client.query(sql, conf)
        iterator = query_job.result(timeout=timeout)
        end = time.time()
        print('Elapsed: ' + str(end - start) + ' secs')
        return iterator
