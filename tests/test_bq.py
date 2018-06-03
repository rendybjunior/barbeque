"""Unit test for sample file"""
import sys, os
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

import pytest
from barbeque.bq import BQClient
from google.cloud import bigquery

def test_bq_query_and_load():
    project_id = "tvlk-dev"
    dataset_dest = "barbeque"
    table_dest = "result10"

    conf = bigquery.QueryJobConfig()
    conf.use_legacy_sql = False
    conf.use_query_cache = False
    
    bq_client = BQClient(project_id=project_id)
    sql = ('SELECT TIMESTAMP(DATE(timestamp)) AS timestamp_day, brand_id, SUM(amount) AS amount_sum, COUNT(*) AS cnt '
            'FROM `barbeque.sales` WHERE timestamp >= "2018-06-17" AND timestamp < "2018-06-21" '
            'GROUP BY 1, 2')
    iterator = bq_client.query_and_load(sql=sql, project_dest=project_id, \
            dataset_dest=dataset_dest, table_dest=table_dest)
    
    bq_client_dest = bigquery.Client(project=project_id)
    sql_dest = ('SELECT timestamp_day, brand_id, amount_sum, cnt FROM `{}.{}` '
            'WHERE timestamp_day >= "2018-06-17" AND timestamp_day < "2018-06-21"')\
                    .format(dataset_dest, table_dest)
    query_job_dest = bq_client_dest.query(sql_dest, conf)
    query_result = list(iterator)
    dest_content = list(query_job_dest.result(timeout=10*60))
    sorted_l1 = sorted(sorted(d.items()) for d in query_result)
    sorted_l2 = sorted(sorted(d.items()) for d in dest_content)
    assert sorted_l1 == sorted_l2
