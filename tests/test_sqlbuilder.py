"""Unit test for sample file"""
import sys, os
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

import pytest
from datetime import date
from barbeque.sqlbuilder import JobConfig, JobParam, JobRuntime, SQLBuilder

def test_create_job_config():
    job_name = "sales_count_by_brand_id_day"
    job_type = "day_partition_preserving"
    source_table = "barbeque.sales"
    partition_field = "timestamp"
    keys = ["brand_id"]
    aggr = [{"field": "amount", "func": "sum"}]
    job_config = JobConfig(job_name, job_type, source_table, partition_field, keys, aggr)
    assert job_name == job_config.job_name
    assert job_type == job_config.job_type
    assert source_table == job_config.source_table
    assert partition_field == job_config.partition_field
    assert all([a == b for a, b in zip(keys, job_config.keys)])
    assert all([cmp(a,b) == 0 for a, b in zip(aggr, job_config.aggr)])

def test_create_job_param():
    start_dt = date(2018,06,17)
    end_dt = date(2018,06,21)
    job_param = JobParam(start_dt, end_dt)
    assert start_dt == job_param.start_dt
    assert end_dt == job_param.end_dt

def test_create_job_runtime():
    job_name = "sales_count_by_brand_id_day"
    job_type = "day_partition_preserving"
    source_table = "barbeque.sales"
    partition_field = "timestamp"
    keys = ["brand_id"]
    aggr = [{"field": "amount", "func": "sum"}]
    job_config = JobConfig(job_name, job_type, source_table, partition_field, keys, aggr)
    start_dt = date(2018,06,17)
    end_dt = date(2018,06,21)
    job_param = JobParam(start_dt, end_dt)
    job_runtime = JobRuntime(job_config, job_param)
    job_config_runtime = job_runtime.job_config
    job_param_runtime = job_runtime.job_param
    assert job_name == job_config_runtime.job_name
    assert job_type == job_config_runtime.job_type
    assert source_table == job_config_runtime.source_table
    assert all([a == b for a, b in zip(keys, job_config_runtime.keys)])
    assert all([cmp(a,b) == 0 for a, b in zip(aggr, job_config_runtime.aggr)])
    assert start_dt == job_param_runtime.start_dt
    assert end_dt == job_param_runtime.end_dt

def test_sqlbuilder():
    job_name = "sales_count_by_brand_id_day"
    job_type = "day_partition_preserving"
    source_table = "barbeque.sales"
    partition_field = "timestamp"
    keys = ["brand_id"]
    aggr = [{"field": "amount", "func": "sum"}]
    job_config = JobConfig(job_name, job_type, source_table, partition_field, keys, aggr)
    start_dt = date(2018,06,17)
    end_dt = date(2018,06,21)
    job_param = JobParam(start_dt, end_dt)
    job_runtime = JobRuntime(job_config, job_param)
    sql = SQLBuilder.build(job_runtime)
    assert sql == ('SELECT DATE(timestamp) AS day, brand_id, SUM(amount) AS amount_sum, COUNT(*) AS cnt '
            'FROM `barbeque.sales` WHERE timestamp >= "2018-06-17" AND timestamp < "2018-06-21" '
            'GROUP BY 1, 2')
