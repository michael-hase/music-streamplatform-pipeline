"""DAG to test data quality check operator during development"""

import datetime 
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime.datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'email_on_retry': False,
    'catchup_by_default': False
}

dag = DAG('data_quality_check_test',
          default_args = default_args,
          description='Test data quality check',
          schedule_interval=None,
        )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_check_queries=["SELECT COUNT(*) FROM songs WHERE songid IS NULL","SELECT COUNT(*) FROM songs"],
    expected_results=[lambda num_records: num_records==0, lambda num_records: num_records==0]
)
