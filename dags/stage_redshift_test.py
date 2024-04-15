"""DAG to test staging operator during development"""

import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import StageToRedshiftOperator

dag = DAG('stage_redshift_test',
          description='Test staging JSON to Redshift',
          #schedule_interval='@daily',
          #start_date=datetime.datetime(2018, 11, 1),
          #end_date = datetime.datetime(2018, 11, 30),
          schedule_interval = None,
          start_date=datetime.datetime(2018, 11, 1)
        )

stage_logdata_task = StageToRedshiftOperator(
    task_id="stage_logdata",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table_name='public.staging_events',
    s3_bucket='s3://udacity-dend/log_data',
    json_path = 's3://udacity-dend/log_json_path.json',
    use_paritioning = False,
    execution_date = '{{ execution_date }}',
    truncate_table=True
)

stage_songdata_task = StageToRedshiftOperator(
    task_id="stage_songdata",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table_name='public.staging_songs',
    s3_bucket='s3://udacity-dend/song_data',
    json_path = 'auto',
    use_paritioning = False,
    execution_date = '{{ execution_date }}',
    truncate_table=True
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_logdata_task
start_operator >> stage_songdata_task
stage_songdata_task >> end_operator
stage_logdata_task >> end_operator
