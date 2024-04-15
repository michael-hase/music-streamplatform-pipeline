"""DAG to test laod dimension and fact operators during development"""

import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadDimensionOperator, LoadFactOperator
from helpers import SqlQueries

dag = DAG('dimension_table_load_test',
          description='Test loading staging data to dimension tables',
          schedule_interval=None,
          start_date=datetime.datetime(2019,1,1)
        )


load_songplays_table = LoadFactOperator(
    task_id="load_songplays",
    dag=dag,
    redshift_conn_id = 'redshift',
    fact_insert_sql = SqlQueries.songplay_table_insert,
    fact_table_name = 'public.songplays',
    fact_insert_columns = 'playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent'
)

load_songs_table = LoadDimensionOperator(
    task_id="load_songs",
    dag=dag,
    redshift_conn_id = 'redshift',
    dimension_insert_sql = SqlQueries.song_table_insert,
    dimension_table_name = 'public.songs',
    dimension_insert_columns = 'songid, title, artistid, year, duration'
)

load_users_table = LoadDimensionOperator(
    task_id="load_users",
    dag=dag,
    redshift_conn_id = 'redshift',
    dimension_insert_sql = SqlQueries.user_table_insert,
    dimension_table_name = 'public.users',
    dimension_insert_columns = 'userid, first_name, last_name, gender, level'
)

load_time_table = LoadDimensionOperator(
    task_id="load_time",
    dag=dag,
    redshift_conn_id = 'redshift',
    dimension_table_name = 'public."time"',
    dimension_insert_sql = SqlQueries.time_table_insert,
    dimension_insert_columns = 'start_time, hour, day, week, month, year, weekday'
)

load_artists_table = LoadDimensionOperator(
    task_id="load_artists",
    dag=dag,
    redshift_conn_id = 'redshift',
    dimension_insert_sql = SqlQueries.artist_table_insert,
    dimension_table_name = 'public.artists',
    dimension_insert_columns = 'artistid, name, location, lattitude, longitude'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> load_songplays_table
load_songplays_table >> load_songs_table
load_songplays_table >> load_users_table
load_songplays_table >> load_artists_table
load_songplays_table >> load_time_table

load_songs_table >> end_operator
load_time_table >> end_operator
load_users_table >> end_operator
load_artists_table >> end_operator