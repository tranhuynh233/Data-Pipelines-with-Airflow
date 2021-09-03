from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get("AWS_KEY")
AWS_SECRET = os.environ.get("AWS_SECRET")

default_args = {
    "owner": "Tran",
    "start_date": datetime(2021, 4, 23),
    "depend_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "catchup": False,
    "email_on_retry":False
}

dag = DAG("sparkify_data_pipelines",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval="0 * * * *"
        )

start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    columns="(playid, start_time, userid, level, songid,\
     artistid, sessionid, location, user_agent)",
    sql=SqlQueries.songplay_table_insert,
    append_only=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    columns="(userid, first_name, last_name, gender, level)",
    append_only=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    columns="(songid, title, artistid, year, duration)",
    sql=SqlQueries.song_table_insert,
    append_only=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    columns="(artistid, name, location, lattitude, longitude)",
    sql=SqlQueries.artist_table_insert,
    append_only=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    columns="(start_time, hour, day, week, month, year, weekday)",
    sql=SqlQueries.time_table_insert,
    append_only=False
)

check_queries=[
    {
        "sql": "SELECT COUNT(*) FROM songplays;",
        "operator": ">",
        "value": 0
    },
    {
        "sql": "SELECT COUNT(*) FROM songplays WHERE playid IS NULL;",
        "operator": "==",
        "value": 0
    },
    {
    "sql": "SELECT COUNT(*) FROM songs WHERE songid IS NULL;",
    "operator": "==",
    "value": 0
    },
    {
    "sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL;",
    "operator": "==",
    "value": 0
    },
    {
    "sql": "SELECT COUNT(*) FROM artists WHERE artistid IS NULL;",
    "operator": "==",
    "value": 0
    },
    {
    "sql": "SELECT COUNT(*) FROM time WHERE start_time IS NULL;",
    "operator": "==",
    "value": 0
    }
]

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    check_queries=check_queries
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)

#Task dependencies

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
