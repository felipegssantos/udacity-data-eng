from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 1, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('project.pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/{{ execution_date.year }}/{{ execution_date.month }}/',
    iam_role='{{ var.value.redshift_iam_role }}',
    jsonpath='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A/',
    iam_role='{{ var.value.redshift_iam_role }}',
    jsonpath='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    select_query=SqlQueries.songplay_table_insert,
    fact_columns=['start_time', 'userid', 'level', 'songid', 'artistid', 'sessionid', 'location', 'user_agent']
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    select_query=SqlQueries.user_table_insert,
    dimension_columns=['userid', 'first_name', 'last_name', 'gender', 'level']
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    select_query=SqlQueries.song_table_insert,
    dimension_columns=['songid', 'title', 'artistid', 'year', 'duration']
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    select_query=SqlQueries.artist_table_insert,
    dimension_columns=['artistid', 'name', 'location', 'latitude', 'longitude']
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    select_query=SqlQueries.time_table_insert,
    dimension_columns=['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,

)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

(start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
 >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
 >> run_quality_checks >> end_operator)

