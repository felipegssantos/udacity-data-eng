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
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
    'depends_on_past': False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('test.dimensions',
          default_args=default_args,
          description='Load dimensions table',
          schedule_interval='@daily')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    select_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    select_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    select_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    select_query=SqlQueries.time_table_insert
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

(start_operator
 >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
 >> end_operator)

