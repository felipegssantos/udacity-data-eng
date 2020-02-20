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

dag = DAG('test.facts',
          default_args=default_args,
          description='Load facts table',
          schedule_interval='@daily')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    select_query=SqlQueries.songplay_table_insert,
    fact_columns=['start_time', 'userid', 'level', 'songid', 'artistid', 'sessionid', 'location', 'user_agent']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

(start_operator
 >> load_songplays_table
 >> end_operator)

