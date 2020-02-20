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

dag = DAG('test.staging',
          default_args=default_args,
          description='Load raw data to staging tables',
          schedule_interval='@daily')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/{{ execution_date.year }}/{{ execution_date.month }}/{{ ds }}-events.json',
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

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

(start_operator
 >> [stage_events_to_redshift, stage_songs_to_redshift]
 >> end_operator)

