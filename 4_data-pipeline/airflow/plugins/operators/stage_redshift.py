import os.path

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# TODO: The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The
#  operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should
#  specify where in S3 the file is loaded and what is the target table.
#       The parameters should be used to distinguish between JSON file. Another important requirement of the stage
#  operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time
#  and run backfills.
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key', 'iam_role')
    sql_template = """
        COPY {table}
        FROM '{path}'
        iam_role '{iam_role_arn}'
        json '{jsonpath}' truncatecolumns;
    """

    @apply_defaults
    def __init__(self,
                 table='',
                 s3_bucket='',
                 s3_key='',
                 iam_role='',
                 redshift_conn_id='redshift',
                 jsonpath='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.iam_role = iam_role
        self.redshift_conn_id = redshift_conn_id
        self.jsonpath = jsonpath

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = os.path.join('s3://', self.s3_bucket, self.s3_key.format(**context))
        arn = self.iam_role.format(**context)
        query = StageToRedshiftOperator.sql_template.format(table=self.table, path=s3_path,
                                                            iam_role=arn, jsonpath=self.jsonpath)
        self.log.info('Copying data to AWS Redshift...')
        redshift.run(query)
