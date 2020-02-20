import os.path

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    The stage operator can load any JSON formatted files from S3 to Amazon Redshift.

    This operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters
    should specify where in S3 the file is loaded and what is the target table. It can, for example, use airflow macros
    like {{ ds }} and {{ execution_date }} to target specific data files. In fact, usage of these macros may be
    particularly useful to run backfills.
    """
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
        """

        :param table: name of the staging table where raw data should be loaded
        :param s3_bucket: name of the S3 bucket containing the raw data to be loaded
        :param s3_key: (templated field) the path to the raw data file(s) inside the S3 bucket
        :param iam_role: (templated field) a IAM ROLE ARN allowing redshift to read S3 data
        :param redshift_conn_id: connection ID to the redshift instance
        :param jsonpath: redshift's jsonpath option (defaults to 'auto')
        :param args: optional positional arguments to airflow.operator.BaseOperator
        :param kwargs: optional keyword arguments to airflow.operator.BaseOperator
        """

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
        self.log.info(f'Templated fields:\n'
                      f'    s3_path={s3_path}\n'
                      f'    ARN={arn}\n')
        query = StageToRedshiftOperator.sql_template.format(table=self.table, path=s3_path,
                                                            iam_role_arn=arn, jsonpath=self.jsonpath)
        self.log.info('Copying data to AWS Redshift...')
        redshift.run(query)
