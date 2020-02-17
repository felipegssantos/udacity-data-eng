from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    sql_template = """
        COPY {table}
        FROM {path}
        iam_role {iam_role}
        json {jsonpath} truncatecolumns;
    """

    @apply_defaults
    def __init__(self,
                 staging_table,
                 s3_path,
                 iam_role,
                 redshift_conn_id='redshift',
                 jsonpath='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = StageToRedshiftOperator.sql_template.format(table=staging_table, path=s3_path,
                                                                 iam_role=iam_role, jsonpath=jsonpath)

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Copying data to AWS Redshift...')
        redshift.run(self.query)
