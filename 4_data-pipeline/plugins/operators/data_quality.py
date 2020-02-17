from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# TODO: The operator's main functionality is to receive one or more SQL based test cases along with the expected results
#  and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no
#  match, the operator should raise an exception and the task should retry and fail eventually.
#       For example one test could be a SQL statement that checks if certain column contains NULL values by counting all
#  the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test
#  would compare the SQL statement's outcome to the expected result.
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 query='',
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        self.log.info('Not implemented')
        # redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # self.log.info('Checking data quality...')
        # redshift.run(self.query)
