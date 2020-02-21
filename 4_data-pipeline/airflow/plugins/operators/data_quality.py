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
    """
    This operator is responsible for performing quality checks on tables.

    It is flexible enough to allow for custom quality queries to be performed and checked against custom conditions.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=None,
                 test_query=None,
                 condition_fn=None,
                 redshift_conn_id='redshift',
                 *args, **kwargs):
        """

        :param tables: list of table names to check data quality
        :param test_query: list of queries to retrieve data for quality tests
        :param condition_fn: list of conditions to test each query result (must be the same size of `test_query`)
        :param redshift_conn_id: connection ID to the redshift instance
        :param args: optional positional arguments to airflow.operator.BaseOperator
        :param kwargs: optional keyword arguments to airflow.operator.BaseOperator
        """

        def _default_condition(records):
            return records[0][0] > 0

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = ['SELECT COUNT(*) FROM {table}'] if test_query is None else test_query
        self.condition_fn = [_default_condition] if condition_fn is None else condition_fn
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Checking data quality...')
        bad_tables = self.find_bad_tables(redshift)
        if bad_tables:
            error_details = '\n'.join([f'table "{table}" failed on queries: {queries}'
                                       for table, queries in bad_tables.items()])
            raise ValueError(f"Data quality check failed. Details: \n{error_details}")
        self.log.info("Data quality checks passed on all tables.")

    def find_bad_tables(self, redshift):
        """
        This method performs all quality tests against all tables and returns which table(s) failed on which test(s).

        :param redshift: hook to a redshift instance
        :return: dictionary with failing tables and keys and a list of the corresponding failing tests as values
        """
        bad_tables = dict()
        for table in self.tables:
            failing_tests = [query for query, condition_fn in zip(self.query, self.condition_fn)
                             if not self._check_table_quality(redshift, table, query, condition_fn)]
            if len(failing_tests) > 0:
                bad_tables[table] = failing_tests
        return bad_tables

    @staticmethod
    def _check_table_quality(redshift, table, query, condition_fn):
        records = redshift.get_records(query.format(table=table))
        return condition_fn(records)
