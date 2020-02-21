from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# TODO: With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations.
#  Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and
#  target database on which to run the query against. You can also define a target table that will contain the results
#  of the transformation.
#       Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the
#  load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions.
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
                 INSERT INTO {table}
                 {select_query}
                 """
    truncate_sql = "TRUNCATE {table}"

    @apply_defaults
    def __init__(self,
                 select_query=None,
                 dimension_table=None,
                 redshift_conn_id='redshift',
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = dimension_table
        self.select_query = select_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info('Truncating dimension table...')
            redshift.run(LoadDimensionOperator.truncate_sql.format(table=self.table))
        query = LoadDimensionOperator.insert_sql.format(table=self.table, select_query=self.select_query)
        self.log.info('Loading dimension table...')
        redshift.run(query)
