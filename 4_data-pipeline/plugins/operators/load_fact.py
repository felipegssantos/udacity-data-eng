from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# TODO: With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations.
#  Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and
#  target database on which to run the query against. You can also define a target table that will contain the results
#  of the transformation.
#       Fact tables are usually so massive that they should only allow append type functionality.
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 select_query='',
                 fact_table='facts',
                 fact_columns=None,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = f"""INSERT INTO {fact_table} ({", ".join(fact_columns)})
                         {select_query}"""

    def execute(self, context):
        self.log.info('Not implemented')
        # redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # self.log.info('Loading fact table...')
        # redshift.run(self.query)
