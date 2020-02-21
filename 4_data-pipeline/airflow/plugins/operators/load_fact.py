from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    This operator copies data from staging tables to fact tables. It allows for append-only because fact
    tables are usually very massive.
    """

    ui_color = '#F98866'
    sql_template = """
                   INSERT INTO {table} ({columns})
                   {select_query}
                   """

    @apply_defaults
    def __init__(self,
                 select_query=None,
                 fact_table=None,
                 fact_columns=None,
                 redshift_conn_id='redshift',
                 *args, **kwargs):
        """

        :param select_query: a SELECT statement over staging tables
        :param fact_table: name of the fact table
        :param fact_columns: list of column names in the fact table
        :param redshift_conn_id: connection ID to the redshift instance
        :param args: optional positional arguments to airflow.operator.BaseOperator
        :param kwargs: optional keyword arguments to airflow.operator.BaseOperator
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = fact_table
        self.columns = ", ".join(fact_columns)
        self.select_query = select_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        query = LoadFactOperator.sql_template.format(table=self.table, columns=self.columns,
                                                     select_query=self.select_query)
        self.log.info('Loading fact table...')
        redshift.run(query)
