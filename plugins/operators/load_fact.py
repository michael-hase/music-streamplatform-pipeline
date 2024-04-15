"""Operator to load a fact table"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator loads and transforms data from Redshift staging table to fact table based on parameters provided.

    :param redshift_conn_id: Connection id of the Redshift connection to use
        Default is 'redshift'
    :type redshift_conn_id: str

    :param fact_table_name: Redshift fact table name where data will be inserted
    :type fact_table_name: str

    :param fact_insert_columns: Redshift fact table column names for table where data will be inserted
    :type fact_insert_columns: str

    :param fact_insert_sql: Query representing data that will be inserted
    :type fact_insert_sql: str

    :param truncate_table: If True, data will be truncated from fact table prior to inserting.
    :type truncate_table: bool

    """

    ui_color = '#F98866'

    TRUNCATE_FACT_SQL = """
        TRUNCATE TABLE {};
        """

    INSERT_FACT_SQL = """
        INSERT INTO {} ({}) {};
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 fact_table_name='',
                 fact_insert_columns='',
                 fact_insert_sql='',
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table_name = fact_table_name
        self.fact_insert_sql = fact_insert_sql
        self.fact_insert_columns = fact_insert_columns
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Truncating table {self.fact_table_name}")
            redshift_hook.run(self.TRUNCATE_FACT_SQL.format(self.fact_table_name))    
        redshift_hook.run(self.INSERT_FACT_SQL.format(
            self.fact_table_name, 
            self.fact_insert_columns,
            self.fact_insert_sql))
