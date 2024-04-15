"""Operator to load a dimension table"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator loads and transforms data from Redshift staging table to dimension table based on parameters provided.

    :param redshift_conn_id: Connection id of the Redshift connection to use
        Default is 'redshift'
    :type redshift_conn_id: str

    :param dimension_table_name: Redshift dimension table name where data will be inserted
    :type dimension_table_name: str

    :param dimension_insert_columns: Redshift dimension table column names for table where data will be inserted
    :type dimension_insert_columns: str

    :param dimension_insert_sql: Query representing data that will be inserted
    :type dimension_insert_sql: str

    :param truncate_table: If True, data will be truncated from dimension table prior to inserting.
    :type truncate_table: bool

    """
    ui_color = '#80BD9E'

    TRUNCATE_DIMENSION_SQL = """
        TRUNCATE TABLE {};
        """

    INSERT_DIMENSION_SQL = """
        INSERT INTO {} ({}) {};
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 dimension_table_name='',
                 dimension_insert_columns='',
                 dimension_insert_sql='',
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dimension_table_name = dimension_table_name
        self.dimension_insert_sql = dimension_insert_sql
        self.dimension_insert_columns = dimension_insert_columns
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Truncating table {self.dimension_table_name}")
            redshift_hook.run(self.TRUNCATE_DIMENSION_SQL.format(self.dimension_table_name))    
        
        self.log.info(f"Inserting data into dimension table {self.dimension_table_name}")
        redshift_hook.run(self.INSERT_DIMENSION_SQL.format(
            self.dimension_table_name, 
            self.dimension_insert_columns,
            self.dimension_insert_sql))
