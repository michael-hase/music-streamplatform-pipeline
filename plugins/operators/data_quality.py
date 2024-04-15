"""Performs data quality checks on data in Redshift"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    :param redshift_conn_id: Connection id of the Redshift connection to use
    :type redshift_conn_id: str

    :param sql_check_queries: List of SQL queries for data quality checks
    :type sql_check_queries: list

    :param expected_results: List of lambda expressions for predicates to validate data quality query results
        i.e. [lambda num_results: num_results > 0]
    :type expected_results: list

    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql_check_queries=[],
                 expected_results=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_check_queries = sql_check_queries
        self.expected_results = expected_results


    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i, query in enumerate(self.sql_check_queries):
            self.log.info(f"Executing data quality check {i}: {query}")
            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
               raise ValueError(f"Data quality check failed. {query} returned no results")
            num_records = records[0][0]
            if not self.expected_results[i](num_records):
               raise ValueError(f"Data quality check failed. {query} expected value did not match returned {num_records}")
            self.log.info(f"Data quality query {query} check passed with expected criteria")