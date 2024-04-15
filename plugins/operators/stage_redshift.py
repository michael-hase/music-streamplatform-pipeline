"""Airflow Operator to load JSON data from S3 into Redshift staging table"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime
from dateutil import parser

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to load JSON data from Amazon S3 into Amazon Redshift

    :param redshift_conn_id: Connection id of the Redshift connection to use
        Default is 'redshift'
    :type redshift_conn_id: str
    
    :param aws_credentials_id: Connection id of the AWS credentials to use to access S3 data
    :type aws_credentials_id: str
        Default is 'aws_credentials'

    :param table_name: Redshift staging table name
    :type table_name: str

    :param s3_bucket: Amazon S3 bucket where staging data is read from
    :type s3_bucket: str

    :param json_path: Amazon S3 bucket JSON path
    :type json_path: str
        Default is 'auto'

    :param use_partitioning: If true, S3 data will be loaded as partitioned data based on year and month of execution_date
    :type use_partitioning: bool
        Default is 'False'

    :param execution_date: Logical execution date of DAG run (templated)
    :type execution_date: str
        
    """
    ui_color = '#358140'

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        """

    TRUNCATE_SQL = """
        TRUNCATE TABLE {};
        """
    
    template_fields = ['execution_date']

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 table_name='',
                 s3_bucket='',
                 json_path='auto',
                 use_partitioning=False,
                 execution_date="",
                 truncate_table=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.execution_date = execution_date
        self.use_partitioning = use_partitioning
        self.truncate_table = truncate_table

    def execute(self, context):
        execution_date = parser.parse(self.execution_date)
        self.log.info(f"Execution date: {execution_date}")
        self.log.info(f"Execution year: {execution_date.year}")
        self.log.info(f"Execution month: {execution_date.month}")
        self.log.info(f"Use Partitioning: {self.use_partitioning}")

        # If we are using partitioning, setup S3 path to use year and month of execution_date
        s3_path = self.s3_bucket+f'/{execution_date.year}/{execution_date.month}' if self.use_partitioning else self.s3_bucket
        self.log.info(f"S3 path: {s3_path}")
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if self.truncate_table:
            # Truncate table
            self.log.info(f"Truncating table {self.table_name}")
            redshift_hook.run(self.TRUNCATE_SQL.format(self.table_name))    
        
        sql_stmt = self.COPY_SQL.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )

        self.log.info(f"Copying data to staging table: {self.table_name}")
        redshift_hook.run(sql_stmt)
        self.log.info(f"Copying completed for table: {self.table_name}")





