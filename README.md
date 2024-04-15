# Music Platform Data Pipeline with Airflow

## Overview

This project is based off a fictious music streaming platform. The ETL process will draw from JSON logs detailing user activities and song metadata stored in Amazon S3, processing it in a data warehouse on Amazon Redshift.


## Airflow Custom Operators

### Stage to Redshift Operator
This operator will load JSON data from S3 into Redshift staging tables. It uses a SQL COPY statement that varies based on the input parameters, which include:
- **redshift_conn_id**: Connection ID for Redshift within Airflow.
- **aws_credentials_id**: AWS credentials connection ID in Airflow.
- **table_name**: Name of the staging table.
- **s3_bucket**: Source S3 bucket.
- **json_path**: JSON path used during the copy operation.
- **execution_date**: Uses the DAG's execution date for data management.

### Load Fact and Dimension Operators
These operators handle loading data into dimension and fact tables, with capabilities for handling different data insertion modes:
- **truncate_insert**: Clears the table before loading new data (common for dimension tables).
- **append**: Adds new entries without deleting old data (used for fact tables).

### Data Quality Operator
Ensures the integrity of the data after the ETL process. It executes SQL checks, comparing the results against expected outcomes, and raises exceptions if discrepancies are found.

## Configuration and Setup

### Airflow Connections
Set up necessary connections through the Airflow UI:
- AWS credentials for accessing S3.
- Redshift connection for accessing the data warehouse.


### Datasets

- Log data: `s3://udacity-dend/log_data`
- Song data: `s3://udacity-dend/song_data`

### Configuring the DAG
Configure your DAG with the following parameters to optimize performance and error handling:
- **No dependencies on past runs.**
- **Retries occur three times with a five-minute interval.**
- **Catchup is turned off to prevent processing historical data unless explicitly required.**
- **No email notifications on retries.**

### Building the Operators
Develop four custom operators to handle the various stages of the ETL process:
- **Stage Operator**: For loading data from S3 to Redshift.
- **Fact and Dimension Operators**: For transforming data into the warehouse schema.
- **Data Quality Operator**: For validating the processed data.

## Execution

Upon setting up operators and ensuring all configurations are correct, enable the DAG in Airflow, and monitor its execution to confirm successful data processing and adherence to data quality standards.
