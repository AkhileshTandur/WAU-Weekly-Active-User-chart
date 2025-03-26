from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

with DAG(
    dag_id = 'SessionToSnowflake',
    start_date = datetime(2024, 3, 1),
    catchup=False,
    tags=['ETL'],
    schedule = '15 1 * * *'  # Run at 1:15 AM every day
) as dag:
    
    # Create S3 stage
    set_stage = SnowflakeOperator(
        task_id='set_stage',
        snowflake_conn_id='snowflake_conn',
        sql="""
        -- Create raw schema if not exists
        CREATE SCHEMA IF NOT EXISTS dev.raw;
        
        -- Create user_session_channel table
        CREATE TABLE IF NOT EXISTS dev.raw.user_session_channel (
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'  
        );
        
        -- Create session_timestamp table
        CREATE TABLE IF NOT EXISTS dev.raw.session_timestamp (
            sessionId varchar(32) primary key,
            ts timestamp  
        );
        
        -- Create stage
        CREATE OR REPLACE STAGE dev.raw.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """,
    )
    
    # Import data
    load = SnowflakeOperator(
        task_id='load',
        snowflake_conn_id='snowflake_conn',
        sql="""
        -- Import user_session_channel data
        COPY INTO dev.raw.user_session_channel
        FROM @dev.raw.blob_stage/user_session_channel.csv;
        
        -- Import session_timestamp data
        COPY INTO dev.raw.session_timestamp
        FROM @dev.raw.blob_stage/session_timestamp.csv;
        """,
    )
    
    # Define task dependencies
    set_stage >> load