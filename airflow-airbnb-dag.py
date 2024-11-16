import os
from datetime import datetime, timedelta
import requests
import boto3
import airflow_airbnb_dag_config as config

import airflow
from airflow import DAG
# from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
# from cosmos.profiles import SnowflakeUserPasswordProfileMapping


def stream_from_github_to_s3():

    # for section in ['listings','reviews']:
    #     for info in config.get(section, []):
    #         # Make an HTTP request to GitHub to get the raw file content
            
    #         github_url = info.get(f'GITHUB_REPO_{section}')
    #         s3_file_path=info.get(f'S3_FILE_PATH_{section}')

    response = requests.get(config.GITHUB_REPO_listings, stream=True)
    
    if response.status_code == 200:
        # Create an S3 client using boto3
        session = boto3.Session(
            aws_access_key_id=BaseHook.get_connection('s3conn').login,
            aws_secret_access_key=BaseHook.get_connection('s3conn').password,
            region_name=config.region  # Specify your region
        )
        
        s3_client = session.client('s3')

        # Upload the file directly to S3 in chunks (streaming data)
        s3_client.upload_fileobj(response.raw, config.S3_BUCKET_NAME, config.S3_FILE_PATH)
        print(f"File uploaded to S3 bucket {config.S3_BUCKET_NAME} with key {config.S3_FILE_PATH}")
    else:
        raise Exception(f"Failed to download file from GitHub. Status code: {response.status_code}")




default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=0.1),
    }

with DAG(
    's3_to_snowflake_dbt_pipeline',
    default_args=default_args,
    description='A pipeline to upload files to S3, load into Snowflake, and run DBT transformations',
    schedule_interval='@daily',  
    start_date=datetime(2024,1,1),
    catchup=False,
) as dag:
    
    stream_from_github_to_s3_tsk = PythonOperator(
        task_id='stream_from_github_to_s3',
        python_callable=stream_from_github_to_s3,
    )
    
    '''
    upload_file_listings = PythonOperator(
        task_id='upload_file_listings',
        python_callable=upload_to_s3,
        op_args=['/usr/local/snowflake-s3-dbt_proj/listings.csv', 'raw/listings/listings.csv', 's3-snowflake-airbnb-elt'],
    )
    
    upload_file_listings = S3FileTransferOperator(
        task_id='upload_file_listings',
        source_file='/usr/local/snowflake-s3-dbt_proj/listings.csv',
        dest_key='raw/listings/listings.csv',
        bucket_name='s3-snowflake-airbnb-elt',
        aws_conn_id='s3conn',  # Make sure this is set to the correct connection ID
        replace=True  # Set to True to replace the file if it already exists
    )
    upload_file_reviews = S3FileTransferOperator(
        task_id='upload_file_reviews',
        source_file='/usr/local/snowflake-s3-dbt_proj/reviews.csv',
        dest_key='raw/listings/reviews.csv',
        bucket_name='s3-snowflake-airbnb-elt',
        aws_conn_id='s3conn',  # Make sure this is set to the correct connection ID
        replace=True  # Set to True to replace the file if it already exists
    )
'''
    copy_sql = """
    COPY INTO airbnb_elt.staging.reviews
    FROM @airbnb_elt.staging.reviews_folder;

    -- Load data from Stage files into the listings table
    COPY INTO airbnb_ELT.STAGING.listings
    FROM @airbnb_ELT.staging.listings_folder;
    """
    load_file_to_snowflake_task = SnowflakeOperator(
        task_id='load_file_to_snowflake',
        sql=copy_sql,  # The SQL query for loading data
        snowflake_conn_id='staging_snowflake_conn_id',  # Replace with your Snowflake connection ID
        autocommit=True,  # Ensure that the transaction is committed after execution
    )
    # 3. Task to run DBT transformations using BashOperator (instead of using DbtDag)

    execution_config = ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt")

    # Use BashOperator to run DBT commands
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd /usr/local/airflow/dags/dbt/airbnb_snowflake_dbt && {execution_config.dbt_executable_path} run",
    )

    # Task dependencies
    # upload_file_listings >> upload_file_reviews >> load_file_to_snowflake_task >> dbt_run
# download_file_from_githubupload_file_listings >> upload_file_reviews >> load_file_to_snowflake_task >> dbt_run
stream_from_github_to_s3_tsk >> load_file_to_snowflake_task >> dbt_run
