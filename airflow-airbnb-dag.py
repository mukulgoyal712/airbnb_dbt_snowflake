import os
from datetime import datetime, timedelta
import requests
import boto3
import airflow_airbnb_dag_config as config

import airflow
from airflow import DAG
# from airflow.decorators import dag, task
from airflow.utils.email import send_email
from airflow.hooks.base_hook import BaseHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
# from cosmos.profiles import SnowflakeUserPasswordProfileMapping

def notify_on_failure(context):
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = context['dag'].dag_id
    execution_time = context['execution_date']
    
    subject = f"Task Failed: {task_id} in DAG: {dag_id}"
    html = f"""
    <h3>Task Failed: {task_id}</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Execution Time:</strong> {execution_time}</p>
    <p><strong>Error:</strong> {task_instance.state}</p>
    """
    
    send_email(
        to=["mukulgoyal712@gmail.com"],  # Change to your email or use a list
        subject=subject,
        html_content=html
    )

def stream_from_github_to_s3():

    for section in ['listings','reviews']:
        for info in config.get(section, []):
            # Make an HTTP request to GitHub to get the raw file content
            
            github_url = info.get(f'GITHUB_REPO_{section}')
            s3_file_path=info.get(f'S3_FILE_PATH_{section}')

            response = requests.get(github_url, stream=True)
            
            if response.status_code == 200:
                # Create an S3 client using boto3
                session = boto3.Session(
                    aws_access_key_id=BaseHook.get_connection('s3conn').login,
                    aws_secret_access_key=BaseHook.get_connection('s3conn').password,
                    region_name=config.region  # Specify your region
                )
                
                s3_client = session.client('s3')

                # Upload the file directly to S3 in chunks (streaming data)
                s3_client.upload_fileobj(response.raw, config.S3_BUCKET_NAME, s3_file_path)
                print(f"File uploaded to S3 bucket {config.S3_BUCKET_NAME} with key {s3_file_path}")
            else:
                raise Exception(f"Failed to download file from GitHub. Status code: {response.status_code}")




default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=0.1),
        'email_on_failure': True, 
        'on_failure_callback': notify_on_failure,
    }

with DAG(
    's3_to_snowflake_dbt_pipeline',
    default_args=default_args,
    description='A pipeline to upload files to S3, load into Snowflake, and run DBT transformations',
    schedule_interval='@daily',  
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=['data_pipeline', 'github', 's3', 'snowflake', 'dbt'],
) as dag:
    
    #task to load csv from github repo to s3 bucket
    stream_from_github_to_s3_tsk = PythonOperator(
        task_id='stream_from_github_to_s3',
        python_callable=stream_from_github_to_s3,
    )
    
    #task to load data into staging area snowflake
    copy_sql = """
    COPY INTO airbnb_elt.staging.reviews
    FROM @airbnb_elt.staging.reviews_folder;

    -- Load data from Stage files into the listings table
    COPY INTO airbnb_ELT.STAGING.listings
    FROM @airbnb_ELT.staging.listings_folder;
    """
    load_file_to_snowflake_task = SnowflakeOperator(
        task_id='load_file_to_snowflake',
        sql=copy_sql, 
        snowflake_conn_id='staging_snowflake_conn_id', 
        autocommit=True, 
    )
    
    #task to run dbt commands

    execution_config = ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt")

    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd /usr/local/airflow/dags/dbt/airbnb_snowflake_dbt && {execution_config.dbt_executable_path} run",
    )

stream_from_github_to_s3_tsk >> load_file_to_snowflake_task >> dbt_run
