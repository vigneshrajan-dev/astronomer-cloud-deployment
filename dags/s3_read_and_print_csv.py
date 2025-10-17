import pandas as pd
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
import pendulum
import io
import logging


# Default arguments for the DAG
default_args = {
   'owner': 'airflow',
   'start_date': pendulum.datetime(2025, 1, 1, tz="UTC")
,
   'retries': 1,
}


# Define the DAG
dag = DAG(
   's3_read_and_print_csv',
   default_args=default_args,
   description='DAG to read CSV from S3 and print the values',
   schedule=None, # Set to None for manual execution
)


# AWS S3 connection details
BUCKET_NAME = 'airflowbuckets25'
OBJECT_KEY = 'awsdata/user.csv'
#LOCAL_FILE_PATH = '/usr/local/airflow/include/'


# Function to read the CSV from S3 and print its values
def read_and_print_csv_from_s3():
   # Use S3Hook to interact with S3
   s3_hook = S3Hook(aws_conn_id='aws_s3') # Use your AWS connection ID


   # Fetch the file from S3 as a byte stream
   file_obj = s3_hook.get_key(key=OBJECT_KEY, bucket_name=BUCKET_NAME)


   if file_obj:
       # Read the content into a pandas DataFrame
       csv_content = file_obj.get()["Body"].read()
       df = pd.read_csv(io.BytesIO(csv_content)) # Read the CSV from byte stream


       # Print the DataFrame content
       logging.info(df)
   else:
       logging.info(f"File {OBJECT_KEY} not found in bucket {BUCKET_NAME}")


wait_for_file = S3KeySensor(
   task_id='wait_for_s3_file',
   bucket_name=BUCKET_NAME,
   bucket_key=OBJECT_KEY,
   aws_conn_id='aws_s3', # Use your AWS connection ID


   poke_interval=60, # How often to check the S3 bucket (in seconds)
   timeout=600, # How long to wait before giving up (in seconds)
   mode='poke', # This mode will keep checking until the file is found
   dag=dag,
)


# Task to read and print CSV content
read_csv_task = PythonOperator(
   task_id='read_and_print_csv',
   python_callable=read_and_print_csv_from_s3,
   dag=dag,
)


wait_for_file >> read_csv_task