from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta

# Define the default arguments for the DAG
default_args = {
'owner': 'airflow',
'retries': 1,
'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
'create_drop_postgres_table',
default_args=default_args,
description='A simple DAG to create and drop PostgreSQL table',
schedule_interval=None, # Trigger manually or set cron schedule

start_date=days_ago(1),
catchup=False,
) as dag:

    # SQL query to create the table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    # SQL query to drop the table
    drop_table_sql = "DROP TABLE IF EXISTS test_table;"

    # Task to create the table
    create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgress_render', # Connection ID set in Airflow UI
    sql=create_table_sql,
    )

    # Task to drop the table

    drop_table = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgress_render', # Connection ID set in Airflow UI
    sql=drop_table_sql,
    )

# Set the task dependencies
create_table >> drop_table