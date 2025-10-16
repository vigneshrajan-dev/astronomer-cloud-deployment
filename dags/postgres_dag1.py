from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'create_drop_postgres_table_test1',
    default_args=default_args,
    description='A simple DAG to create and drop PostgreSQL table',
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
) as dag:

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    drop_table_sql = "DROP TABLE IF EXISTS test_table;"

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_render',  # <-- Use 'conn_id'
        sql=create_table_sql,
    )

    drop_table = SQLExecuteQueryOperator(
        task_id='drop_table',
        conn_id='postgres_render',  # <-- Use 'conn_id'
        sql=drop_table_sql,
    )

    create_table >> drop_table