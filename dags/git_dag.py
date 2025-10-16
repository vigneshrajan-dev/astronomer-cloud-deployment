from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta  # <-- Only one import needed

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'git_deployment_example',
    default_args=default_args,
    description='A simple DAG deployed via GitHub integration',
    schedule='@daily',
    start_date=datetime.now() - timedelta(days=1),  # <-- Use a valid start date
    catchup=False,
) as dag:

    hello_task = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello from Astro Cloud deployment!"',
    )

    goodbye_task = BashOperator(
        task_id='print_goodbye',
        bash_command='echo "Goodbye from Astro Cloud deployment!"',
    )

    hello_task >> goodbye_task