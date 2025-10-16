#Dag with task 1
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 1. Define the DAG's arguments (settings)
with DAG(
    dag_id="simple_print_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example", "basic"],
) as dag:
    
    # 2. Define the Task
    print_message_task = BashOperator(
        task_id="print_a_message",
        bash_command='echo "Hello from my first Airflow DAG!"',
    )


