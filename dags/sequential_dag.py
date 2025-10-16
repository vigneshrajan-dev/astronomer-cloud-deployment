from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 1. Define the DAG's arguments (settings)
with DAG(
    dag_id="two_task_sequential_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example", "sequential"],
) as dag:
    
    # 2. Define Task A
    task_a = BashOperator(
        task_id="first_task",
        bash_command='echo "--- Starting the process (Task A) ---"',
    )
    
    # 3. Define Task B
    task_b = BashOperator(
        task_id="second_task",
        bash_command='echo "--- Task A finished, now completing the process (Task B) ---"',
    )
    
    # 4. Define the Task Flow (Dependency)
    # This line sets the dependency: task_a MUST run before task_b.
    task_a >> task_b