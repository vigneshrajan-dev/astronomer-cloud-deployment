from airflow.decorators import dag
from datetime import datetime


# Import the custom operator from the plugins directory
from operators.greeting_operator import GreetingOperator


@dag(
   dag_id='custom_operator_dag_example',
   start_date=datetime(2025, 10, 16),
   schedule=None,
   catchup=False,
   tags=['example', 'custom_operator'],
)
def use_custom_operator_dag():
   """
   A simple DAG to demonstrate the use of a custom operator.
   """
   # Instantiate the custom operator as a task
   greet_airflow_task = GreetingOperator(
       task_id='greet_airflow',
       name='Airflow'  # Pass the custom parameter here
   )


   # You can use it multiple times with different parameters
   greet_world_task = GreetingOperator(
       task_id='greet_the_world',
       name='World'
   )


   greet_airflow_task >> greet_world_task


# Instantiate the DAG
use_custom_operator_dag()