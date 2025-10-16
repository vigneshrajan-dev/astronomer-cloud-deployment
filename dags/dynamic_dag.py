from __future__ import annotations


import pendulum


from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator


# --- 1. Get the list of DAG names from the Airflow Variable ---
# We use deserialize_json=True to automatically parse the JSON list.
try:
   dag_names = Variable.get("dag_names_list", deserialize_json=True)
except KeyError:
   # Set a default list if the variable is not found
   dag_names = ["default_dynamic_dag"]


# --- 2. Define a function to create a DAG ---
# This is a "DAG factory" function.
def create_dag(dag_id: str):
   """
   Factory function to create a DAG object with a simple task.


   Args:
       dag_id: The unique identifier for the DAG.


   Returns:
       A new DAG object.
   """
   with DAG(
       dag_id=dag_id,
       start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
       catchup=False,
       schedule=None,
       tags=["dynamic-generation", "example"],
   ) as dag:
       # Define a simple task for this DAG
       EmptyOperator(task_id="start_task")


   return dag


# --- 3. Loop through the names and generate DAGs ---


for name in dag_names:
   dag_id = f"{name}"
   globals()[dag_id] = create_dag(dag_id)