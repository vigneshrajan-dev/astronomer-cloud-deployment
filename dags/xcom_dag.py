from __future__ import annotations


import pendulum


from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _producer_explicit_push(**kwargs):
   """
   Pushes multiple values to XCom with custom keys.
   This provides more control than just returning a single value.
   """
   ti = kwargs["ti"]
   user_data = {"name": "Alice", "id": "user_1234", "role": "admin"}
   transaction_ids = [9001, 9002, 9005, 9010]
  
   # Push a dictionary with a specific key
   ti.xcom_push(key="user_profile", value=user_data)
  
   # Push a list with another key
   ti.xcom_push(key="recent_transactions", value=transaction_ids)
  
   print(f"Pushed user profile: {user_data}")
   print(f"Pushed transaction IDs: {transaction_ids}")


def _consumer_pull_multiple(**kwargs):
   """
   Pulls the multiple, specific values pushed by the producer task.
   """
   ti = kwargs["ti"]
  
   # Pull the dictionary by its custom key
   user = ti.xcom_pull(task_ids="producer_explicit_push", key="user_profile")
  
   # Pull the list by its custom key
   transactions = ti.xcom_pull(task_ids="producer_explicit_push", key="recent_transactions")
  
   if not user or not transactions:
       raise ValueError("Failed to pull required values from XCom!")
      
   print(f"Pulled user name: {user['name']}")
   print(f"Number of transactions pulled: {len(transactions)}")
   print(f"Processing transactions for user ID {user['id']}...")




with DAG(
   dag_id="xcom_dag",
   start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
   catchup=False,
   schedule=None,
   tags=["example", "xcom", "advanced"],
   doc_md=__doc__,
) as dag:
   # Traditional PythonOperator for explicit push/pull
   producer_task = PythonOperator(
       task_id="producer_explicit_push",
       python_callable=_producer_explicit_push,
   )


   consumer_task = PythonOperator(
       task_id="consumer_pull_multiple",
       python_callable=_consumer_pull_multiple,
   )
  
   # BashOperator using Jinja templating to pull an XCom value
   # Note the special Jinja syntax to pull the 'user_profile' dictionary
   # and access the 'name' key within it.
   bash_consumer = BashOperator(
       task_id="bash_consumer_jinja",
       bash_command='echo "User name pulled from XCom via Jinja: {{ ti.xcom_pull(task_ids=\'producer_explicit_push\', key=\'user_profile\')[\'name\'] }}"',
   )


   # Set dependencies for the traditional tasks
   producer_task >> [consumer_task, bash_consumer]
