from airflow import DAG
#from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import requests

# Function to be executed by the PythonOperator
def print_task(task_name):
    print(f"Executing task: {task_name}")

def fetch_people_from_api():
    api_url = "http://api.open-notify.org/astros.json"
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError("Failed to fetch data from the API")

people = fetch_people_from_api()
task_names = []
for i in range(people['number']):
    p = people['people'][i]['name'].split(" ")[0]
    if p not in task_names:
        task_names.append(p)

dag = DAG(
    'dynamic_task_example',
    description='A simple dynamic DAG',
    schedule=None,
    start_date=datetime(2024, 11, 21),
    catchup=False,
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

tasks = []
for task_name in task_names:
    task = PythonOperator(
        task_id=task_name,
        python_callable=print_task,
        op_args=[task_name],
        dag=dag,
    )
    start_task >> task >> end_task
    tasks.append(task)