from __future__ import annotations


import pendulum


from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


with DAG(
   dag_id="task_group_example_dag",
   start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
   catchup=False,
   schedule=None,
   tags=["example", "taskgroup"],
   doc_md=__doc__,
) as dag:
   # A task defined at the top level of the DAG
   start = BashOperator(task_id="start", bash_command="echo 'Starting the pipeline...'")


   # Define the main TaskGroup
   # The group_id is what's displayed in the Graph View
   with TaskGroup(group_id="data_processing_group", tooltip="All data processing steps") as data_processing_group:
      
       extract = BashOperator(
           task_id="extract_data",
           bash_command="echo 'Extracting data...'"
       )
      
       validate = BashOperator(
           task_id="validate_data",
           bash_command="echo 'Validating data...'"
       )


       # Define a nested TaskGroup for transformation tasks
       with TaskGroup(group_id="transformation_group", tooltip="Data transformation tasks") as transformation_group:
          
           # These tasks will appear inside the 'transformation_group' which is inside 'data_processing_group'
           transform_a = BashOperator(
               task_id="transform_a",
               bash_command="echo 'Running transformation A'"
           )
          
           transform_b = BashOperator(
               task_id="transform_b",
               bash_command="echo 'Running transformation B'"
           )
          
           transform_c = BashOperator(
               task_id="transform_c",
               bash_command="echo 'Running transformation C'"
           )
          
           # Set dependencies within the nested group
           [transform_a, transform_b] >> transform_c


       load = BashOperator(
           task_id="load_data",
           bash_command="echo 'Loading data...'"
       )


       # Set dependencies within the main TaskGroup.
       # Note how we refer to the entire nested TaskGroup `transformation_group` as a single unit.
       extract >> validate >> transformation_group >> load


   # A final task at the top level
   end = BashOperator(task_id="end", bash_command="echo 'Pipeline finished.'")


   # Set the final top-level dependencies
   # The entire `data_processing_group` is treated as one entity in this dependency chain.
   start >> data_processing_group >> end