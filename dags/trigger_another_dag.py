from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# #####################################################################
# 1. The Controller DAG - This DAG triggers the other one
# #####################################################################
with DAG(
    dag_id="controller_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 10 * * *", # Runs daily at 10:00 AM
    catchup=False,
    tags=["controller", "example"],
    doc_md="""
    ### Controller DAG

    This DAG uses the `TriggerDagRunOperator` to start a run of the `target_dag`.
    It also passes some configuration data to it.
    """,
) as controller_dag:
    start_task = BashOperator(
        task_id="start_task",
        bash_command='echo "Controller DAG starting... Will now trigger the target DAG." ',
    )

    # The operator that triggers the other DAG
    trigger_target_dag = TriggerDagRunOperator(
        task_id="trigger_target_dag",
        trigger_dag_id="target_dag",  # 🎯 MUST match the dag_id of the DAG to trigger
        wait_for_completion=True,     # Waits for the target DAG to finish
        poke_interval=30,             # Checks the status of the target DAG every 30 seconds
        conf={                        # ⚙️ Passes a configuration dictionary to the target DAG
            "message": "Hello from the controller!",
            "triggered_at": "{{ ts }}",
        },
    )

    end_task = BashOperator(
        task_id="end_task",
        bash_command='echo "Target DAG finished. Controller DAG is now complete." ',
    )

    start_task >> trigger_target_dag >> end_task

# #####################################################################
# 2. The Target DAG - This DAG is triggered by the controller
# #####################################################################
with DAG(
    dag_id="trigger_another_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  #  Set to None so it only runs when triggered
    catchup=False,
    tags=["target", "example"],
    doc_md="""
    ### Target DAG

    This DAG is triggered by `controller_dag`. It accesses the configuration
    data passed to it by the trigger.
    """,
) as target_dag:
    process_triggered_data = BashOperator(
        task_id="process_triggered_data",
        bash_command='echo "Message from controller: {{ dag_run.conf.get(\'message\') }}" && echo "Triggered at: {{ dag_run.conf.get(\'triggered_at\') }}"',
    )