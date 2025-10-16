from __future__ import annotations
import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id="taskflow",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "taskflow"],
)
def simple_etl_dag():
    """
    ### Simple ETL DAG
    A simple DAG with three tasks demonstrating a linear data flow
    using the TaskFlow API.
    """

    @task
    def extract() -> list[int]:
        """Pretend to extract data from a source."""
        print("Extracting data...")
        return [10, 20, 30, 40]

    @task
    def transform(numbers: list[int]) -> int:
        """Sum the numbers from the extracted data."""
        print(f"Transforming data: {numbers}")
        total = sum(numbers)
        print(f"Total is: {total}")
        return total

    @task
    def load(total: int):
        """Pretend to load the final result."""
        print(f"Loading final result: {total}")
        print("Load complete!")

    # --- Define the workflow ---
    # The output of one task is passed as the input to the next
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)

# Instantiate the DAG
simple_etl_dag()