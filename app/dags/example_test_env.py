# This is a simple DAG to test the Airflow environment setup.
# It contains a single task that does nothing (EmptyOperator).

# Imports may look unused or missing locally, but they are required when Airflow runs inside the container.
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def hello_world():
    # A simple function that prints "Hello World".
    print("Hello World from Airflow!")


with DAG(
    dag_id="dag_example_test_env",
    start_date=timezone.datetime(2025, 1, 1),
    schedule=None,  # Only execute on demand/manual trigger.
    catchup=False,
    tags=["test"],
) as dag:
    hello = PythonOperator(task_id="hello", python_callable=hello_world)
