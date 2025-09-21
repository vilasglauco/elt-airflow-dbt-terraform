"""
Example DAG to test the Airflow environment setup.
This DAG demonstrates a simple task that prints "Hello World" to verify that the Airflow 
environment is functioning correctly.
"""

from airflow.utils import timezone
from airflow.decorators import dag, task


@dag(
    dag_id="example_test_env",
    start_date=timezone.datetime(2025, 1, 1),
    schedule=None,  # Only execute on demand/manual trigger.
    catchup=False,
    tags=["test"],
)

def example_test_env():
    """ A simple DAG to test the Airflow environment setup. """
    @task()
    def hello_world():
        # A simple function that prints "Hello World".
        print("Hello World from Airflow!")

    hello_world()


dag = example_test_env()
