from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def print_hello():
    import os

    print(os.getcwd())


with DAG(
    "test_dag",
    default_args=default_args,
    description="A simple test DAG",
    tags=["example"],
) as dag:

    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )
    task1
