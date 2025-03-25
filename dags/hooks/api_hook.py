from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator


def make_http_request(**kwargs):
    """
    Makes an HTTP GET request using HttpHook.
    """
    http_conn_id = "my_http_connection"  # Replace with your HTTP connection ID
    endpoint = "/2.0.0/launch/upcoming/"  # Replace with the desired API endpoint

    http_hook = HttpHook(http_conn_id=http_conn_id, method="GET")

    try:
        response = http_hook.run(endpoint=endpoint)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        print(f"Successfully retrieved data: {data}")
    except Exception as e:
        print(f"Error during HTTP request: {e}")


with DAG(
    dag_id="http_hook_example",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["session5"]
) as dag:
    http_task = PythonOperator(
        task_id="make_http_get_request",
        python_callable=make_http_request,
    )
