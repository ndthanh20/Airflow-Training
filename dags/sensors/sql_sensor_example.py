from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from typing import Dict
import os
from pendulum import datetime

MY_HOME = os.path.realpath(os.path.sep.join([os.path.dirname(__file__), ".."]))


def _success_criteria(record):
    return record


def _failure_criteria(record):
    return True if not record else False


def validation_function() -> Dict[str, str]:
    return {"partner_name": "partner_a", "partner_validation": True}


def storing_function():
    print("storing")


dag = DAG(
    dag_id="sql_sensor_example",
    start_date=datetime(2025, 3, 15),
    schedule="@daily",
    catchup=False,
    template_searchpath=os.path.join(MY_HOME, "resources"),
    tags=['session5']
)
waidata_for_data = SqlSensor(
    task_id="waidata_for_data",
    conn_id="my_postgres",
    sql="data_validation.sql",
    parameters={"pagename": "Facebook"},
    success=_success_criteria,
    failure=_failure_criteria,
    fail_on_empty=False,
    poke_interval=20,
    mode="reschedule",
    timeout=60 * 5,
    dag=dag,
)


validation = PythonOperator(
    task_id="validation",
    python_callable=validation_function,
    dag=dag,
)


storing = PythonOperator(
    task_id="storing",
    python_callable=storing_function,
    dag=dag,
)

waidata_for_data >> validation >> storing
