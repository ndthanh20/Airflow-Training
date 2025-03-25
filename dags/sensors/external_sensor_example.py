import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 3, 14, tz="UTC"),
    "retries": 1,
    "max_active_runs": 1,
}

external_dag = DAG(
    dag_id="external_dag_example",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=['session5']
)

task_in_external_dag = BashOperator(
    task_id="task_in_external_dag",
    bash_command="echo 'This task is running in the external_dag_example DAG' && sleep 10",
    dag=external_dag,
)

sensor_dag = DAG(
    dag_id="sensor_dag_example",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=['session5']
)

wait_for_external_task = ExternalTaskSensor(
    task_id="wait_for_external_dag_task",
    external_dag_id="external_dag_example",
    external_task_id="task_in_external_dag",
    allowed_states=["success"],
    timeout=60 * 5,  # 5 minutes timeout for the sensor
    poke_interval=30,  # Check every 30 seconds
    dag=sensor_dag,
)

continue_processing = BashOperator(
    task_id="continue_processing",
    bash_command="echo 'External DAG task completed successfully! Continuing processing in sensor_dag_example DAG'",
    dag=sensor_dag,
)

wait_for_external_task >> continue_processing
