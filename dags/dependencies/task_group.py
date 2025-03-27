from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="task_group_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["session7"],
) as dag:
    extract_task = BashOperator(
        task_id="extract_data", bash_command="echo 'Extracting data...'"
    )

    with TaskGroup("processing_group") as processing_group:
        transform_task = BashOperator(
            task_id="transform_data", bash_command="echo 'Transforming data...'"
        )
        load_task = BashOperator(
            task_id="load_data", bash_command="echo 'Loading data...'"
        )

        transform_task >> load_task

    extract_task >> processing_group
