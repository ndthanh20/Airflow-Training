from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),
}

with DAG(
    'session4_macros_example',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['session4', 'templating']
) as dag:

    bash_task = BashOperator(
        task_id='print_dates',
        bash_command='echo "Today is {{ ds }} and tomorrow is {{ macros.ds_add(ds, 1) }}."'
    )
