from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from datetime import datetime

with DAG(
    dag_id="linear_dependencies_chain_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["session7"],
) as dag:
    task_one = BashOperator(
        task_id="task_one",
        bash_command='echo "Executing task one..."',
    )

    task_two = BashOperator(
        task_id="task_two",
        bash_command='echo "Executing task two..."',
    )

    task_three = BashOperator(
        task_id="task_three",
        bash_command='echo "Executing task three..."',
    )

    end_task = BashOperator(
        task_id="end",
        bash_command='echo "All tasks completed in sequence using chain."',
        trigger_rule="all_done",
    )

    chain(task_one, task_two, task_three, end_task)
