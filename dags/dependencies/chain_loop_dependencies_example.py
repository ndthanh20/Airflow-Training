from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from datetime import datetime

with DAG(
    dag_id="linear_dependencies_chain_loop_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["session7"],
) as dag:
    start_task = BashOperator(
        task_id="start",
        bash_command='echo "Starting the process..."',
    )

    num_tasks = 5
    tasks = []
    for i in range(1, num_tasks + 1):
        task = BashOperator(
            task_id=f"task_{i}",
            bash_command=f'echo "Executing task {i}..."',
        )
        tasks.append(task)

    end_task = BashOperator(
        task_id="end",
        bash_command='echo "All tasks completed in sequence using chain and loop."',
        trigger_rule="all_done",
    )

    chain(start_task, *tasks, end_task)
