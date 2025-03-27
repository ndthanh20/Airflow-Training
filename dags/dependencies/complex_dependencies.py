from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="fan_out_fan_in_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["session7"],
) as dag:
    start_task = BashOperator(
        task_id="start",
        bash_command='echo "Starting the process..."',
    )

    process_part_a = BashOperator(
        task_id="process_part_a",
        bash_command='echo "Processing part A..."',
    )

    process_part_b = BashOperator(
        task_id="process_part_b",
        bash_command='echo "Processing part B..."',
    )

    process_part_c = BashOperator(
        task_id="process_part_c",
        bash_command='echo "Processing part C..."',
    )

    aggregate_results = BashOperator(
        task_id="aggregate_results",
        bash_command='echo "Aggregating results..."',
        trigger_rule="all_success",
    )

    end_task = BashOperator(
        task_id="end",
        bash_command='echo "Final aggregation complete."',
        trigger_rule="all_done",
    )

    (
        start_task
        >> [process_part_a, process_part_b, process_part_c]
        >> aggregate_results
        >> end_task
    )
