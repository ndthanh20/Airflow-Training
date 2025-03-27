from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import cross_downstream
from datetime import datetime

with DAG(
    dag_id="cross_downstream_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["session7"],
) as dag:
    start_task = BashOperator(
        task_id="start",
        bash_command='echo "Starting the process..."',
    )

    prepare_data_1 = BashOperator(
        task_id="prepare_data_1",
        bash_command='echo "Preparing data source 1..."',
    )

    prepare_data_2 = BashOperator(
        task_id="prepare_data_2",
        bash_command='echo "Preparing data source 2..."',
    )

    process_a = BashOperator(
        task_id="process_a",
        bash_command='echo "Processing task A..."',
    )

    process_b = BashOperator(
        task_id="process_b",
        bash_command='echo "Processing task B..."',
    )

    analyze_results = BashOperator(
        task_id="analyze_results",
        bash_command='echo "Analyzing processed results..."',
        trigger_rule="all_done",
    )

    end_task = BashOperator(
        task_id="end",
        bash_command='echo "Cross-downstream example completed."',
        trigger_rule="all_done",
    )

    start_task >> [prepare_data_1, prepare_data_2]

    cross_downstream([prepare_data_1, prepare_data_2], [process_a, process_b])

    [process_a, process_b] >> analyze_results >> end_task
