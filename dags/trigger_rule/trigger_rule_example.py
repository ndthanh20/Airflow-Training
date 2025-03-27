from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


def decide_branch(**kwargs):
    """Decides whether to go to success or failure branch."""
    if kwargs["dag_run"].logical_date.day % 2 == 0:
        return "always_success"
    else:
        return "always_fail"


def log_message(message, **kwargs):
    print(f"Task {kwargs['task_instance'].task_id}: {message}")


with DAG(
    dag_id="trigger_rule_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["session7"],
) as dag:
    # Task that always succeeds
    always_success = BashOperator(
        task_id="always_success",
        bash_command='echo "This task always succeeds" && exit 0',
    )

    # Task that always fails
    always_fail = BashOperator(
        task_id="always_fail",
        bash_command='echo "This task always fails" && exit 1',
    )

    # Task that might be skipped based on the day of the month
    branching_task = BranchPythonOperator(
        task_id="branching_task",
        python_callable=decide_branch,
    )

    skip_success = BashOperator(
        task_id="skip_success",
        bash_command='echo "This task will be skipped"',
    )

    skip_fail = BashOperator(
        task_id="skip_fail",
        bash_command='echo "This task will be skipped"',
    )

    # Set dependency for the branching task
    branching_task >> [skip_success, skip_fail]

    # Task with trigger_rule='all_success' (default)
    all_success_downstream = PythonOperator(
        task_id="all_success_downstream",
        python_callable=log_message,
        op_kwargs={"message": "All upstream tasks succeeded"},
    )
    [always_success, skip_success] >> all_success_downstream

    # Task with trigger_rule='all_failed'
    all_failed_downstream = PythonOperator(
        task_id="all_failed_downstream",
        python_callable=log_message,
        trigger_rule=TriggerRule.ALL_FAILED,
        op_kwargs={"message": "All upstream tasks failed"},
    )
    always_fail >> all_failed_downstream

    # Task with trigger_rule='all_done'
    all_done_downstream = PythonOperator(
        task_id="all_done_downstream",
        python_callable=log_message,
        trigger_rule=TriggerRule.ALL_DONE,
        op_kwargs={"message": "All upstream tasks are done (regardless of status)"},
    )
    [always_success, always_fail, skip_success, skip_fail] >> all_done_downstream

    # Task with trigger_rule='one_success'
    one_success_downstream = PythonOperator(
        task_id="one_success_downstream",
        python_callable=log_message,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        op_kwargs={"message": "At least one upstream task succeeded"},
    )
    [always_success, always_fail] >> one_success_downstream

    # Task with trigger_rule='one_failed'
    one_failed_downstream = PythonOperator(
        task_id="one_failed_downstream",
        python_callable=log_message,
        trigger_rule=TriggerRule.ONE_FAILED,
        op_kwargs={"message": "At least one upstream task failed"},
    )
    [always_success, always_fail] >> one_failed_downstream

    # Task with trigger_rule='none_failed'
    none_failed_downstream = PythonOperator(
        task_id="none_failed_downstream",
        python_callable=log_message,
        trigger_rule=TriggerRule.NONE_FAILED,
        op_kwargs={
            "message": "None of the upstream tasks failed (may have succeeded or been skipped)"
        },
    )
    [always_success, skip_success] >> none_failed_downstream

    # Task with trigger_rule='none_skipped'
    none_skipped_downstream = PythonOperator(
        task_id="none_skipped_downstream",
        python_callable=log_message,
        trigger_rule=TriggerRule.NONE_SKIPPED,
        op_kwargs={
            "message": "None of the upstream tasks were skipped (may have succeeded or failed)"
        },
    )
    [always_success, always_fail] >> none_skipped_downstream
