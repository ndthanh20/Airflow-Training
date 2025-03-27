from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from config.params import default_args, params, ingest_config
from lib.utils import fetch_and_store_api_data, check_data_exists
from airflow.utils.task_group import TaskGroup

import os

AIRFLOW_HOME = os.path.realpath(
    os.path.sep.join([os.path.dirname(__file__), "..", ".."])
)
MY_HOME = os.path.realpath(os.path.sep.join([os.path.dirname(__file__), ".."]))

with DAG(
    "branching_dag_example",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params=params,
    template_searchpath=os.path.join(MY_HOME, "resources"),
    tags=["session7"],
) as dag:

    start_pipeline = DummyOperator(task_id="start_pipeline")

    end_pipeline = DummyOperator(task_id="end_pipeline")

    analyze_task = SQLExecuteQueryOperator(
        task_id="summary_info_task",
        conn_id="{{ params.postgres_conn_id }}",
        sql="summary_info.sql",
    )

    no_data_found = DummyOperator(task_id="no_data_found")

    data_exists = DummyOperator(task_id="data_exists")

    for param in ingest_config["tables"]:
        target_table_name = param["target_table_name"]
        source_table_name = param["source_table_name"]
        target_fields = param["target_fields"]

        if not target_table_name:
            print(
                f"Skipping table configuration due to missing 'target_table_name': {param}"
            )
            continue
        if not source_table_name:
            print(
                f"Skipping table configuration for '{target_table_name}' due to missing 'source_table_name': {param}"
            )
            continue

        with TaskGroup(f"processing_{target_table_name}_group") as processing_group:
            create_raw_table = SQLExecuteQueryOperator(
                task_id=f"create_raw_table_{target_table_name}_task",
                conn_id="{{ params.postgres_conn_id }}",
                sql=f"{target_table_name}.sql",
            )

            fetch_and_store_data = PythonOperator(
                task_id=f"fetch_and_store_{target_table_name}_task",
                python_callable=fetch_and_store_api_data,
                op_kwargs={
                    "schema_name": "raw",
                    "target_table_name": target_table_name,
                    "target_fields": target_fields,
                },
            )

            check_data = BranchPythonOperator(
                task_id=f"check_data_exists_{target_table_name}_task",
                python_callable=check_data_exists,
                op_kwargs={
                    "schema_name": "raw",
                    "target_table_name": target_table_name,
                },
            )

            (
                create_raw_table
                >> fetch_and_store_data
                >> check_data
                >> [no_data_found, data_exists]
            )

        start_pipeline >> processing_group

    no_data_found >> end_pipeline
    data_exists >> analyze_task >> end_pipeline
