import datetime
import logging
import os
import tempfile
from os import path

import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

dag = DAG(
    "gcp_movie_ranking",
    start_date=datetime.datetime(year=2025, month=1, day=1),
    end_date=datetime.datetime(year=2025, month=3, day=1),
    schedule_interval="@monthly",
    default_args={"depends_on_past": True},
)

def _extract_gcs_path_from_pubsub(**context):
    messages = context['ti'].xcom_pull(task_ids="listen_for_new_file", key="messages")
    if messages:
        gcs_path = messages[0].data.name
        logging.info(f"Extract GCS path from Pub/Sub message: {gcs_path}")
        return gcs_path
    else:
        raise ValueError("No Pub/Sub message received.")


listen_for_new_file = PubSubPullSensor(
    project_id=os.environ["GCP_PROJECT"],
    subsciprtion=os.environ["PUBSUB_SUBSCRIPTION"],
    ack_mode="AUTO",
    process_messages="return_immediately",
    max_message=1,
    dag=dag,
)


fetch_ratings = PythonOperator(
    task_id="get_gcs_path",
    python_callable=_extract_gcs_path_from_pubsub,
    dag=dag
)


import_in_bigquery = GCSToBigQueryOperator(
    task_id="import_in_bigquery",
    bucket=os.environ["RATINGS_BUCKET"],
    source_objects="{{ ti.xcom_pull(task_ids='get_gcs_path') }}",
    source_format="CSV",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    bigquery_conn_id="gcp",
    skip_leading_rows=1,
    schema_fields=[
        {"name": "userId", "type": "INTEGER"},
        {"name": "movieId", "type": "INTEGER"},
        {"name": "rating", "type": "FLOAT"},
        {"name": "timestamp", "type": "TIMESTAMP"},
    ],
    destination_project_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "ratings${{ ds_nodash }}"
    ),
    dag=dag,
)

query_top_ratings = BigQueryExecuteQueryOperator(
    task_id="query_top_ratings",
    destination_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "rating_results_{{ ds_nodash }}"
    ),
    sql=(
        "SELECT movieid, AVG(rating) as avg_rating, COUNT(*) as num_ratings "
        "FROM " + os.environ["BIGQUERY_DATASET"] + ".ratings "
        "WHERE DATE(timestamp) <= DATE({{ ds }}) "
        "GROUP BY movieid "
        "ORDER BY avg_rating DESC"
    ),
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    bigquery_conn_id="gcp",
    dag=dag,
)

extract_top_ratings = BigQueryToGCSOperator(
    task_id="extract_top_ratings",
    source_project_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "rating_results_{{ ds_nodash }}"
    ),
    destination_cloud_storage_uris=[
        "gs://" + os.environ["RESULT_BUCKET"] + "/{{ ds_nodash }}.csv"
    ],
    export_format="CSV",
    bigquery_conn_id="gcp",
    dag=dag,
)

delete_result_table = BigQueryDeleteTableOperator(
    task_id="delete_result_table",
    deletion_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "rating_results_{{ ds_nodash }}"
    ),
    bigquery_conn_id="gcp",
    dag=dag,
)

fetch_ratings >> import_in_bigquery >> query_top_ratings >> extract_top_ratings >> delete_result_table
