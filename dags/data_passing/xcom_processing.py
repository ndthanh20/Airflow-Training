from urllib import request
import os

import airflow
from airflow import DAG
import datetime as dt
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

AIRFLOW_HOME = os.path.realpath(
    os.path.sep.join([os.path.dirname(__file__), "..", ".."])
)
MY_HOME = os.path.realpath(os.path.sep.join([os.path.dirname(__file__), ".."]))

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": dt.datetime(2025,3,20),
    "end_date": dt.datetime(2025,3,21),
    "retries": 1,
    "max_active_runs": 1,
}

params = {
    "postgres_conn_id": "my_postgres",
    "sql_file_path": os.path.join(MY_HOME, "resources"),
}

dag = DAG(
    dag_id="pageviews_xcom_usage_example_dag",
    default_args=default_args,
    params=params,
    description="DAG extracted data from API",
    schedule_interval="@daily",
    catchup=True,
    template_searchpath=os.path.join(MY_HOME, "resources"),
    tags=["session6", "xcom_example"]
)


def _get_data(year, month, day, hour, **context):
    obj = context['dag'].dag_id.split('_')[0]
    task_instance = context['ti']

    url = (
        f"https://dumps.wikimedia.org/other/{obj}/"
        f"{year}/{year}-{month:0>2}/"
        f"{obj}-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path = f"{AIRFLOW_HOME}/data/{obj}_{year}{month:0>2}{day:0>2}-{hour:0>2}.gz"
    output_file_name = f"{AIRFLOW_HOME}/data/{obj}_{year}{month:0>2}{day:0>2}-{hour:0>2}"

    task_instance.xcom_push(key="table_name", value=obj)
    task_instance.xcom_push(key="output_path", value=output_path)
    task_instance.xcom_push(key='output_file_name', value=output_file_name)

    print(f"Starting: Retrieve data from url: {url}")
    request.urlretrieve(url, output_path)
    print(f"Completed: Retrieve data from url: {url}")
    


def _fetch_pageviews(pagenames, execution_date, **context):
    result = dict.fromkeys(pagenames, 0)
    output_file_name = context['ti'].xcom_pull(task_ids='get_data', key='output_file_name')

    with open(output_file_name, "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    resource_file_path = context["params"]["sql_file_path"]

    with open(f"{resource_file_path}/insert_statement.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                f"INSERT INTO {context['ti'].xcom_pull(task_ids='get_data', key='table_name')} VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )
    
    context['ti'].xcom_push(key='stats', value=result)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ data_interval_start.year }}",
        "month": "{{ data_interval_start.month }}",
        "day": "{{ data_interval_start.day }}",
        "hour": "{{ data_interval_start.hour }}",
    },
    dag=dag,
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force {{ ti.xcom_pull(task_ids='get_data', key='output_path') }}",
    dag=dag,
)

fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook",
        },
    },
    dag=dag,
)

create_table_in_postgres = SQLExecuteQueryOperator(
    task_id="create_table_in_postgres",
    conn_id="my_postgres",
    sql="create_table_statement.sql",
    dag=dag,
)

write_data_to_postgres = SQLExecuteQueryOperator(
    task_id="write_data_to_postgres",
    conn_id="my_postgres",
    sql="insert_statement.sql",
    dag=dag,
)

get_data >> extract_gz >> create_table_in_postgres >> fetch_pageviews >> write_data_to_postgres
