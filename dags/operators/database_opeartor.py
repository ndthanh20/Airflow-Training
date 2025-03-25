from urllib import request
import os

import airflow
from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


AIRFLOW_HOME = os.path.realpath(
    os.path.sep.join([os.path.dirname(__file__), "..", ".."])
)
MY_HOME = os.path.realpath(os.path.sep.join([os.path.dirname(__file__), ".."]))

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
    "max_active_runs": 1
}

params = {
    "output_path": f"{AIRFLOW_HOME}/data/wikipageviews.gz",
    "output_file_name": f"{AIRFLOW_HOME}/data/wikipageviews",
    "sql_file_path": os.path.join(MY_HOME, "resources"),
}

dag = DAG(
    dag_id="operator_dag_example",
    default_args=default_args,
    params=params,
    description="DAG extracted stocksense data",
    schedule_interval=None,
    catchup=False,
    template_searchpath=os.path.join(MY_HOME, "resources"),
    tags=["session5"]
)


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-010000.gz"
    )
    print(f"Starting: Retrieve data from url: {url}")
    request.urlretrieve(url, output_path)
    print(f"Completed: Retrieve data from url: {url}")


def _fetch_pageviews(pagenames, output_file_name, execution_date, **context):
    result = dict.fromkeys(pagenames, 0)
    with open(output_file_name, "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    resource_file_path = context["params"]["sql_file_path"]

    with open(f"{resource_file_path}/insert_statement.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "{{ params.output_path }}",
    },
    dag=dag,
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force {{ params.output_path }}",
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
        "output_file_name": "{{ params.output_file_name }}",
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

(
    get_data
    >> extract_gz
    >> create_table_in_postgres
    >> fetch_pageviews
    >> write_data_to_postgres
)
