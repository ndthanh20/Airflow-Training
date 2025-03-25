from urllib import request
import os

import airflow
from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


AIRFLOW_HOME = os.path.realpath(
    os.path.sep.join([os.path.dirname(__file__), "..", ".."])
)
MY_HOME = os.path.realpath(os.path.sep.join([os.path.dirname(__file__), ".."]))

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
    "max_active_runs": 1,
}

params = {
    "output_path": f"{AIRFLOW_HOME}/data/wikipageviews.gz",
    "output_file_name": f"{AIRFLOW_HOME}/data/wikipageviews",
    "postgres_conn_id": "my_postgres",
    "sql_file_path": os.path.join(MY_HOME, "resources"),
}

dag = DAG(
    dag_id="stock_sense_dag_example",
    default_args=default_args,
    params=params,
    description="DAG extracted stocksense data",
    schedule_interval="@hourly",
    catchup=False,
    template_searchpath=os.path.join(MY_HOME, "resources"),
    tags=["session5"]
)


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    print(f"Starting: Retrieve data from url: {url}")
    request.urlretrieve(url, output_path)
    print(f"Completed: Retrieve data from url: {url}")


def _fetch_pageviews(pagenames, output_file_name, execution_date, **context):
    result = dict.fromkeys(pagenames, 0)
    hook = PostgresHook(postgres_conn_id=context["params"]["postgres_conn_id"])
    with open(output_file_name, "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open("/tmp/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            insert_statement = (
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )
            print(f"Starting: Executed SQL statement: {insert_statement}")
            hook.run(insert_statement)
            print(f"Completed: Executed SQL statement: {insert_statement}")


def _execute_query(sql_file_path, **context):
    try:
        hook = PostgresHook(postgres_conn_id=context["params"]["postgres_conn_id"])

        with open(sql_file_path, "r") as sql_file:
            sql_statement = sql_file.read()

        # Validate SQL statement
        if not sql_statement:
            raise ValueError("SQL statement cannot be empty.")

        # Execute the statement
        if sql_statement.strip(" \n\t").startswith("SELECT"):
            result = hook.get_pandas_df(sql_statement)
            print(f"Completed: Executed SQL statement: {sql_statement}")
            return result
        else:
            hook.run(sql_statement)
            print(f"Completed: Executed SQL statement: {sql_statement}")
    except ValueError as e:
        print(f"Error: Invalid SQL statement: {e}")
    except Exception as e:
        print(f"Error: Executing SQL statement: {e}")
        raise e


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

create_table_in_postgres = PythonOperator(
    task_id="create_table_in_postgres",
    python_callable=_execute_query,
    op_kwargs={
        "sql_file_path": "{{params.sql_file_path}}/create_table_statement.sql",
    },
    dag=dag,
)

get_data >> extract_gz >> create_table_in_postgres >> fetch_pageviews
