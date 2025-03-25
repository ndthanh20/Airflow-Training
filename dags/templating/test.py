from airflow.utils.dates import days_ago
import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = "session4_test",
    schedule_interval="@daily",
    start_date = dt.datetime(2025,3,7),
    end_date= dt.datetime(2025,3,8),
    max_active_runs=1,
    tags=['session4', 'templating']
)

extrget_date = BashOperator(
    task_id="get_data",
    bash_command= (
        ""
        "curl -o /tmp/wikipageviews.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"
        "{{ execution_date.year }}-"
        "{{ '{:02}'.format(execution_date.month) }}/"
        "pageviews-{{ execution_date.year }}"
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
        ),
    dag=dag
)

extract_gz = BashOperator(
    task_id = "extract_gz",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
)

def _fetch_pageviews (pagenames, **context):
    result = dict.fromkeys(pagenames, 0)
    with open(f"/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
    print(result)

fetch_pageviews = PythonOperator(
    task_id = "fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook"
        }
    }
)

extrget_date >> extract_gz >> fetch_pageviews