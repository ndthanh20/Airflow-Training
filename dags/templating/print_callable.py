from airflow.utils.dates import days_ago
from urllib import request
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="session4_templating_print_context",
    start_date=days_ago(1),
    schedule_interval="@daily",
    tags=['session4', 'templating']
)


def _print_context(**kwargs):
    for key, value in kwargs.items():
        print("key: ", key, " , value:", value)


def _get_data1(execution_date):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path = "/tmp/wikipageviews.gz"
    print(url)
    request.urlretrieve(url, output_path)


def _get_data2(output_path, **context):
    year,month, day, hour, *_ = context['data_interval_start'].timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    print(url)
    request.urlretrieve(url, output_path)
def _get_data3(year, month, day, hour, output_path, **context):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    print(url)
    request.urlretrieve(url, output_path)


print_context = PythonOperator(
    task_id="print_context", python_callable=_print_context, dag=dag
)
# get_data = PythonOperator(
#     task_id = "get_data",
#     python_callable=_get_data1,
#     dag=dag
# )


# get_data = PythonOperator(
#     task_id = "get_data",
#     python_callable=_get_data2,
#     op_args=["/tmp/wikipageviews_{{ds}}.gz"],
#     # op_kwargs={"output_path2": "/tmp/wikipageviews.gz"},
#     dag=dag
# )
get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data3,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "/tmp/wikipageviews.gz",
    },
)

print_context >> get_data
