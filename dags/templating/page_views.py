from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = "session4_page_views",
    start_date=days_ago(2),
    schedule_interval="@daily",
    tags=['session4', 'templating']
)

create_dir = BashOperator(
        task_id='check_and_create_dir',
        bash_command="""
            if [ ! -d "/tmp/data/{{ ds_nodash }}" ]; then
                mkdir -p /tmp/data/{{ ds_nodash }}
                echo "Directory /tmp/data/{{ ds_nodash }} created."
            else
                echo "Directory /tmp/data/{{ ds_nodash }} already exists."
            fi
        """
    )

extrget_date = BashOperator(
    task_id="get_data",
    bash_command= (
        ""
        "curl -o /tmp/data/{{ds_nodash}}/wikipageviews.gz " 
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
    bash_command="gunzip --force /tmp/data/{{ds_nodash}}/wikipageviews.gz",
)

def _fetch_pageviews (pagenames, **context):
    result = dict.fromkeys(pagenames, 0)
    with open(f"/tmp/data/{context['ds_nodash']}/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
    
    return result

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

def _print_pageviews(pageview, **context):
    print(pageview)

print_page_views = PythonOperator(
    task_id = "print_pageviews",
    python_callable=_print_pageviews,
    op_kwargs={"pageview": "{{ti.xcom_pull(key='return_value', task_ids='fetch_pageviews')}}"},
    dag = dag
)

create_dir >> extrget_date >> extract_gz >> fetch_pageviews >> print_page_views