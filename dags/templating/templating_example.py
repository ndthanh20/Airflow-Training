from airflow.utils.dates import days_ago
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="session4_templating",
    start_date=days_ago(1),
    schedule="0 */2 * * *",
    catchup=False,
    tags=['session4', 'templating']
)

get_date = BashOperator(
    task_id="get_data",
    bash_command= (
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
def _get_variable (task_instance, **context):
    print("Task intansce:", task_instance)

get_variable = PythonOperator(
    task_id = "get_variable",
    python_callable=_get_variable,
    op_kwargs={"task_intansce": "{{ti.x}}"},
    dag=dag
)
