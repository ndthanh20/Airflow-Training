from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from datetime import datetime

# Define a custom operator with templated fields.
class MyCustomOperator(BaseOperator):
    # Specify which attributes should be templated.
    template_fields = ('my_param', 'my_list_param')

    @apply_defaults
    def __init__(self, my_param, my_list_param=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.my_param = my_param
        self.my_list_param = my_list_param or []

    def execute(self, context):
        # Log the rendered parameters.
        self.log.info("Rendered my_param: %s", self.my_param)
        self.log.info("Rendered my_list_param: %s", self.my_list_param)
        
        # For demonstration, join list elements if provided.
        if self.my_list_param:
            joined_list = ','.join(self.my_list_param)
            self.log.info("Joined list: %s", joined_list)
        
        return "Execution Completed"

# Define a sample DAG that uses the custom operator.
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),
}

with DAG(
    'session4_custom_operator',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=["session4", "templating"]
) as dag:

    # Use the custom operator.
    custom_task = MyCustomOperator(
        task_id='my_custom_task',
        # These fields can include Jinja templating expressions.
        my_param='Value for {{ ds }}',
        my_list_param=['item1', 'item2', 'Current date: {{ ds }}']
    )

    custom_task