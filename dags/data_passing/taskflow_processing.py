from urllib import request
import os

from airflow.decorators import dag, task
import datetime as dt
from airflow.providers.postgres.hooks.postgres import PostgresHook

AIRFLOW_HOME = os.path.realpath(
    os.path.sep.join([os.path.dirname(__file__), "..", ".."])
)
MY_HOME = os.path.realpath(os.path.sep.join([os.path.dirname(__file__), ".."]))

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": dt.datetime(2025, 3, 20),
    "end_date": dt.datetime(2025, 3, 21),
    "retries": 1,
    "max_active_runs": 1,
}

params = {
    "postgres_conn_id": "my_postgres",
    "sql_file_path": os.path.join(MY_HOME, "resources"),
}


@dag(
    dag_id="pageviews_taskflow_example_dag",
    default_args=default_args,
    params=params,
    description="DAG extracted data from API using Taskflow API",
    schedule_interval="@daily",
    catchup=True,
    template_searchpath=os.path.join(MY_HOME, "resources"),
    tags=["session6", "taskflow_example"],
)
def pageviews_taskflow_example_dag():
    @task
    def get_data(year, month, day, hour, **context):
        obj = context['dag'].dag_id.split('_')[0]  # Assuming dag_id is always "pageviews_..."
        
        url = (
            f"https://dumps.wikimedia.org/other/{obj}/"
            f"{year}/{year}-{month:0>2}/"
            f"{obj}-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )
        output_path = f"{AIRFLOW_HOME}/data/{obj}_{year}{month:0>2}{day:0>2}-{hour:0>2}.gz"

        print(f"Starting: Retrieve data from url: {url}")
        request.urlretrieve(url, output_path)
        print(f"Completed: Retrieve data from url: {url}")

        return {"table_name": obj, "output_path": output_path}

    @task
    def extract_gz(data_info):
        file_path = data_info['output_path']
        bash_command = f"gunzip --force {file_path}"
        os.system(bash_command)
        return {"output_file_name": file_path.replace(".gz", ""), "table_name": data_info['table_name']} 

    @task
    def fetch_pageviews(pagenames, execution_date=None, data_info=None, **context):
        result = dict.fromkeys(pagenames, 0)
        output_file_name = context['ti'].xcom_pull(task_ids='extract_gz', key='return_value')['output_file_name']
        table_name = context['ti'].xcom_pull(task_ids='extract_gz', key='return_value')['table_name']
        hook = PostgresHook(postgres_conn_id=params["postgres_conn_id"])

        with open(output_file_name, "r") as f:
            for line in f:
                domain_code, page_title, view_counts, _ = line.split(" ")
                if domain_code == "en" and page_title in pagenames:
                    result[page_title] = view_counts
        
        # for pagename, pageviewcount in result.items():
        #             insert_statement = (
        #                 f"INSERT INTO {table_name} VALUES ("
        #                 f"'{pagename}', {pageviewcount}, '{execution_date}'"
        #                 ");"
        #             )
        #             print(f"Starting: Executed SQL statement: {insert_statement}")
        #             hook.run(insert_statement)
        #             print(f"Completed: Executed SQL statement: {insert_statement}")
        

        return {"table_name": table_name, "stats": result}

    @task
    def create_table_in_postgres(sql_file_path, data_info):
        try:
            hook = PostgresHook(postgres_conn_id=params["postgres_conn_id"])
            table_name = data_info['table_name']

            with open(sql_file_path, "r") as sql_file:
                sql_statement = sql_file.read()

            # Validate SQL statement
            if not sql_statement:
                raise ValueError("SQL statement cannot be empty.")

            # Execute the statement
            rendered_sql = sql_statement.format(table_name=table_name)
            if rendered_sql.strip(" \n\t").startswith("SELECT"):
                result = hook.get_pandas_df(rendered_sql)
                print(f"Completed: Executed SQL statement: {rendered_sql}")
                return result
            else:
                hook.run(rendered_sql)
                print(f"Completed: Executed SQL statement: {rendered_sql}")
        except ValueError as e:
            print(f"Error: Invalid SQL statement: {e}")
        except Exception as e:
            print(f"Error: Executing SQL statement: {e}")
            raise e
        
    @task.branch
    def filter_pageviews(data_info, execution_date, **context):
        result = data_info['stats']
        table_name = data_info['table_name']
        resource_file_path = context["params"]["sql_file_path"]
        check_pageviews = False

        with open(f"{resource_file_path}/insert_statement.sql", "w") as f:
            for pagename, pageviewcount in result.items():
                if int(pageviewcount) > 300:
                    f.write(
                        f"INSERT INTO {table_name} VALUES ("
                        f"'{pagename}', {pageviewcount}, '{execution_date}'"
                        ");\n"
                    )
                    check_pageviews = True
        
        if check_pageviews:
            return "insert_data_to_postgres"
        else:
            return "end_task"
    
    @task
    def insert_data_to_postgres(data_info=None, **context):
        hook = PostgresHook(postgres_conn_id=params["postgres_conn_id"])
        resource_file_path = context["params"]["sql_file_path"]

        try:
            with open(f"{resource_file_path}/insert_statement.sql", 'r') as f:
                sql_commands = f.read()
            
            hook.run(sql_commands)
            print(f"Successfully execute SQL commands from: {resource_file_path}/insert_statement.sql")
        except FileNotFoundError:
            print(f"Error: SQL file not found at {resource_file_path}/insert_statement.sql")
            raise
        except Exception as e:
            print(f"An error occurred while executing SQL from file: {e}")
            raise
    
    @task
    def end_task():
        print("End task without insert to DB")


    data_info = get_data(
        year="{{ data_interval_start.year }}",
        month="{{ data_interval_start.month }}",
        day="{{ data_interval_start.day }}",
        hour="{{ data_interval_start.hour }}",
    )

    extracted_file = extract_gz(data_info)

    create_table = create_table_in_postgres(
        sql_file_path=params["sql_file_path"] + "/create_table_statement.sql",
        data_info=extracted_file,
    )

    fetch_pageview = fetch_pageviews(
        pagenames={
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook",
        },
        execution_date="{{ ds }}",
        data_info=create_table,
    )

    filter_condtion = filter_pageviews(
        data_info=fetch_pageview,
        execution_date="{{ ds }}"
    )
    insert_data_db = insert_data_to_postgres()
    end_task_instance = end_task()

    filter_condtion >> insert_data_db
    filter_condtion >> end_task_instance


pageviews_taskflow_example_dag()