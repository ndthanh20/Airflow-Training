from datetime import datetime
import airflow

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": airflow.utils.dates.timedelta(minutes=5),
}

params = {
    "postgres_conn_id": "my_postgres",
    "http_conn_id": "my_http_connection",
}

ingest_config = {
    "source": "API",
    "tables": [
        {
            "target_table_name": "character",
            "source_table_name": "character",
            "target_fields": ["id", "name", "status", "species", "type", "gender"],
        },
        {
            "target_table_name": "location",
            "source_table_name": "location",
            "target_fields": ["id", "name", "type", "dimension", "url", "created"],
        },
    ],
}
