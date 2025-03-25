CREATE TABLE IF NOT EXISTS "{{ ti.xcom_pull(task_ids='get_data', key='table_name') }}" (
    pagename VARCHAR(50) NOT NULL,
    pageviewcount INT NOT NULL,
    datetime TIMESTAMP NOT NULL
);