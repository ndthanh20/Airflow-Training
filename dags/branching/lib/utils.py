import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
import json


def fetch_and_store_api_data(schema_name, target_table_name, target_fields, **context):
    postgres_hook = PostgresHook(postgres_conn_id=context["params"]["postgres_conn_id"])
    http_hook = HttpHook(http_conn_id=context["params"]["http_conn_id"], method="GET")
    all_data = []
    base_endpoint = f"/api/{target_table_name}"
    try:
        # Get the number of pages
        response = http_hook.run(endpoint=base_endpoint)
        response.raise_for_status()
        data = response.json()
        number_pages = data.get("info", {}).get("pages", 1)

        for page in range(1, number_pages + 1):
            endpoint = f"{base_endpoint}?page={page}"
            print(
                f"Fetching data from page {page} of {target_table_name} from {endpoint}"
            )
            response = http_hook.run(endpoint=endpoint)
            response.raise_for_status()
            page_data = response.json()
            print(
                f"Successfully retrieved data for page {page}: {page_data.get('info')}"
            )
            all_data.extend(
                page_data.get(
                    "results",
                )
            )

        records_to_insert = []
        for item in all_data:
            row_values = tuple(item.get(field) for field in target_fields)
            records_to_insert.append(row_values)

        if records_to_insert:
            postgres_hook.insert_rows(
                table=f"{schema_name}.{target_table_name}",
                rows=records_to_insert,
                target_fields=target_fields,
                commit_every=1000,
            )
            print(
                f"Successfully stored {len(all_data)} records in {target_table_name}."
            )

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        raise
    except Exception as e:
        print(f"Error storing data in PostgreSQL ({target_table_name}): {e}")
        raise


def check_data_exists(schema_name, target_table_name, **context):
    postgres_hook = PostgresHook(postgres_conn_id=context["params"]["postgres_conn_id"])
    sql = f"SELECT COUNT(*) FROM {schema_name}.{target_table_name};"
    print(f"Checking data existence in {schema_name}.{target_table_name}")
    records = postgres_hook.get_records(sql)
    if records and records[0][0] > 0:
        print(f"Data exists in {schema_name}.{target_table_name}.")
        return "data_exists"
    else:
        print(f"No data found in {schema_name}.{target_table_name}.")
        return "no_data_found"
