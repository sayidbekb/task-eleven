from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests

BASE_PATH = "/opt/airflow/shared"

SCHEMA_URL = "https://raw.githubusercontent.com/jOOQ/sakila/master/mysql-sakila-db/sakila-schema.sql"
DATA_URL = "https://raw.githubusercontent.com/jOOQ/sakila/master/mysql-sakila-db/sakila-data.sql"


def download_file(url, path):
    r = requests.get(url)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)


with DAG(
    dag_id='setup_sakila_db',
    start_date=datetime(2026, 4, 12),
    schedule=None,
    catchup=False,
    tags=['sakila', 'setup'],
) as dag:

    download_schema = PythonOperator(
        task_id="download_schema",
        python_callable=download_file,
        op_kwargs={
            "url": SCHEMA_URL,
            "path": f"{BASE_PATH}/sakila-schema.sql"
        }
    )

    download_data = PythonOperator(
        task_id="download_data",
        python_callable=download_file,
        op_kwargs={
            "url": DATA_URL,
            "path": f"{BASE_PATH}/sakila-data.sql"
        }
    )

    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id='mysql_sakila_conn',  # FIXED NAME
        sql=f"{BASE_PATH}/sakila-schema.sql"
    )

    populate_data = SQLExecuteQueryOperator(
        task_id='populate_data',
        conn_id='mysql_sakila_conn',
        sql=f"{BASE_PATH}/sakila-data.sql"
    )

    [download_schema, download_data] >> create_schema >> populate_data