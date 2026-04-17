from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests

BASE_PATH = "/opt/airflow/shared"

SCHEMA_URL = "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql"
DATA_URL = "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql"


def download_file(url, path):
    response = requests.get(url)
    response.raise_for_status()
    with open(path, "wb") as f:
        f.write(response.content)


with DAG(
    dag_id='setup_pagila_db',
    start_date=datetime(2026, 4, 12),
    schedule=None,
    catchup=False,
    tags=['pagila', 'setup'],
) as dag:

    download_schema = PythonOperator(
        task_id="download_schema",
        python_callable=download_file,
        op_kwargs={
            "url": SCHEMA_URL,
            "path": f"{BASE_PATH}/pagila-schema.sql"
        }
    )

    download_data = PythonOperator(
        task_id="download_data",
        python_callable=download_file,
        op_kwargs={
            "url": DATA_URL,
            "path": f"{BASE_PATH}/pagila-data.sql"
        }
    )

    drop_schema = SQLExecuteQueryOperator(
        task_id='drop_schema',
        conn_id='postgres_pagila_conn',
        sql="""
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            GRANT ALL ON SCHEMA public TO pagila_admin;
            GRANT ALL ON SCHEMA public to public;
        """
    )

    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id='postgres_pagila_conn',
        sql=f"{BASE_PATH}/pagila-schema.sql"
    )

    populate_data = SQLExecuteQueryOperator(
        task_id='populate_data',
        conn_id='postgres_pagila_conn',
        sql=f"{BASE_PATH}/pagila-data.sql"
    )

    [download_schema, download_data] >> drop_schema >> create_schema >> populate_data