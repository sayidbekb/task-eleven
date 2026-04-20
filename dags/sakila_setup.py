from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os

from config.db_init_config import BASE_PATH, DB_CONFIGS
from tasks.download import download_file

cfg = DB_CONFIGS['sakila']

with DAG(
    dag_id='setup_sakila_db',
    start_date=datetime(2026, 4, 12),
    schedule=None,
    catchup=False,
    tags=['sakila', 'setup'],
) as dag:

    schema_path = os.path.join(BASE_PATH, cfg["schema_file"])
    data_path = os.path.join(BASE_PATH, cfg["data_file"])

    download_schema = download_file(cfg["schema_url"], schema_path)
    download_data = download_file(cfg["data_url"], data_path)
    
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id=cfg['conn_id'],
        sql=schema_path,
    )

    populate_data = SQLExecuteQueryOperator(
        task_id='populate_data',
        conn_id=cfg['conn_id'],
        sql=data_path
    )

    [download_schema, download_data] >> create_schema >> populate_data