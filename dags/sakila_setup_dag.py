from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


SQL_CODE_PATH = '/opt/airflow/sql_codes'

with DAG(
    dag_id='setup_sakila_db',
    start_date=datetime(2026, 4, 12),
    schedule=None,
    catchup=False,
    template_searchpath = [SQL_CODE_PATH],
    tags=['sakila', 'setup'],
) as dag:
    
    
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id='mysql_pagila_conn',
        sql='sakila-schema.sql'
    )
    
    populate_data = SQLExecuteQueryOperator(
        task_id='populate_data',
        conn_id='mysql_pagila_conn',
        sql='sakila-data.sql'
    )
    
    
    create_schema >> populate_data