from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


SQL_CODE_PATH = '/opt/airflow/sql_codes'

with DAG(
    dag_id='setup_pagila_db',
    start_date=datetime(2026, 4, 12),
    schedule=None,
    catchup=False,
    template_searchpath = [SQL_CODE_PATH],
    tags=['pagila', 'setup'],
) as dag:
    
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
        sql='pagila-schema.sql'
    )
    
    populate_data = SQLExecuteQueryOperator(
        task_id='populate_data',
        conn_id='postgres_pagila_conn',
        sql='pagila-data.sql'
    )
    
    
    drop_schema >> create_schema >> populate_data