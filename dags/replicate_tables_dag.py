from airflow import DAG
from datetime import datetime
from datetime import timedelta

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="task3_airbyte_replication",
    default_args=default_args,
    description="Replicate Pagila and Sakila to Snowflake using Airbyte",
    schedule=None,
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=["task3", "airbyte", "snowflake"],
) as dag:

    
    # Pagila → Snowflake
    
    sync_pagila = AirbyteTriggerSyncOperator(
        task_id="sync_pagila",
        airbyte_conn_id="airbyte_conn",
        connection_id="5c71412a-dd4d-4a43-bf4d-24ffefa9f415",
        asynchronous=False,
        timeout=3600,
        wait_seconds=10,
    )

   
    # Sakila → Snowflake
    
    sync_sakila = AirbyteTriggerSyncOperator(
        task_id="sync_sakila",
        airbyte_conn_id="airbyte_conn",
        connection_id="be5ccd3d-5e4b-4dc8-a739-fc8699b981b4",
        asynchronous=False,
        timeout=3600,
        wait_seconds=10,
    )

    # Run them
    sync_pagila >> sync_sakila