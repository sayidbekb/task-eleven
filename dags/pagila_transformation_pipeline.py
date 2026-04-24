from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='pagila_transformation_pipeline',
    start_date=datetime(2026, 4, 23),
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_dbt = BashOperator(
        task_id='dbt_run_all_clients',
        bash_command="""
        cd /path/to/dbt_project &&
        dbt run --models staging.* intermediate.* marts.* --target prod
        """
    )