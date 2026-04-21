BASE_PATH = "/opt/airflow/shared"

DB_CONFIGS = {
    "sakila": {
        "schema_url": "https://raw.githubusercontent.com/jOOQ/sakila/master/mysql-sakila-db/sakila-schema.sql",
        "data_url": "https://raw.githubusercontent.com/jOOQ/sakila/master/mysql-sakila-db/sakila-data.sql",
        "schema_file": "sakila-schema.sql",
        "data_file": "sakila-data.sql",
        "conn_id": "mysql_sakila_conn",
    },
    "pagila": {
        "schema_url": "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql",
        "data_url": "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql",
        "schema_file": "pagila-schema.sql",
        "data_file": "pagila-data.sql",
        "conn_id": "postgres_pagila_conn",
    },
}