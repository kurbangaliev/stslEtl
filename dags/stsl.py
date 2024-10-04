from multiprocessing.spawn import prepare

import pendulum
import etl
import sql_scripts
import logging
from datetime import timedelta
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 10, 4, tz="UTC"),
    catchup=False,
    template_searchpath='data/sql/',
    tags=["atms", "version 1.47"]
)

def stsl():
    clear_data_task = PythonOperator(
        task_id="clear_data",
        python_callable=etl.Loader.clear_data
    )

    drop_data_tables = PostgresOperator(
        task_id = "drop_tables",
        postgres_conn_id='postgres_conn',
        sql = sql_scripts.sql_drop_tables
    )

    create_data_tables = PostgresOperator(
        task_id = "create_tables",
        postgres_conn_id='postgres_conn',
        sql = sql_scripts.sql_create_tables
    )

    load_departure_documents_task = PythonOperator(
        task_id="load_departure_documents",
        python_callable=etl.Loader.get_departure_documents
    )

    load_giving_documents_task = PythonOperator(
        task_id="load_giving_documents",
        python_callable=etl.Loader.get_giving_documents
    )

    fill_total_documents = PythonOperator(
        task_id="fill_total_documents",
        python_callable=etl.Loader.fill_total_documents
    )

    create_dims = PythonOperator(
        task_id="create_dims",
        python_callable=etl.Loader.create_dims
    )

    fill_table_empty_types = PostgresOperator(
        task_id = "fill_table_empty_types",
        postgres_conn_id='postgres_conn',
        sql = sql_scripts.sql_fill_empty_types
    )

    create_facts_departure = PythonOperator(
        task_id="create_facts_departure",
        python_callable=etl.Loader.create_facts_departures
    )

    load_dims_task = PostgresOperator(
        task_id="load_dims_to_database",
        postgres_conn_id='postgres_conn',
        sql='dims.sql'
    )

    load_departures_task = PostgresOperator(
        task_id="load_departures_to_database",
        postgres_conn_id='postgres_conn',
        sql='departures.sql'
    )

    clear_data_task >> drop_data_tables >> create_data_tables >> [load_departure_documents_task, load_giving_documents_task] >> fill_total_documents >> create_dims >> fill_table_empty_types >> create_facts_departure >> load_dims_task >> load_departures_task

stsl()