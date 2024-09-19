from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from typing import Any
from scripts.database_etl import PostgresETL

CONN_DB_RASA = "conn_db_rasa"
CONN_DB_DESTINY = "conn_db_destiny"
TABLE_NAME = "events"

default_args = {
    'owner': 'Arthur Alves',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1
}

@dag(
    dag_id='index_sync_db_dag',
    start_date=datetime(2024, 9, 17),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
)
def index_sync_db_dag():
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task
    def index_sync_and_insert_db():
        try:
            etl = PostgresETL(source_conn_id=CONN_DB_RASA, target_conn_id=CONN_DB_DESTINY)
            etl.clone_table_incremental(table_name_source=TABLE_NAME, table_name_target=TABLE_NAME, key_column="id")
        
        except Exception as e:
            logging.error(f"Ocorreu um erro durante a sincronização: {str(e)}")

    start >> index_sync_and_insert_db() >> end

# Instancia a DAG
instancia_dag = index_sync_db_dag()
