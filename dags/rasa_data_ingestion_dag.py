from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging
from typing import Any

# Configurando o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _log(obj: Any) -> None:
    """
    Logs the obj in a fancy look.

    Returns:
    -------
    None
    """

    info2 = f"="
    logging.info(info2)
    logging.info("==============START LOGGING===============")
    logging.info(obj)
    logging.info("==============END LOGGING===============")
    logging.info(info2)


def verify_if_exists_table(cursor_destino):

    query_exists_table = f"""
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{TABLE_NAME}';
        """
    cursor_destino.execute(query_exists_table)
    result_query_exists_table = cursor_destino.fetchone()

    if result_query_exists_table:
        _log(f"A tabela '{TABLE_NAME}' existe.")
        return True
    else:
        _log(f"A tabela '{TABLE_NAME}' não existe.")
        return False

CONN_DB_RASA = "conn_db_rasa"
CONN_DB_ORIGEM = "conn_db_origem"
TABLE_NAME = "events"

default_args = {
    'owner': 'Eric Silveira',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1
}

@dag(
    dag_id='rasa_data_ingestion_dag',
    start_date=datetime(2024, 9, 17),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
)
def rasa_data_ingestion_dag():
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task
    def verify_and_create_table():
        try:
            postgres_source_hook = PostgresHook(postgres_conn_id=CONN_DB_RASA)
            conn_source = postgres_source_hook.get_conn()
            cursor_source = conn_source.cursor()
                
            postgres_destino_hook = PostgresHook(postgres_conn_id=CONN_DB_ORIGEM)
            conn_destino = postgres_destino_hook.get_conn()
            cursor_destino = conn_destino.cursor()

            check_table = verify_if_exists_table(cursor_destino)


        except Exception as e:
            logging.error(f"Ocorreu um erro: {str(e)}")


    start >> verify_and_create_table() >> end

# Instanciar a DAG
instancia_dag_ingestion = rasa_data_ingestion_dag()
