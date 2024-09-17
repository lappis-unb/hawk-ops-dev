from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging
from typing import Any

# Configurando o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_variable(data: Any):
    """
    Função que aceita qualquer dado e imprime usando o logging.
    
    Args:
        data (Any): Pode ser qualquer tipo de dado (string, número, lista, dicionário, etc.)
    """
    logging.info(f'Valor da variável: {data}')

default_args = {
    'owner': 'Eric Silveira',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1
}

CONN_DB_RASA = "conn_db_rasa"

@dag(
    dag_id='rasa_data_ingestion_dag',
    start_date=datetime(2024, 9, 14),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
)
def rasa_data_ingestion_dag():
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    create_pet_table = SQLExecuteQueryOperator(
        task_id="create_pet_table",
        conn_id=CONN_DB_RASA,
        sql="""
            CREATE TABLE IF NOT EXISTS public.pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )
    

    start >> create_pet_table >> end

# Instanciar a DAG
instancia_dag_ingestion = rasa_data_ingestion_dag()
