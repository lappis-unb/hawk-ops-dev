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

default_args = {
    'owner': 'Eric Silveira',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1
}

CONN_DB_RASA = "conn_db_rasa"

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

    columns_name = SQLExecuteQueryOperator(
        task_id="columns_name",
        conn_id=CONN_DB_RASA,
        return_last=True,
        sql="""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events';
          """,
    )
    
    @task
    def hashes_table(ti=None):
        """
        Extrai o nome de todas as colunas da tabela do banco de origem
        a ser ingerida para realizar uma hash com a agregação dessas colunas.
        """
        query_result = ti.xcom_pull(task_ids="columns_name")


        _log(query_result)

    start >> columns_name >> hashes_table() >> end

# Instanciar a DAG
instancia_dag_ingestion = rasa_data_ingestion_dag()
