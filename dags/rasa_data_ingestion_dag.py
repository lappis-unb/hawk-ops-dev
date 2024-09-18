from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging
from typing import Any

CONN_DB_RASA = "conn_db_rasa"
CONN_DB_DESTINY = "conn_db_destiny"
TABLE_NAME = "events"
SCHEMA = "public"

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



def verify_if_exists_table_and_schema(conn_destiny):

    cursor_destiny = conn_destiny.cursor()

    query_exists_schema = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"
    cursor_destiny.execute(query_exists_schema)
    conn_destiny.commit()

    query_exists_table = f"""
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = {SCHEMA} 
                AND table_name = '{TABLE_NAME}';
        """
    cursor_destiny.execute(query_exists_table)
    result_query_exists_table = cursor_destiny.fetchone()

    if result_query_exists_table:
        _log(f"A tabela '{TABLE_NAME}' existe.")
        return True
    else:
        _log(f"A tabela '{TABLE_NAME}' não existe.")
        return False


    
def source_columns_and_types_table(cursor_source):
    query_columns_table = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{TABLE_NAME}'
            ORDER BY ordinal_position;
        """
    cursor_source.execute(query_columns_table)
    result_query_columns_table = cursor_source.fetchall()

    return result_query_columns_table


def create_table(conn_destiny, columns, types):

    try:
        values_table = ''

        for names, tipes_columns in zip(columns, types):
            values_table = values_table + f"{names} {tipes_columns}, "

        values_table = values_table[:-2]

        query_create_table = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({values_table});"

        cursor_destiny = conn_destiny.cursor()

        cursor_destiny.execute(query_create_table)
        conn_destiny.commit()
        return
    except Exception as e:
        logging.error(f"Ocorreu um erro: {str(e)}")

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

    postgres_source_hook = PostgresHook(postgres_conn_id=CONN_DB_RASA)
    conn_source = postgres_source_hook.get_conn()
    cursor_source = conn_source.cursor()
        
    postgres_destino_hook = PostgresHook(postgres_conn_id=CONN_DB_ORIGEM)
    conn_destino = postgres_destino_hook.get_conn()

    @task
    def verify_and_create_table():
        try:

            check_table = verify_if_exists_table_and_schema(conn_destiny)

            result_query_columns_and_types_table = source_columns_and_types_table(cursor_source)

            _log(result_query_columns_and_types_table)
            
            columns = [item[0] for item in result_query_columns_and_types_table]
            types = [item[1] for item in result_query_columns_and_types_table]

            if check_table == False:
                create_table(conn_destiny, columns, types)
            else:
                return

        except Exception as e:
            logging.error(f"Ocorreu um erro: {str(e)}")

    @task
    def write_data():
        _log('oi')


    start >> verify_and_create_table() >> write_data() >> end

# Instanciar a DAG
instancia_dag_ingestion = rasa_data_ingestion_dag()