import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.database_etl.base import SourceTables
from scripts.database_etl import PostgresETL
from scripts.database_etl.utils import setup_logging

default_args = {
    "owner": "Arthur Alves",
    "depends_on_past": False,
    "email": ["arthuralves1538@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="clone_ej_conversations_conversations",
    default_args=default_args,
    description="DAG para inserir incrementalmente os dados da tabela source na tabela destino",
    schedule_interval="@daily",
    start_date=datetime(2023, 10, 19),
    catchup=False,
)
def clone_rasa_profiles():

    @task()
    def clone():

        logging.info("Configurando ETL")
        setup_logging()

        source_conn_id = "conn_db_rasa"
        target_conn_id = "conn_db_rasa"
        source_schema = "public"
        target_schema = "public"

        source_tables: SourceTables = []

        conversations = SourceTables(name="Conversations", key_column="id", foreign_key="id")
        source_tables.append(conversations)

        target_table = "conversations_target"

        etl = PostgresETL(
            source_conn_id=source_conn_id,
            target_conn_id=target_conn_id,
            source_schema=source_schema,
            target_schema=target_schema,
            chunk_size=50000,
            max_threads=10,
            multithreading=True,
        )

        etl.clone_tables_incremental(source_tables, target_table)

    clone()


dag = clone_rasa_profiles()