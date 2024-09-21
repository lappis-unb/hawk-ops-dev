import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.database_etl.base import SourceTables
from scripts.database_etl import PostgresETL
from scripts.database_etl.utils import setup_logging

default_args = {
    "owner": "Will Bernardo",
    "depends_on_past": False,
    "email": ["williambernardo838@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="clone_ej_conversations_comment",
    default_args=default_args,
    description="DAG para executar operações ETL da tabela de Ej Conversations Comment",
    schedule_interval="@daily",
    start_date=datetime(2023, 10, 19),
    catchup=False,
)
def clone_ej_conversations_comment():

    @task()
    def clone():

        logging.info("Configurando ETL")
        setup_logging()

        source_conn_id = "rasa_db_airflow"
        target_conn_id = "rasa_db_airflow"
        source_schema = "public"
        target_schema = "public"

        source_tables: SourceTables = []

        ej_conversations_comment = SourceTables("ej_conversations_comment", "", "")
        source_tables.append(ej_conversations_comment)

        target_table = "conversations_comment_target"

        etl = PostgresETL(
            source_conn_id=source_conn_id,
            target_conn_id=target_conn_id,
            source_schema=source_schema,
            target_schema=target_schema,
            chunk_size=50000,
            max_threads=10,
            multithreading=True,
        )

        etl.clone_tables_replace(source_tables, target_table)
        

    clone()


dag = clone_ej_conversations_comment()
