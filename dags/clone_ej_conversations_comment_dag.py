import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.database_etl.base import SourceTables
from scripts.database_etl import PostgresETL, Mapping
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

SOURCE_CONN_ID = "rasa_db_airflow"
TARGET_CONN_ID = "rasa_db_airflow"
SOURCE_SCHEMA = "public"
TARGET_SCHEMA = "public"
TARGET_TABLE = "conversations_comment_target"


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

        source_tables: SourceTables = []

        ej_conversations_comment = SourceTables("users", "id", "id")
        source_tables.append(ej_conversations_comment)

        etl = PostgresETL(
            source_conn_id=SOURCE_CONN_ID,
            target_conn_id=TARGET_CONN_ID,
            source_schema=SOURCE_SCHEMA,
            target_schema=TARGET_SCHEMA,
            chunk_size=50000,
            max_threads=10,
            multithreading=True,
        )

        etl.clone_tables_replace(source_tables, TARGET_TABLE)
    
    @task
    def mapping():


        relations = Mapping(
             target_conn_id = TARGET_CONN_ID,
             target_schema = TARGET_SCHEMA,
             target_table = TARGET_TABLE,
        )

        relations.mapping_relations()

    clone() >> mapping()


dag = clone_ej_conversations_comment()
