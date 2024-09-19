import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.database_etl import PostgresETL
from scripts.database_etl.utils import setup_logging

default_args = {
    "owner": "Giovani Giampauli / Eric Silveira",
    "depends_on_past": False,
    "email": ["giovanni.acg@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="clone_rasa_events",
    default_args=default_args,
    description="DAG para executar operações ETL da tabela de eventos do Rasa",
    schedule_interval="@daily",
    start_date=datetime(2023, 10, 1),
    catchup=False,
)
def clone_rasa_events():

    @task()
    def clone():
        logging.info("Configurando ETL")
        setup_logging()

        source_conn_id = "rasa_db_airflow"
        target_conn_id = "rasa_db_airflow"
        source_schema = "public"
        target_schema = "public"

        source_tables = ["table1", "table2"]
        target_table = "merged_table"
        key_column = "id"

        etl = PostgresETL(
            source_conn_id=source_conn_id,
            target_conn_id=target_conn_id,
            source_schema=source_schema,
            target_schema=target_schema,
            chunk_size=50000,
            max_threads=10,
            multithreading=True,
        )

        etl.clone_tables_incremental(source_tables, target_table, key_column)

    clone()


dag = clone_rasa_events()
