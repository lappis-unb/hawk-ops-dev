import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.database_etl.base import SourceTables
from scripts.database_etl import PostgresETL
from scripts.database_etl.utils import setup_logging
from scripts.transformations import transformation_ej_profiles_profile

default_args = {
    "owner": "Eric Silveira",
    "depends_on_past": False,
    "email": ["silveirames@hotmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="clone_and_transform_ej_profiles_profile",
    default_args=default_args,
    description="DAG para executar operações ETL da tabela ej_profiles_profile",
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

        profiles = SourceTables(name="profiles", key_column="user_id", foreign_key="id")
        user = SourceTables(name="users", key_column="id", foreign_key="id")
        source_tables.append(profiles)
        source_tables.append(user)

        target_table = "users_with_profiles_target"
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


dag = clone_rasa_profiles()
