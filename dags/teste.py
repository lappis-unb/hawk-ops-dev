from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_data():
    # Conexão com a base de dados de origem e destino
    source_hook = PostgresHook(postgres_conn_id='source_postgres')  # Conexão com a origem
    target_hook = PostgresHook(postgres_conn_id='target_postgres')  # Conexão com o destino

    # Passo 1: Selecionar dados da origem com hash gerado
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute("""
        SELECT id, MD5(ROW(id, sender_id, type_name, action_name, data)::TEXT) AS row_hash
        FROM events;
    """)
    source_data = source_cursor.fetchall()

    # Passo 2: Selecionar dados do destino com hash gerado
    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()
    target_cursor.execute("""
        SELECT id, MD5(ROW(id, sender_id, type_name, action_name, data)::TEXT) AS row_hash
        FROM events;
    """)
    target_data = target_cursor.fetchall()

    # Converter os resultados em dicionário para fácil comparação
    source_dict = {row[0]: row[1] for row in source_data}  # id: hash
    target_dict = {row[0]: row[1] for row in target_data}  # id: hash

    # Passo 3: Identificar registros novos ou modificados
    new_or_updated_rows = []
    for id, hash_value in source_dict.items():
        if id not in target_dict or target_dict[id] != hash_value:
            new_or_updated_rows.append(id)

    # Passo 4: Inserir ou atualizar registros modificados no destino
    if new_or_updated_rows:
        ids_tuple = tuple(new_or_updated_rows)
        source_cursor.execute(f"SELECT * FROM events WHERE id IN {ids_tuple}")
        rows_to_insert = source_cursor.fetchall()

        for row in rows_to_insert:
            # Inserir ou atualizar no destino
            insert_query = """
            INSERT INTO events (id, sender_id, type_name, action_name, data)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET sender_id = EXCLUDED.sender_id,
                type_name = EXCLUDED.type_name,
                action_name = EXCLUDED.action_name,
                data = EXCLUDED.data;
            """
            target_cursor.execute(insert_query, row)

    # Commit para salvar as mudanças no destino
    target_conn.commit()
    target_cursor.close()
    source_cursor.close()

with DAG('incremental_ingestion_dag', default_args=default_args, schedule_interval='@daily') as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data
    )
 