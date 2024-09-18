import logging
import threading
import pandas as pd
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from .base import BaseETL


class PostgresETL(BaseETL):
    def __init__(
        self,
        source_conn_id,
        target_conn_id,
        source_schema,
        target_schema,
        chunk_size=100000,
        max_threads=5,
    ):
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.chunk_size = chunk_size
        self.max_threads = max_threads

        # Inicializa os hooks
        self.source_hook = PostgresHook(postgres_conn_id=source_conn_id)
        self.target_hook = PostgresHook(postgres_conn_id=target_conn_id)

        # Inicializa os engines
        source_engine = self.source_hook.get_sqlalchemy_engine()
        target_engine = self.target_hook.get_sqlalchemy_engine()

        super().__init__(source_engine, target_engine, chunk_size, max_threads)

    def clone_table_incremental(self, table_name_source, table_name_target, key_column):
        logging.info(f"Iniciando clonagem incremental da tabela {table_name_source}")

        try:
            # Verifica se existe um schema no banco de destino
            with self.target_engine.connect() as conn:
                self.check_schema(self.target_schema, self.target_engine)
                self.verify_and_create_table(self.target_schema, table_name_source, table_name_target, self.target_engine)

            # Obtém o último valor da chave no banco de destino
            with self.target_engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT MAX({key_column}) FROM {table_name_target};")
                )
                last_key = result.scalar() or 0
                logging.info(f"Último valor da chave no destino: {last_key}")

            # Conta o total de registros a serem transferidos
            with self.source_engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM {table_name_source} WHERE {key_column} > :last_key"
                    ),
                    {"last_key": last_key},
                )
                total_rows = result.scalar()
                logging.info(f"Total de registros a serem transferidos: {total_rows}")

            if total_rows == 0:
                logging.info("Nenhum novo registro para transferir.")
                return

            # Função para processar um chunk de dados
            def process_chunk(offset):
                try:
                    with self.source_engine.connect() as source_conn, self.target_engine.connect() as target_conn:
                        query = text(
                            f"""
                            SELECT * FROM {table_name_source}
                            WHERE {key_column} > :last_key
                            ORDER BY {key_column}
                            LIMIT :limit OFFSET :offset
                            """
                        )
                        df_chunk = pd.read_sql_query(
                            query,
                            source_conn,
                            params={
                                "last_key": last_key,
                                "limit": self.chunk_size,
                                "offset": offset,
                            },
                        )
                        if not df_chunk.empty:
                            df_chunk.to_sql(
                                table_name_target,
                                target_conn,
                                if_exists="append",
                                index=False,
                                method="multi",
                            )
                            logging.info(
                                f"Chunk com offset {offset} processado com sucesso"
                            )
                        else:
                            logging.info(
                                f"Nenhum dado encontrado para o chunk com offset {offset}"
                            )
                except Exception as e:
                    logging.error(
                        f"Erro ao processar o chunk com offset {offset}", exc_info=True
                    )
                    raise e

            # Calcula o número de chunks
            num_chunks = (total_rows // self.chunk_size) + 1

            # Lista para controlar as threads
            threads = []
            for i in range(num_chunks):
                offset = i * self.chunk_size
                t = threading.Thread(target=process_chunk, args=(offset,))
                threads.append(t)
                t.start()

                # Limita o número de threads simultâneas
                if len(threads) >= self.max_threads:
                    for t in threads:
                        t.join()
                    threads = []

            # Aguarda as threads restantes
            for t in threads:
                t.join()

            logging.info(
                f"Clonagem incremental da tabela {table_name_source} concluída com sucesso"
            )

        except Exception as e:
            logging.error("Erro ao clonar a tabela incrementalmente", exc_info=True)
            raise e

    def clone_table_replace(self, table_name_source, table_name_target):
        logging.info(f"Iniciando clonagem completa da tabela {table_name_source}")

        try:
            # Lê a tabela de origem em chunks
            chunks = pd.read_sql_table(
                table_name_source,
                self.source_engine,
                chunksize=self.chunk_size,
            )

            # Remove a tabela de destino se existir
            with self.target_engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name_target} CASCADE"))
                logging.info(f"Tabela {table_name_target} removida no destino")

            # Escreve os chunks na tabela de destino
            for idx, df_chunk in enumerate(chunks):
                df_chunk.to_sql(
                    table_name_target,
                    self.target_engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                logging.info(f"Chunk {idx} escrito com sucesso")

            logging.info(
                f"Clonagem completa da tabela {table_name_source} concluída com sucesso"
            )

        except Exception as e:
            logging.error("Erro ao clonar a tabela completamente", exc_info=True)
            raise e

    # Você pode adicionar métodos específicos para Postgres aqui
