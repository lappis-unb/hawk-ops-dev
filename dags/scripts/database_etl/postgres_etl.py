import logging
import threading
import pandas as pd
from sqlalchemy import text, MetaData, Table, inspect
from sqlalchemy.exc import NoSuchTableError
from airflow.providers.postgres.hooks.postgres import PostgresHook
from .base import BaseETL, SourceTables


class PostgresETL(BaseETL):
    def __init__(
        self,
        source_conn_id,
        target_conn_id,
        source_schema,
        target_schema,
        chunk_size=100000,
        max_threads=5,
        multithreading=True,
        transform_func=None,
    ):
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.source_schema = source_schema
        self.target_schema = target_schema

        self.source_hook = PostgresHook(postgres_conn_id=source_conn_id)
        self.target_hook = PostgresHook(postgres_conn_id=target_conn_id)

        source_engine = self.source_hook.get_sqlalchemy_engine()
        target_engine = self.target_hook.get_sqlalchemy_engine()

        super().__init__(
            source_engine,
            target_engine,
            chunk_size,
            max_threads,
            multithreading,
            transform_func,
        )

    def check_schema(self, schema_name, engine):
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema_name"
                ),
                {"schema_name": schema_name},
            )
            if not result.fetchone():
                conn.execute(text(f"CREATE SCHEMA {schema_name}"))
                logging.info(f"Schema {schema_name} criado.")

    def check_table(self, target_table, transformed_df):
        inspector = inspect(self.target_engine)
        try:
            if inspector.has_table(target_table, schema=self.target_schema):
                metadata = MetaData()
                table = Table(
                    target_table,
                    metadata,
                    autoload_with=self.target_engine,
                    schema=self.target_schema,
                )
                target_columns = {col.name: col.type for col in table.columns}
                df_columns = transformed_df.dtypes.to_dict()

                dtype_mapping = {
                    "int64": "BIGINT",
                    "float64": "FLOAT",
                    "object": "TEXT",
                    "bool": "BOOLEAN",
                    "datetime64[ns]": "TIMESTAMP",
                }

                df_columns_str = {
                    col: dtype_mapping.get(str(dtype), "VARCHAR")
                    for col, dtype in df_columns.items()
                }
                target_columns_str = {
                    col: str(coltype).upper().split("(")[0]
                    for col, coltype in target_columns.items()
                }

                if df_columns_str != target_columns_str:
                    raise ValueError(
                        f"Incompatibilidade de esquema entre o DataFrame transformado e a tabela de destino {target_table}:\nDataFrame:\t\t {df_columns_str}\nTabela destino:\t\t: {target_columns_str}"
                    )
                else:
                    logging.info(
                        f"Esquema da tabela de destino {target_table} é compatível."
                    )
            else:
                logging.info(f"Tabela de destino {target_table} não existe. Criando-a.")
                transformed_df.head(0).to_sql(
                    target_table,
                    self.target_engine,
                    schema=self.target_schema,
                    if_exists="fail",
                    index=False,
                )
                logging.info(
                    f"Tabela {target_table} criada no schema {self.target_schema}."
                )
        except Exception as e:
            logging.error(
                f"Erro ao verificar ou criar a tabela {target_table}", exc_info=True
            )
            raise e

    def process_chunk(
        self, offset, last_keys_target, source_tables, target_table, key_column
    ):
        try:
            dfs = []
            with self.source_engine.connect() as source_conn:
                for idx, table in enumerate(source_tables):
                    table_name = table.name
                    query = text(
                        f"""
                        SELECT * FROM {table_name}
                        WHERE {key_column} > :last_key
                        ORDER BY {key_column}
                        LIMIT :limit OFFSET :offset
                        """
                    )
                    df_chunk = pd.read_sql_query(
                        query,
                        source_conn,
                        params={
                            "last_key": last_keys_target[table.foreign_key],
                            "limit": self.chunk_size,
                            "offset": offset,
                        },
                    )
                    logging.info(
                        f"Chunk com offset {offset} da tabela {table_name} lido."
                    )
                    dfs.append(df_chunk)

            transformed_df = self.transform_data(dfs)

            with self.target_engine.connect() as target_conn:
                if not transformed_df.empty:
                    transformed_df.to_sql(
                        target_table,
                        target_conn,
                        schema=self.target_schema,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )
                    logging.info(f"Chunk com offset {offset} processado com sucesso.")
                else:
                    logging.info(
                        f"Nenhum dado para escrever no chunk com offset {offset}."
                    )
        except Exception as e:
            logging.error(f"Erro ao processar chunk com offset {offset}", exc_info=True)
            raise e

    def process_chunk_replace(self, dfs_chunk, target_table, idx, offset):
        try:
            transformed_df = self.transform_data(dfs_chunk)
            if not transformed_df.empty:
                transformed_df.to_sql(
                    target_table,
                    self.target_engine,
                    schema=self.target_schema,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                logging.info(f"Chunk com offset {offset} e índice {idx} escrito com sucesso.")
            else:
                logging.info(f"Nenhum dado para escrever no chunk com offset {offset} e índice {idx}.")
        except Exception as e:
            logging.error(f"Erro ao escrever chunk com offset {offset} e índice {idx}", exc_info=True)
            raise e

    def clone_tables_incremental(
        self, source_tables: SourceTables, target_table, key_column
    ):
        logging.info(f"Iniciando clonagem incremental das tabelas {source_tables}.")

        try:
            self.check_schema(self.target_schema, self.target_engine)

            dfs_sample = []
            with self.source_engine.connect() as source_conn:
                for table in source_tables:
                    table_name = table.name
                    query = text(f"SELECT * FROM {table_name} LIMIT :limit")
                    df_sample = pd.read_sql_query(
                        query, source_conn, params={"limit": 10}
                    )
                    dfs_sample.append(df_sample)

            transformed_df_sample = self.transform_data(dfs_sample)
            self.check_table(target_table, transformed_df_sample)

            with self.target_engine.connect() as target_conn:
                last_key_target = dict()
                for table in source_tables:
                    result = target_conn.execute(
                        text(
                            f"SELECT MAX({table.foreign_key}) FROM {self.target_schema}.{target_table};"
                        )
                    )
                    last_key_target[table.foreign_key] = result.scalar() or 0
                    logging.info(f"Último valor de chave no destino: {last_key_target}")

            total_rows = 0
            with self.source_engine.connect() as source_conn:
                with self.target_engine.connect() as target_conn:
                    for table in source_tables:
                        result = source_conn.execute(
                            text(
                                f"SELECT COUNT(*) FROM {table_name} WHERE {key_column} > :last_key"
                            ),
                            {"last_key": last_key_target[table.foreign_key]},
                        )
                        total_rows_table = result.scalar()
                        total_rows += total_rows_table
                        logging.info(
                            f"Total de registros a serem transferidos de {table_name}: {total_rows_table}"
                        )

            if total_rows == 0:
                logging.info("Nenhum novo registro para transferir.")
                return

            num_chunks = (total_rows // self.chunk_size) + 1

            tasks = []
            for i in range(num_chunks):
                offset = i * self.chunk_size
                tasks.append(
                    {
                        "function": self.process_chunk,
                        "args": (
                            offset,
                            last_key_target,
                            source_tables,
                            target_table,
                            key_column,
                        ),
                    }
                )

            self.execute_tasks(tasks)

            logging.info(
                f"Clonagem incremental das tabelas {source_tables} concluída com sucesso."
            )

        except Exception as e:
            logging.error("Erro ao clonar tabelas incrementalmente.", exc_info=True)
            raise e

    def clone_tables_replace(self, source_tables: SourceTables, target_table):
        logging.info(f"Iniciando clonagem completa das tabelas {source_tables}.")

        try:
            with self.target_engine.connect() as conn:
                conn.execute(
                    text(
                        f"DROP TABLE IF EXISTS {self.target_schema}.{target_table} CASCADE"
                    )
                )
                logging.info(f"Tabela {target_table} removida no destino.")

            dfs_sample = []
            with self.source_engine.connect() as source_conn:
                for table in source_tables:
                    table_name = table.name
                    query = text(f"SELECT * FROM {table_name} LIMIT :limit")
                    df_sample = pd.read_sql_query(
                        query, source_conn, params={"limit": 10}
                    )
                    dfs_sample.append(df_sample)

            transformed_df_sample = self.transform_data(dfs_sample)
            self.check_table(target_table, transformed_df_sample)

            total_rows_per_table = {}
            with self.source_engine.connect() as source_conn:
                for table in source_tables:
                    table_name = table.name
                    result = source_conn.execute(
                        text(f"SELECT COUNT(*) FROM {table_name}")
                    )
                    total_rows = result.scalar()
                    total_rows_per_table[table_name] = total_rows
                    logging.info(f"Total de registros na tabela {table_name}: {total_rows}")

            if sum(total_rows_per_table.values()) == 0:
                logging.info("Nenhum novo registro para transferir.")
                return

            chunk_iters = []
            for table in source_tables:
                table_name = table.name
                chunk_iter = pd.read_sql_table(
                    table_name,
                    self.source_engine,
                    schema=self.source_schema,
                    chunksize=self.chunk_size,
                )
                chunk_iters.append(chunk_iter)

            idx = 0
            offset = 0
            while True:
                dfs_chunk = []
                all_empty = True
                for chunk_iter in chunk_iters:
                    try:
                        df_chunk = next(chunk_iter)
                        dfs_chunk.append(df_chunk)
                        all_empty = False
                    except StopIteration:
                        dfs_chunk.append(pd.DataFrame())

                if all_empty:
                    break

                self.process_chunk_replace(dfs_chunk, target_table, idx, offset)
                idx += 1
                offset += self.chunk_size

            logging.info(
                f"Clonagem completa das tabelas {source_tables} concluída com sucesso."
            )

        except Exception as e:
            logging.error("Erro ao clonar tabelas completamente.", exc_info=True)
            raise e
