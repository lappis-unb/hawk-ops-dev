from dataclasses import dataclass
import logging
import threading
from abc import ABC, abstractmethod
import pandas as pd
from sqlalchemy import text

@dataclass
class SourceTables:
    name: str
    key_column: str
    foreign_key: str


class BaseETL(ABC):
    def __init__(
        self,
        source_engine,
        target_engine,
        chunk_size=100000,
        max_threads=5,
        multithreading=True,
        transform_func=None,
        custom_query=None,  # Adiciona a query customizada ao construtor
    ):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.chunk_size = chunk_size
        self.max_threads = max_threads
        self.multithreading = multithreading
        self.transform_func = transform_func
        self.custom_query = custom_query

    def transform_data(self, dfs):
        if self.transform_func:
            return self.transform_func(dfs)
        else:
            # Default behavior is to return the first DataFrame unmodified
            return dfs[0]

    def execute_tasks(self, tasks):
        if self.multithreading:
            # Use multithreading
            threads = []
            for task in tasks:
                t = threading.Thread(target=task["function"], args=task["args"])
                threads.append(t)
                t.start()

                # Limit the number of simultaneous threads
                if len(threads) >= self.max_threads:
                    for t in threads:
                        t.join()
                    threads = []
            # Wait for remaining threads
            for t in threads:
                t.join()
        else:
            # Execute tasks sequentially
            for task in tasks:
                task["function"](*task["args"])

    def get_sample_data(self, source_conn, source_tables, limit=10):
        """
        Obtém uma amostra dos dados usando a query customizada ou as tabelas de origem.

        :param source_conn: Conexão com a base de dados de origem.
        :param source_tables: Lista de tabelas de origem.
        :param limit: Número de registros a serem obtidos na amostra.
        :return: Lista de DataFrames com as amostras dos dados.
        """
        dfs_sample = []
        if self.custom_query:
            logging.info("Usando query customizada para obter amostra.")
            custom_query_with_limit = f"""
                SELECT * 
                FROM ({self.custom_query}) AS custom_query
                LIMIT %(limit)s
            """
            df_sample = pd.read_sql_query(
                custom_query_with_limit,
                source_conn,
                params={"limit": limit}
            )
            dfs_sample.append(df_sample)
        else:
            logging.info("Obtendo amostra das tabelas de origem.")
            for table in source_tables:
                table_name = table.name
                query = text(f"SELECT * FROM {table_name} LIMIT :limit")
                df_sample = pd.read_sql_query(
                    query, source_conn, params={"limit": limit}
                )
                dfs_sample.append(df_sample)
        return dfs_sample

    @abstractmethod
    def check_schema(self, schema_name, engine):
        pass

    @abstractmethod
    def check_table(self, target_table, transformed_df):
        pass

    @abstractmethod
    def process_chunk(self, *args, **kwargs):
        pass

    @abstractmethod
    def process_chunk_replace(self, *args, **kwargs):
        pass

    @abstractmethod
    def clone_tables_incremental(self, *args, **kwargs):
        pass

    @abstractmethod
    def clone_tables_replace(self, *args, **kwargs):
        pass
