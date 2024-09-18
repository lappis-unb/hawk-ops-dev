import logging
import threading
import pandas as pd
from sqlalchemy import text


class BaseETL:
    def __init__(
        self,
        source_engine,
        target_engine,
        chunk_size=100000,
        max_threads=5,
    ):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.chunk_size = chunk_size
        self.max_threads = max_threads

    def clone_table_incremental(self, table_name_source, table_name_target, key_column):
        logging.info(f"Iniciando clonagem incremental da tabela {table_name_source}")
        # Implementação do método (pode ser genérico ou abstrato)
        pass

    def clone_table_replace(self, table_name_source, table_name_target):
        logging.info(f"Iniciando clonagem completa da tabela {table_name_source}")
        # Implementação do método (pode ser genérico ou abstrato)
        pass

    def execute_custom_query(self, query, params=None, target=True):
        engine = self.target_engine if target else self.source_engine
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                logging.info("Consulta executada com sucesso")
                return result.fetchall()
        except Exception as e:
            logging.error("Erro ao executar a consulta customizada", exc_info=True)
            raise e

    def transform_data(self, df):
        # Método genérico para transformações
        return df

    def check_schema(self, schema, engine):

        try:
            with engine.connect() as conn:
                query_exists_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
                result = conn.execute(query_exists_schema)
            
                if result:
                    logging.info(f"Schema {schema} criado com sucesso!")
                    return True
                else:
                    logging.error(f"Erro na criação do schema {schema}!")
                    return False

        except Exception as e:
            logging.error("Erro ao se conectar com o banco de dados", exc_info=True)
            raise e
        
    
    def verify_and_create_table(self, schema, table_name_source, table_name_target, engine):

        try:
            with engine.connect() as conn:
                query_exists_schema = f"""
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name_target}';
                """

                result = conn.execute(query_exists_schema)
                result_query_exists_table = result.fetchone()
            
                if result_query_exists_table:
                   return
                else:
                    query_columns_table = f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = '{table_name_source}'
                        ORDER BY ordinal_position;
                    """
                    result = conn.execute(query_columns_table)
                    result_query_columns_and_types_table = result.fetchall()

                    columns = [item[0] for item in result_query_columns_and_types_table]
                    types = [item[1] for item in result_query_columns_and_types_table]

                    logging.info(f"Colunas: {columns}, tipos: {types}")
                    try:
                        values_table = ''

                        for names, tipes_columns in zip(columns, types):
                            values_table = values_table + f"{names} {tipes_columns}, "

                        values_table = values_table[:-2]

                        query_create_table = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name_target} ({values_table});"

                        conn.execute(query_create_table)
                        conn.commit()
                        return
                    except Exception as e:
                        logging.error(f"Ocorreu um erro: {str(e)}")
 
        except Exception as e:
            logging.error("Erro ao se conectar com o banco de dados", exc_info=True)
            raise e