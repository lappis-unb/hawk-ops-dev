import os
import json
import logging
import threading
from sqlalchemy import text, MetaData, Table, inspect
from sqlalchemy.exc import NoSuchTableError
from airflow.providers.postgres.hooks.postgres import PostgresHook

class Mapping():

    def __init__(
        self,
        target_conn_id,
        target_schema,
        target_table,
    ):
        
        self.target_conn_id = target_conn_id
        self.target_schema = target_schema
        self.target_table = target_table

        self.target_hook = PostgresHook(postgres_conn_id=target_conn_id)

        self.target_engine = self.target_hook.get_sqlalchemy_engine()


    def mapping_relations(self):
        dag_directory = os.path.dirname(os.path.abspath(__file__))
        mapping_file_path = os.path.join(dag_directory, '../../mapping.json')

        with open(mapping_file_path, 'r') as mapping_file:
            mapping_data = json.load(mapping_file)

        # Garantir que a tabela desejada está presente no JSON
        if self.target_table not in mapping_data:
            logging.error(f"Tabela {self.target_table} não encontrada no mapping.json.")
            return

        try:
            with self.target_engine.connect() as conn:
                for relationships in mapping_data[self.target_table]:
                    logging.info(f"Processando relacionamento: {relationships}")

                    schema = relationships.get('schema', self.target_schema)

                    add_constraint = f"""
                        DO $$
                        BEGIN
                            -- Verificar se a tabela já possui uma constraint UNIQUE
                            IF NOT EXISTS (
                                SELECT 1
                                FROM pg_constraint
                                WHERE conrelid = '{schema}.{relationships['related_table']}'::regclass
                                AND contype = 'u'  -- 'u' representa uma constraint UNIQUE
                            ) THEN
                                -- Se não existir constraint UNIQUE, adicionar
                                ALTER TABLE {schema}.{relationships['related_table']}
                                ADD CONSTRAINT unique_{relationships['primary_key']} UNIQUE ({relationships['primary_key']});
                            END IF;
                        END $$;
                    """


        except Exception as e:
            logging.error(f"Erro ao conectar ao banco de dados: {str(e)}")



