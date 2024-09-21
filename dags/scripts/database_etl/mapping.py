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

            for relationships in mapping_data[self.target_table]:
                    log = f"""
                    \n foreign_key: {relationships['foreign_key']}
                    \n related_table: {relationships['related_table']}
                    \n primary_key: {relationships['primary_key']}
                    """
                    logging.info(log)