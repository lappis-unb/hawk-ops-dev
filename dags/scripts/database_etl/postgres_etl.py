import logging
import threading
import pandas as pd
from sqlalchemy import text, MetaData, Table, inspect
from sqlalchemy.exc import NoSuchTableError
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
        multithreading=True,
        transform_func=None,
    ):
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.source_schema = source_schema
        self.target_schema = target_schema

        # Initialize hooks
        self.source_hook = PostgresHook(postgres_conn_id=source_conn_id)
        self.target_hook = PostgresHook(postgres_conn_id=target_conn_id)

        # Initialize engines
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
                logging.info(f"Schema {schema_name} created.")

    def verify_and_create_table(self, target_table, transformed_df):
        inspector = inspect(self.target_engine)
        try:
            # Check if the table exists
            if inspector.has_table(target_table, schema=self.target_schema):
                # Reflect the target table
                metadata = MetaData()
                table = Table(
                    target_table,
                    metadata,
                    autoload_with=self.target_engine,
                    schema=self.target_schema,
                )
                # Table exists, compare schemas
                target_columns = {col.name: col.type for col in table.columns}
                df_columns = transformed_df.dtypes.to_dict()

                # Map pandas dtypes to simple types
                dtype_mapping = {
                    "int64": "INTEGER",
                    "float64": "FLOAT",
                    "object": "VARCHAR",
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
                        f"Schema mismatch between transformed DataFrame and target table {target_table}"
                    )
                else:
                    logging.info(f"Target table {target_table} schema is compatible.")
            else:
                # Table does not exist, create it
                logging.info(
                    f"Target table {target_table} does not exist. Creating it."
                )
                transformed_df.head(0).to_sql(
                    target_table,
                    self.target_engine,
                    schema=self.target_schema,
                    if_exists="fail",
                    index=False,
                )
                logging.info(f"Table {target_table} created in target schema.")
        except Exception as e:
            logging.error(
                f"Error in verifying or creating table {target_table}", exc_info=True
            )
            raise e

    def process_chunk(self, offset, last_keys, source_tables, target_table, key_column):
        try:
            dfs = []
            with self.source_engine.connect() as source_conn:
                for idx, table_name in enumerate(source_tables):
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
                            "last_key": last_keys[idx],
                            "limit": self.chunk_size,
                            "offset": offset,
                        },
                    )
                    dfs.append(df_chunk)
            # Apply transformation
            transformed_df = self.transform_data(dfs)
            # Write transformed data to the target table
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
                    logging.info(f"Chunk with offset {offset} processed successfully")
                else:
                    logging.info(f"No data to write for chunk with offset {offset}")
        except Exception as e:
            logging.error(f"Error processing chunk with offset {offset}", exc_info=True)
            raise e

    def process_chunk_replace(self, dfs_chunk, target_table, idx):
        try:
            # Apply transformation
            transformed_df = self.transform_data(dfs_chunk)
            # Write transformed data to the target table
            if not transformed_df.empty:
                transformed_df.to_sql(
                    target_table,
                    self.target_engine,
                    schema=self.target_schema,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                logging.info(f"Chunk {idx} written successfully")
            else:
                logging.info(f"No data to write for chunk {idx}")
        except Exception as e:
            logging.error(f"Error writing chunk {idx}", exc_info=True)
            raise e

    def clone_tables_incremental(self, source_tables, target_table, key_column):
        logging.info(f"Starting incremental clone of tables {source_tables}")

        try:
            # Check if schema exists in the target database
            self.check_schema(self.target_schema, self.target_engine)

            # Get a sample transformed DataFrame to verify or create the target table
            # Read a small sample from source tables
            dfs_sample = []
            with self.source_engine.connect() as source_conn:
                for table_name in source_tables:
                    query = text(f"SELECT * FROM {table_name} LIMIT :limit")
                    df_sample = pd.read_sql_query(
                        query, source_conn, params={"limit": 10}
                    )
                    dfs_sample.append(df_sample)
            # Apply transformation
            transformed_df_sample = self.transform_data(dfs_sample)
            # Verify and create target table if necessary
            self.verify_and_create_table(target_table, transformed_df_sample)

            # Get last key from target and count total rows from each source table
            with self.target_engine.connect() as target_conn:
                result = target_conn.execute(
                    text(
                        f"SELECT MAX({key_column}) FROM {self.target_schema}.{target_table};"
                    )
                )
                last_key_target = result.scalar() or 0
                logging.info(f"Last key value in target: {last_key_target}")

            last_keys = []
            total_rows = 0
            with self.source_engine.connect() as source_conn:
                for table_name in source_tables:
                    # Get last key from source table
                    result = source_conn.execute(
                        text(f"SELECT MAX({key_column}) FROM {table_name};")
                    )
                    last_key_source = result.scalar() or 0
                    last_keys.append(last_key_source)

                    # Count total rows to transfer from this table
                    result = source_conn.execute(
                        text(
                            f"SELECT COUNT(*) FROM {table_name} WHERE {key_column} > :last_key"
                        ),
                        {"last_key": last_key_target},
                    )
                    total_rows_table = result.scalar()
                    total_rows += total_rows_table
                    logging.info(
                        f"Total records to be transferred from {table_name}: {total_rows_table}"
                    )

            if total_rows == 0:
                logging.info("No new records to transfer.")
                return

            # Calculate the number of chunks
            num_chunks = (total_rows // self.chunk_size) + 1

            # Prepare tasks for execution
            tasks = []
            for i in range(num_chunks):
                offset = i * self.chunk_size
                tasks.append(
                    {
                        "function": self.process_chunk,
                        "args": (
                            offset,
                            last_keys,
                            source_tables,
                            target_table,
                            key_column,
                        ),
                    }
                )

            # Execute tasks
            self.execute_tasks(tasks)

            logging.info(
                f"Incremental cloning of tables {source_tables} completed successfully"
            )

        except Exception as e:
            logging.error("Error cloning tables incrementally", exc_info=True)
            raise e

    def clone_tables_replace(self, source_tables, target_table):
        logging.info(f"Starting full clone of tables {source_tables}")

        try:
            # Remove the target table if it exists
            with self.target_engine.connect() as conn:
                conn.execute(
                    text(
                        f"DROP TABLE IF EXISTS {self.target_schema}.{target_table} CASCADE"
                    )
                )
                logging.info(f"Table {target_table} dropped in target")

            # Get a sample transformed DataFrame to verify or create the target table
            # Read a small sample from source tables
            dfs_sample = []
            with self.source_engine.connect() as source_conn:
                for table_name in source_tables:
                    query = text(f"SELECT * FROM {table_name} LIMIT :limit")
                    df_sample = pd.read_sql_query(
                        query, source_conn, params={"limit": 10}
                    )
                    dfs_sample.append(df_sample)
            # Apply transformation
            transformed_df_sample = self.transform_data(dfs_sample)
            # Verify and create target table
            self.verify_and_create_table(target_table, transformed_df_sample)

            # Initialize chunk iterators for all source tables
            chunk_iters = []
            for table_name in source_tables:
                chunk_iter = pd.read_sql_table(
                    table_name,
                    self.source_engine,
                    schema=self.source_schema,
                    chunksize=self.chunk_size,
                )
                chunk_iters.append(chunk_iter)

            idx = 0
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

                self.process_chunk_replace(dfs_chunk, target_table, idx)
                idx += 1

            logging.info(
                f"Full cloning of tables {source_tables} completed successfully"
            )

        except Exception as e:
            logging.error("Error cloning tables completely", exc_info=True)
            raise e
