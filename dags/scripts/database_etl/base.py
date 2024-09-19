import logging
import threading
from abc import ABC, abstractmethod
import pandas as pd


class BaseETL(ABC):
    def __init__(
        self,
        source_engine,
        target_engine,
        chunk_size=100000,
        max_threads=5,
        multithreading=True,
        transform_func=None,
    ):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.chunk_size = chunk_size
        self.max_threads = max_threads
        self.multithreading = multithreading
        self.transform_func = transform_func

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

    @abstractmethod
    def check_schema(self, schema_name, engine):
        pass

    @abstractmethod
    def verify_and_create_table(self, target_table, transformed_df):
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
