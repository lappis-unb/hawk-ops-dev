from scripts.utils.timestamp_converter import TimestampConverter
from scripts.utils.data_expander import DataExpander
import logging


def transform_rasa(dfs):
    df = dfs[0]

    df = TimestampConverter(df).convert_column("timestamp", unit="s")

    df = DataExpander(df).expand_data_column("data")

    df.drop("data", axis=1, inplace=True)

    return df
