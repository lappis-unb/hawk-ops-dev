import pandas as pd


class TimestampConverter:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def convert_column(self, column_name: str, unit: str = "s") -> pd.DataFrame:
        try:
            self.df[f"{column_name}"] = pd.to_datetime(self.df[column_name], unit=unit)
            return self.df
        except Exception as e:
            raise e
