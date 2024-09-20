import pandas as pd
import json


class DataExpander:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def expand_data_column(self, column) -> pd.DataFrame:
        if column in self.df.columns:
            self.df[column] = self.df[column].apply(json.loads)
            data_expanded = pd.json_normalize(self.df[column])
            self.df = pd.concat([self.df, data_expanded], axis=1)

        return self.df
