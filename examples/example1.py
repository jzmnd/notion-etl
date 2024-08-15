import datetime as dt

import pandas as pd

from notion_etl.transformer import BaseTransformer


class Example1Transformer(BaseTransformer):

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data["DateTime"] = pd.to_datetime(data["Date"] + " " + data["Time"])
        data["Date"] = pd.to_datetime(data["Date"])
        data["LoadDateTime"] = dt.datetime.now()
        return data
