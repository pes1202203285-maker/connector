import pandas as pd
from connectors.base import BaseConnector

class CSVConnector(BaseConnector):

    def connect(self):
        pass

    def extract(self):
        return pd.read_csv(self.config["path"])

    def normalize(self, df):
        mapping = self.config.get("mapping", {})
        return df.rename(columns=mapping)

    def validate(self, df):
        return df.dropna()
