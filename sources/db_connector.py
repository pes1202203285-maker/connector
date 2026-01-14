import pandas as pd
from sqlalchemy import create_engine
from connectors.base import BaseConnector

class DBConnector(BaseConnector):

    def connect(self):
        self.engine = create_engine(self.config["connection_string"])

    def extract(self):
        query = self.config["query"]

        if self.last_sync and "updated_at" in query.lower():
            query += f" WHERE updated_at > '{self.last_sync}'"

        return pd.read_sql(query, self.engine)

    def normalize(self, df):
        return df.rename(columns=self.config.get("mapping", {}))

    def validate(self, df):
        return df.dropna()
