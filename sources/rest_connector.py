import requests
from connectors.base import BaseConnector

class RESTConnector(BaseConnector):

    def connect(self):
        self.headers = {
            "Authorization": f"Bearer {self.config.get('api_key', '')}"
        }

    def extract(self):
        params = {}

        if self.last_sync:
            params["updated_since"] = self.last_sync

        res = requests.get(
            self.config["url"],
            headers=self.headers,
            params=params
        )
        return res.json()

    def normalize(self, raw):
        records = raw.get("data", [])

        mapped = []
        for r in records:
            obj = {}
            for src, tgt in self.config["mapping"].items():
                obj[tgt] = r.get(src)
            mapped.append(obj)

        return mapped

    def validate(self, data):
        return [d for d in data if None not in d.values()]
