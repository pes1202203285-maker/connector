from abc import ABC, abstractmethod
from datetime import datetime

class BaseConnector(ABC):

    def __init__(self, config, last_sync=None):
        self.config = config
        self.last_sync = last_sync  # datetime

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def normalize(self, raw_data):
        pass

    @abstractmethod
    def validate(self, clean_data):
        pass

    def discover_schema(self, sample):
        if hasattr(sample, "columns"):
            return list(sample.columns)
        elif isinstance(sample, list) and len(sample) > 0:
            return list(sample[0].keys())
        return []

    def run(self):
        self.connect()
        raw = self.extract()
        clean = self.normalize(raw)
        validated = self.validate(clean)

        return {
            "data": validated,
            "schema": self.discover_schema(validated),
            "last_sync": datetime.utcnow().isoformat()
        }
