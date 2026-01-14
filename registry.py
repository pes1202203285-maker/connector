from connectors.sources.csv_connector import CSVConnector
from connectors.sources.rest_connector import RESTConnector
from connectors.sources.db_connector import DBConnector

CONNECTORS = {
    "csv": CSVConnector,
    "rest": RESTConnector,
    "db": DBConnector
}

def get_connector(type, config, last_sync=None):
    return CONNECTORS[type](config, last_sync)
