"""Module to configure the Debezium PostgreSQL connector for Kafka Connect."""

import os
import json
import requests
from dotenv import load_dotenv
from urllib.error import HTTPError

from utilities.logger import Logger
from utilities.tools import wait_for_kafka_connector

# Load environment
load_dotenv(".env")

# Kafka connect URL
KAFKA_CONNECTOR_URL = os.getenv("KAFKA_CONNECTOR_URL", "http://kafka-connector:8083/")
CONNECTORS_URL = os.getenv("CONNECTORS_URL", "http://kafka-connector:8083/connectors")
CONNECTOR_CONFIG_PATH = os.getenv("CONNECTOR_CONFIG_PATH", "connector_config.json")

# Set up logger
logger = Logger.get_logger(__name__)


class DebeziumConfigurator:
    """
    Class to configure the Debezium PostgreSQL connector with Kafka Connect.
    """

    def __init__(
        self, kafka_connector_url, connectors_url: str, connector_config_path: str
    ):
        self.kafka_connector_url = kafka_connector_url
        self.connectors_url = connectors_url
        self.connector_config_path = connector_config_path

    def load_connector_configuration(self):
        """Loads the connector configuration from the specified configuration path"""
        try:
            with open(self.connector_config_path, "r", encoding="utf-8") as file:
                data = json.load(file)
            return data
        except Exception as e:
            print(f"Failed to import connector config: {e}")
        return {}

    def configure_debezium_connector(self) -> bool:
        """Configure Debezium PostgreSQL connector"""
        if not wait_for_kafka_connector(KAFKA_CONNECTOR_URL, max_retries=20, delay=5):
            return False

        connector_config = self.load_connector_configuration()
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(
                self.connectors_url,
                data=json.dumps(connector_config),
                headers=headers,
                timeout=10,
            )
            if response.status_code == 201:
                logger.info("Debezium connector configured successfully")
                return True
            else:
                logger.info(f"Failed to configure Debezium connector: {response.text}")
                return False
        except Exception as e:
            logger.info(f"Error configuration Debezium connector: {e}")
            return False

    def __call__(self):
        """Main function to configure"""
        self.configure_debezium_connector()


if __name__ == "__main__":
    debezium_configurator = DebeziumConfigurator(
        kafka_connector_url=KAFKA_CONNECTOR_URL,
        connectors_url=CONNECTORS_URL,
        connector_config_path=CONNECTOR_CONFIG_PATH,
    )
    debezium_configurator()
