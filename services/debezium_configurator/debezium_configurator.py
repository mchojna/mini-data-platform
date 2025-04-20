import json
import time
import logging
import requests
from urllib.error import HTTPError

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Set up handler
console_handler = logging.StreamHandler()

# Set up logging format
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add handler to logger
if not logger.handlers:
    logger.addHandler(console_handler)

def wait_for_kafka_connect():
    """Wait for Kafka Connect to be available"""
    url = "http://kafka-connect:8083/"
    max_retries = 30

    for i in range(max_retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logger.info("Kafka Connect is available")
                return True
        except requests.exceptions.ConnectionError:
            pass

        logger.info(
            f"Waiting for Kafka Connect to be available... ({i+1}/{max_retries})"
        )
        time.sleep(5)

    logger.info("Failed to connect to Kafka Connect after multiple attemps")
    return False


def configure_debezium_connector():
    """Configure Debezium PostgreSQL connector"""
    if not wait_for_kafka_connect():
        return False

    connector_config = {
        "name": "postgres-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname": "postgres",
            "database.server.name": "postgres",
            "topic.prefix": "debezium",
            "table.include.list": "public.customers,public.products,public.orders,public.order_items",
            "plugin.name": "pgoutput",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://schema-registry:8081",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "http://schema-registry:8081",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
        },
    }

    url = "http://kafka-connect:8083/connectors"
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            url, data=json.dumps(connector_config), headers=headers
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


if __name__ == "__main__":
    # Wait a bit for Kafka Connection to initialize
    time.sleep(30)
    configure_debezium_connector()
