"""Module with utilities."""

import time
import requests
from sqlalchemy import Engine
from utilities.models import Base
from utilities.logger import Logger

logger = Logger.get_logger(__name__)


def wait_for_postgres(engine: Engine, max_retries: int, delay: int) -> bool:
    """Wait for PostgreSQL to be available."""
    for i in range(max_retries):
        try:
            with engine.connect():
                logger.info("Successfully connected to PostgreSQL")
                return True
        except Exception:
            logger.info(
                f"Waiting for PostgreSQL to be available... ({i+1}/{max_retries})"
            )
            time.sleep(delay)
    return False


def setup_database(engine: Engine) -> None:
    """Create database tables."""
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully")


def wait_for_kafka_connector(kafka_connector_url: str, max_retries: int, delay: int):
    """Wait for Kafka Connector to be available"""
    for i in range(max_retries):
        try:
            response = requests.get(kafka_connector_url)
            if response.status_code == 200:
                logger.info("Kafka Connect is available")
                return True
        except requests.exceptions.ConnectionError:
            pass

        logger.info(
            f"Waiting for Kafka Connector to be available... ({i+1}/{max_retries})"
        )
        time.sleep(delay)

    logger.info("Failed to connect to Kafka Connect after multiple attemps")
    return False
