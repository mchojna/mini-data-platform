"""Kafka consumer to read and validate JSON messages from Debezium topics."""

import os
import time
import json
from typing import Dict, Any
from confluent_kafka import Consumer
from kafka.admin import KafkaAdminClient, NewTopic
from dotenv import load_dotenv

from utilities.logger import Logger

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "kafka-consumer-group")

# Topics to subscribe to
TOPICS = [
    "debezium.public.customers",
    "debezium.public.products",
    "debezium.public.orders",
    "debezium.public.order_items",
]

# Set up logger
logger = Logger.get_logger(__name__)


class KafkaConsumer:
    """
    Kafka consumer that deserializes and logs JSON messages from Debezium change data capture topics.
    """

    def __init__(self, bootstrap_servers: str, group_id: str):
        """Initialize Kafka consumer for JSON messages."""
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

        # Initialize consumer
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            }
        )

    def wait_for_topics(self, max_retries: int = 30, delay: int = 10) -> bool:
        """Wait for Kafka topics to be created."""
        for attempt in range(max_retries):
            try:
                admin_client = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers
                )
                existing_topics = admin_client.list_topics()
                missing_topics = [
                    topic for topic in TOPICS if topic not in existing_topics
                ]

                if not missing_topics:
                    logger.info("All required topics are available")
                    admin_client.close()
                    return True

                logger.info(f"Waiting for topics to be created: {missing_topics}")
                admin_client.close()

            except Exception as e:
                logger.error(
                    f"Error checking topics (attempt {attempt + 1}/{max_retries}): {str(e)}",
                    exc_info=True,
                )

            time.sleep(delay)

        return False

    def decode_message(self, message: Any, topic: str) -> Dict:
        """Decode JSON message."""
        try:
            if message.value():
                return json.loads(message.value().decode('utf-8'))
            return None
        except Exception as e:
            logger.info(f"Error decoding JSON message from topic {topic}: {e}")
            return None

    def process_message(self, message: Any) -> None:
        """Process incoming Kafka message."""
        if message is None:
            return

        topic = message.topic()
        decoded_message = self.decode_message(message, topic)

        if decoded_message:
            logger.info(f"Received message from topic {topic}:")
            logger.info(json.dumps(decoded_message, indent=2))

    def __call__(self) -> None:
        """Main consumer loop."""
        try:
            # Wait for topics to be created
            if not self.wait_for_topics():
                logger.error("Topics not available after maximum retries")
                return

            # Subscribe to topics
            self.consumer.subscribe(TOPICS)
            logger.info(f"Subscribed to topics: {TOPICS}")

            while True:
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    logger.info(f"Consumer error: {message.error()}")
                    continue

                self.process_message(message)

        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
    )
    consumer()
