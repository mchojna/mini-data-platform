"""Spark processor with Delta Lake and MinIO integration."""

import os
from typing import List, Dict
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro

from minio_configurator import MinioConfigurator  # Fixed import
from delta_lake_writer import DeltaLakeWriter  # Fixed import
from utilities.logger import Logger

logger = Logger.get_logger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS = [
    "debezium.public.customers",
    "debezium.public.products",
    "debezium.public.orders",
    "debezium.public.order_items",
]
SCHEMAS = {
    "debezium.public.customers": """
                {
                  "type": "record",
                  "name": "Customer",
                  "fields": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "registration_date", "type": "string"}
                  ]
                }
            """,
    "debezium.public.products": """
                {
                  "type": "record",
                  "name": "Product",
                  "fields": [
                    {"name": "product_id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "category", "type": "string"},
                    {"name": "price", "type": "double"},
                    {"name": "stock", "type": "int"}
                  ]
                }
            """,
    "debezium.public.orders": """
                {
                  "type": "record",
                  "name": "Order",
                  "fields": [
                    {"name": "order_id", "type": "int"},
                    {"name": "customer_id", "type": "int"},
                    {"name": "order_date", "type": "string"},
                    {"name": "total_amount", "type": "double"}
                  ]
                }
            """,
    "debezium.public.order_items": """
                {
                  "type": "record",
                  "name": "OrderItem",
                  "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "order_id", "type": "int"},
                    {"name": "product_id", "type": "int"},
                    {"name": "quantity", "type": "int"},
                    {"name": "price", "type": "double"}
                  ]
                }
            """,
}


class SparkProcessor:
    """Spark processor that writes to Delta Lake format in MinIO."""

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        topics: List,
        schemas: Dict,
        minio_config: MinioConfigurator,
        delta_writer: DeltaLakeWriter,
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topics = topics
        self.schemas = schemas
        self.minio_config = minio_config
        self.delta_writer = delta_writer

    def initialize_spark(self) -> SparkSession:
        """Initialize and configure Spark with Delta Lake and MinIO."""
        try:
            builder = SparkSession.builder.appName("SparkProcessor")
            
            # Configure Delta Lake
            builder = (
                builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
            )
            
            # Create Spark session
            spark = builder.master("spark://spark-master:7077").getOrCreate()
            
            # Configure MinIO
            self.minio_config.configure_spark(spark)
            
            return spark
        except Exception as e:
            logger.info(f"Failed to initialize Spark session: {e}")
            raise

    def process_products(self, spark, schemas):
        """Process products and write to Delta Lake."""
        try:
            # Read from Kafka and process products
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", "debezium.public.products") \
                .load()

            # Parse Avro data
            df_transformed = df.select(
                from_avro(col("value"), schemas["debezium.public.products"]).alias("data")
            ).select("data.*")

            # Write to Delta Lake
            self.delta_writer.write_stream_to_delta(
                df_transformed, "products", partition_by=["category"]
            )
            
            return df_transformed
        except Exception as e:
            logger.info(f"Failed to process products: {e}")
            raise

    def process_orders(self, spark, schemas):
        """Process orders and write to Delta Lake."""
        # Read from Kafka and process orders
        try:
            # Read from Kafka and process orders
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", "debezium.public.orders") \
                .load()

            # Parse Avro data
            df_transformed = df.select(
                from_avro(col("value"), schemas["debezium.public.orders"]).alias("data")
            ).select("data.*")

            # Write to Delta Lake
            self.delta_writer.write_stream_to_delta(
                df_transformed, "orders", partition_by=["order_date"]
            )
            
            return df_transformed
        except Exception as e:
            logger.info(f"Failed to process orders: {e}")
            raise

    def process_order_items(self, spark, schemas):
        """Process order items and write to Delta Lake."""
        try:
            # Read from Kafka and process order items
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", "debezium.public.order_items") \
                .load()

            # Parse Avro data
            df_transformed = df.select(
                from_avro(col("value"), schemas["debezium.public.order_items"]).alias("data")
            ).select("data.*")

            # Write to Delta Lake
            self.delta_writer.write_stream_to_delta(
                df_transformed, "order_items", partition_by=["order_id"]
            )
            
            return df_transformed
        except Exception as e:
            logger.info(f"Failed to process order items: {e}")
            raise

    def process_customers(self, spark, schemas):
        """Process customers and write to Delta Lake."""
        try:
            # Read from Kafka and process customers
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", "debezium.public.customers") \
                .load()

            # Parse Avro data
            df_transformed = df.select(
                from_avro(col("value"), schemas["debezium.public.customers"]).alias("data")
            ).select("data.*")

            # Write to Delta Lake
            self.delta_writer.write_stream_to_delta(
                df_transformed, "customers", partition_by=["registration_date"]
            )
            
            return df_transformed
        except Exception as e:
            logger.info(f"Failed to process customers: {e}")
            raise

    def __call__(self):
        """Main processing logic."""
        try:
            logger.info("Starting Spark processing pipeline...")
            
            # Initialize components
            spark = self.initialize_spark()

            # Ensure bucket exists
            self.minio_config.ensure_bucket_exists(self.delta_writer.bucket)

            # Process each data type
            queries = []
            
            # Process customers
            df_customers = self.process_customers(spark, self.schemas)
            queries.append(
                df_customers.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .start()
            )

            # Process products
            df_products = self.process_products(spark, self.schemas)
            queries.append(
                df_products.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .start()
            )

            # Process orders
            df_orders = self.process_orders(spark, self.schemas)
            queries.append(
                df_orders.writeStream.outputMode("append").format("console").start()
            )

            # Process order items
            df_order_items = self.process_order_items(spark, self.schemas)
            queries.append(
                df_order_items.writeStream.outputMode("append").format("console").start()
            )

            # Wait for all queries to complete
            for query in queries:
                query.awaitTermination()

            logger.info("Spark processing pipeline completed successfully.")
        except Exception as e:
            logger.info(f"Error in Spark processing pipeline: {e}")
            raise


if __name__ == "__main__":
    # MinIO configuration
    minio_config = MinioConfigurator(
        endpoint="http://minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )

    # Delta writer configuration
    delta_writer = DeltaLakeWriter(bucket="processed-data")  # Ensure bucket matches

    # Create and run processor
    processor = SparkProcessor(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topics=TOPICS,
        schemas=SCHEMAS,
        minio_config=minio_config,
        delta_writer=delta_writer,
    )
    processor()
