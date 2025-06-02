"""Spark processor with Delta Lake and MinIO integration."""

import os
from typing import List, Dict
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

from minio_configurator import MinioConfigurator
from delta_lake_writer import DeltaLakeWriter
from utilities.logger import Logger

logger = Logger.get_logger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS = [
    "debezium.public.customers",
    "debezium.public.products",
    "debezium.public.orders",
    "debezium.public.order_items",
]

# Debezium JSON message structure (with schemas.enable=false)
SCHEMAS = {
    "debezium.public.customers": StructType([
        StructField("after", StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("registration_date", IntegerType(), True)
        ]), True),
        StructField("before", StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("registration_date", IntegerType(), True)
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", IntegerType(), True),
        StructField("source", StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", IntegerType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True)
        ]), True)
    ]),
    "debezium.public.products": StructType([
        StructField("after", StructType([
            StructField("product_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", FloatType(), True),
            StructField("stock", IntegerType(), True)
        ]), True),
        StructField("before", StructType([
            StructField("product_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", FloatType(), True),
            StructField("stock", IntegerType(), True)
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", IntegerType(), True),
        StructField("source", StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", IntegerType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True)
        ]), True)
    ]),
    "debezium.public.orders": StructType([
        StructField("after", StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", IntegerType(), True),
            StructField("total_amount", FloatType(), True)
        ]), True),
        StructField("before", StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", IntegerType(), True),
            StructField("total_amount", FloatType(), True)
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", IntegerType(), True),
        StructField("source", StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", IntegerType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True)
        ]), True)
    ]),
    "debezium.public.order_items": StructType([
        StructField("after", StructType([
            StructField("order_item_id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", FloatType(), True)
        ]), True),
        StructField("before", StructType([
            StructField("order_item_id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", FloatType(), True)
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", IntegerType(), True),
        StructField("source", StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", IntegerType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True)
        ]), True)
    ])
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
                .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
                # Add S3A configurations
                .config("spark.hadoop.fs.s3a.metrics.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
                .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
                # Add streaming configurations
                .config("spark.streaming.stopGracefullyOnShutdown", "true")
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
                .config("spark.sql.streaming.minBatchesToRetain", "100")
                .config("spark.sql.streaming.pollingDelay", "1s")
            )
            
            # Create Spark session
            spark = builder.master("spark://spark-master:7077").getOrCreate()
            
            # Configure MinIO
            self.minio_config.configure_spark(spark)
            
            return spark
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise

    def process_data(self, spark, topic: str, partition_by: str):
        """Generic process function for all data types."""
        try:
            logger.info(f"Starting to process {topic}")
            
            # Read from Kafka
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .load()

            # Debug raw Kafka message
            logger.info(f"=== Raw Kafka Schema for {topic} ===")
            df.printSchema()

            # Parse JSON data with schema
            df_with_schema = df.select(
                from_json(
                    col("value").cast("string"), 
                    self.schemas[topic]
                ).alias("data")
            )

            # Debug JSON parsed schema
            logger.info(f"=== JSON Parsed Schema for {topic} ===")
            df_with_schema.printSchema()

            # Transform data - extract the 'after' field which contains the actual record data
            df_transformed = df_with_schema.select("data.after.*")
            
            # Debug transformed schema
            logger.info(f"=== Transformed Schema for {topic} ===")
            df_transformed.printSchema()
            
            # Write to Delta Lake and return the query
            table_name = topic.split('.')[-1]  # Get last part of topic name
            logger.info(f"Writing {table_name} to Delta Lake with partition column: {partition_by}")
            query = self.delta_writer.write_stream_to_delta(
                df_transformed, 
                table_name, 
                partition_by=[partition_by]
            )

            return query

        except Exception as e:
            logger.error(f"Failed to process {topic}: {e}")
            raise

    def process_customers(self, spark, schemas):
        """Process customers data."""
        return self.process_data(
            spark, 
            "debezium.public.customers", 
            "registration_date"
        )

    def process_products(self, spark, schemas):
        """Process products data."""
        return self.process_data(
            spark, 
            "debezium.public.products", 
            "category"
        )

    def process_orders(self, spark, schemas):
        """Process orders data."""
        return self.process_data(
            spark, 
            "debezium.public.orders", 
            "order_date"
        )

    def process_order_items(self, spark, schemas):
        """Process order items data."""
        return self.process_data(
            spark, 
            "debezium.public.order_items", 
            "order_id"
        )

    def __call__(self):
        """Main processing logic."""
        try:
            logger.info("Starting Spark processing pipeline...")
            
            # Initialize Spark
            spark = self.initialize_spark()
            # spark.sparkContext.setLogLevel("DEBUG")

            # Test MinIO connectivity and ensure bucket exists
            if not self.minio_config.test_connectivity():
                raise Exception("Failed to connect to MinIO")
            self.minio_config.ensure_bucket_exists(self.delta_writer.bucket)

            # Process all streams
            queries = []
            processors = [
                (self.process_customers, "customers"),
                (self.process_products, "products"),
                (self.process_orders, "orders"),
                (self.process_order_items, "order_items")
            ]

            for process_fn, name in processors:
                logger.info(f"=== Processing {name} ===")
                query = process_fn(spark, self.schemas)
                queries.append(query)

            # Monitor and wait for all queries to complete
            logger.info(f"Started {len(queries)} streaming queries")
            for i, query in enumerate(queries):
                logger.info(f"Query {i+1}: ID={query.id}, Status={query.status}")
            
            # Wait for all queries to complete
            for query in queries:
                query.awaitTermination()

            logger.info("Spark processing pipeline completed successfully.")
        except Exception as e:
            logger.error(f"Error in Spark processing pipeline: {e}")
            raise

if __name__ == "__main__":
    # MinIO configuration (uses environment variables)
    minio_config = MinioConfigurator()

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
