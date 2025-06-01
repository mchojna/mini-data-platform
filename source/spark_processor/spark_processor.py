"""Spark processor with Delta Lake and MinIO integration."""

import os
from typing import List, Dict
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro

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
# SCHEMAS = {
#     "debezium.public.customers": """
#     {
#     "type": "record",
#     "name": "Envelope",
#     "namespace": "debezium.public.customers",
#     "fields": [
#         {
#         "name": "after",
#         "type": [
#             "null",
#             {
#             "type": "record",
#             "name": "Value",
#             "fields": [
#                 { "name": "customer_id", "type": "int", "default": 0 },
#                 { "name": "name", "type": "string" },
#                 { "name": "email", "type": "string" },
#                 {
#                 "name": "registration_date",
#                 "type": {
#                     "type": "int",
#                     "connect.version": 1,
#                     "connect.name": "io.debezium.time.Date"
#                 }
#                 }
#             ],
#             "connect.name": "debezium.public.customers.Value"
#             }
#         ],
#         "default": null
#         }
#     ]
#     }
#     """,
#     "debezium.public.products": """
#     {
#     "type": "record",
#     "name": "Envelope",
#     "namespace": "debezium.public.products",
#     "fields": [
#         {
#         "name": "after",
#         "type": [
#             "null",
#             {
#             "type": "record",
#             "name": "Value",
#             "fields": [
#                 { "name": "product_id", "type": "int", "default": 0 },
#                 { "name": "name", "type": "string" },
#                 { "name": "category", "type": "string" },
#                 { "name": "price", "type": "float" },
#                 { "name": "stock", "type": "int" }
#             ],
#             "connect.name": "debezium.public.products.Value"
#             }
#         ],
#         "default": null
#         }
#     ]
#     }
#     """,
#     "debezium.public.orders": """
#     {
#     "type": "record",
#     "name": "Envelope",
#     "namespace": "debezium.public.orders",
#     "fields": [
#         {
#         "name": "after",
#         "type": [
#             "null",
#             {
#             "type": "record",
#             "name": "Value",
#             "fields": [
#                 { "name": "order_id", "type": "int", "default": 0 },
#                 { "name": "customer_id", "type": "int" },
#                 {
#                 "name": "order_date",
#                 "type": {
#                     "type": "int",
#                     "connect.version": 1,
#                     "connect.name": "io.debezium.time.Date"
#                 }
#                 },
#                 { "name": "total_amount", "type": "float" }
#             ],
#             "connect.name": "debezium.public.orders.Value"
#             }
#         ],
#         "default": null
#         }
#     ]
#     }
#     """,
#     "debezium.public.order_items": """
#     {
#     "type": "record",
#     "name": "Envelope",
#     "namespace": "debezium.public.order_items",
#     "fields": [
#         {
#         "name": "after",
#         "type": [
#             "null",
#             {
#             "type": "record",
#             "name": "Value",
#             "fields": [
#                 { "name": "order_item_id", "type": "int", "default": 0 },
#                 { "name": "order_id", "type": "int" },
#                 { "name": "product_id", "type": "int" },
#                 { "name": "quantity", "type": "int" },
#                 { "name": "price", "type": "float" }
#             ],
#             "connect.name": "debezium.public.order_items.Value"
#             }
#         ],
#         "default": null
#         }
#     ]
#     }
#     """,
# }

SCHEMAS = {
    "debezium.public.customers": """
    {
        "type": "record",
        "name": "Envelope",
        "namespace": "debezium.public.customers",
        "fields": [
            {
                "name": "data",
                "type": {
                    "type": "record",
                    "name": "DataRecord",
                    "fields": [
                        {
                            "name": "after",
                            "type": ["null", {
                                "type": "record",
                                "name": "CustomerValue",
                                "fields": [
                                    {"name": "customer_id", "type": ["null", "int"], "default": null},
                                    {"name": "name", "type": ["null", "string"]},
                                    {"name": "email", "type": ["null", "string"]},
                                    {"name": "registration_date", "type": ["null", {
                                        "type": "int",
                                        "connect.version": 1,
                                        "connect.name": "io.debezium.time.Date"
                                    }]}
                                ]
                            }]
                        }
                    ]
                }
            }
        ]
    }
    """,
    "debezium.public.products": """
    {
        "type": "record",
        "name": "Envelope",
        "namespace": "debezium.public.products",
        "fields": [
            {
                "name": "data",
                "type": {
                    "type": "record",
                    "name": "DataRecord",
                    "fields": [
                        {
                            "name": "after",
                            "type": ["null", {
                                "type": "record",
                                "name": "ProductValue",
                                "fields": [
                                    {"name": "product_id", "type": ["null", "int"], "default": null},
                                    {"name": "name", "type": ["null", "string"]},
                                    {"name": "category", "type": ["null", "string"]},
                                    {"name": "price", "type": ["null", "float"]},
                                    {"name": "stock", "type": ["null", "int"]}
                                ]
                            }]
                        }
                    ]
                }
            }
        ]
    }
    """,
    "debezium.public.orders": """
    {
        "type": "record",
        "name": "Envelope",
        "namespace": "debezium.public.orders",
        "fields": [
            {
                "name": "data",
                "type": {
                    "type": "record",
                    "name": "DataRecord",
                    "fields": [
                        {
                            "name": "after",
                            "type": ["null", {
                                "type": "record",
                                "name": "OrderValue",
                                "fields": [
                                    {"name": "order_id", "type": ["null", "int"], "default": null},
                                    {"name": "customer_id", "type": ["null", "int"]},
                                    {"name": "order_date", "type": ["null", {
                                        "type": "int",
                                        "connect.version": 1,
                                        "connect.name": "io.debezium.time.Date"
                                    }]},
                                    {"name": "total_amount", "type": ["null", "float"]}
                                ]
                            }]
                        }
                    ]
                }
            }
        ]
    }
    """,
    "debezium.public.order_items": """
    {
        "type": "record",
        "name": "Envelope",
        "namespace": "debezium.public.order_items",
        "fields": [
            {
                "name": "data",
                "type": {
                    "type": "record",
                    "name": "DataRecord",
                    "fields": [
                        {
                            "name": "after",
                            "type": ["null", {
                                "type": "record",
                                "name": "OrderItemValue",
                                "fields": [
                                    {"name": "order_item_id", "type": ["null", "int"], "default": null},
                                    {"name": "order_id", "type": ["null", "int"]},
                                    {"name": "product_id", "type": ["null", "int"]},
                                    {"name": "quantity", "type": ["null", "int"]},
                                    {"name": "price", "type": ["null", "float"]}
                                ]
                            }]
                        }
                    ]
                }
            }
        ]
    }
    """
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

    # def initialize_spark(self) -> SparkSession:
    #     """Initialize and configure Spark with Delta Lake and MinIO."""
    #     try:
    #         builder = SparkSession.builder.appName("SparkProcessor")
            
    #         # Configure Delta Lake
    #         builder = (
    #             builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    #             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    #             .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    #         )
            
    #         # Create Spark session
    #         spark = builder.master("spark://spark-master:7077").getOrCreate()
            
    #         # Configure MinIO
    #         self.minio_config.configure_spark(spark)
            
    #         return spark
    #     except Exception as e:
    #         logger.info(f"Failed to initialize Spark session: {e}")
    #         raise

    # def process_products(self, spark, schemas):
    #     """Process products and write to Delta Lake."""
    #     try:
    #         # Read from Kafka and process products
    #         df = spark.readStream \
    #             .format("kafka") \
    #             .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
    #             .option("subscribe", "debezium.public.products") \
    #             .load()

    #         # Parse Avro data
    #         df_transformed = df.select(
    #             from_avro(col("value"), schemas["debezium.public.products"]).alias("data")
    #         ).select("data.after.*")

    #         # Write to Delta Lake
    #         self.delta_writer.write_stream_to_delta(
    #             df_transformed, "products", partition_by=["category"]
    #         )
            
    #         return df_transformed
    #     except Exception as e:
    #         logger.info(f"Failed to process products: {e}")
    #         raise

    # def process_orders(self, spark, schemas):
    #     """Process orders and write to Delta Lake."""
    #     # Read from Kafka and process orders
    #     try:
    #         # Read from Kafka and process orders
    #         df = spark.readStream \
    #             .format("kafka") \
    #             .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
    #             .option("subscribe", "debezium.public.orders") \
    #             .load()

    #         # Parse Avro data
    #         df_transformed = df.select(
    #             from_avro(col("value"), schemas["debezium.public.orders"]).alias("data")
    #         ).select("data.after.*")

    #         # Write to Delta Lake
    #         self.delta_writer.write_stream_to_delta(
    #             df_transformed, "orders", partition_by=["order_date"]
    #         )
            
    #         return df_transformed
    #     except Exception as e:
    #         logger.info(f"Failed to process orders: {e}")
    #         raise

    # def process_order_items(self, spark, schemas):
    #     """Process order items and write to Delta Lake."""
    #     try:
    #         # Read from Kafka and process order items
    #         df = spark.readStream \
    #             .format("kafka") \
    #             .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
    #             .option("subscribe", "debezium.public.order_items") \
    #             .load()

    #         # Parse Avro data
    #         df_transformed = df.select(
    #             from_avro(col("value"), schemas["debezium.public.order_items"]).alias("data")
    #         ).select("data.after.*")

    #         # Write to Delta Lake
    #         self.delta_writer.write_stream_to_delta(
    #             df_transformed, "order_items", partition_by=["order_id"]
    #         )
            
    #         return df_transformed
    #     except Exception as e:
    #         logger.info(f"Failed to process order items: {e}")
    #         raise

    # def process_customers(self, spark, schemas):
    #     """Process customers and write to Delta Lake."""
    #     try:
    #         # Read from Kafka and process customers
    #         # df = spark.readStream \
    #         #     .format("kafka") \
    #         #     .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
    #         #     .option("subscribe", "debezium.public.customers") \
    #         #     .load()

    #         # # Parse Avro data
    #         # df_transformed = df.select(
    #         #     from_avro(col("value"), schemas["debezium.public.customers"]).alias("data")
    #         # ).select("data.after.*")

    #         # # Write to Delta Lake
    #         # self.delta_writer.write_stream_to_delta(
    #         #     df_transformed, "customers", partition_by=["registration_date"]
    #         # )
    #         # 
    #         # return df_transformed
            
    #         # Read from Kafka and process customers
    #         df = spark.readStream \
    #             .format("kafka") \
    #             .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
    #             .option("subscribe", "debezium.public.customers") \
    #             .load()

    #         # Print raw Kafka schema
    #         logger.info("=== Original Kafka Schema ===")
    #         df.printSchema()

    #         # Print raw Kafka data with schema
    #         df.writeStream \
    #             .outputMode("append") \
    #             .format("console") \
    #             .option("truncate", False) \
    #             .start() \
    #             .awaitTermination(timeout=5)

    #         # Parse Avro data and print its schema
    #         df_with_schema = df.select(
    #             from_avro(
    #                 col("value"), 
    #                 schemas["debezium.public.customers"],
    #                 {"mode": "PERMISSIVE", "printSchema": True}  # This will print the actual Avro schema
    #             ).alias("data")
    #         )

    #         # Print the schema after Avro deserialization
    #         logger.info("=== Schema After Avro Deserialization ===")
    #         df_with_schema.printSchema()
            
    #         # Continue with normal processing
    #         df_transformed = df_with_schema.select("data.after.*")

    #         return df_transformed            
            
    #     except Exception as e:
    #         logger.info(f"Failed to process customers: {e}")
    #         raise

    # def __call__(self):
    #     """Main processing logic."""
    #     try:
    #         logger.info("Starting Spark processing pipeline...")
            
    #         # Initialize components
    #         spark = self.initialize_spark()
            
    #         # Set more verbose logging
    #         spark.sparkContext.setLogLevel("DEBUG")

    #         # Ensure bucket exists
    #         self.minio_config.ensure_bucket_exists(self.delta_writer.bucket)

    #         # Process each data type
    #         queries = []
            
    #         ####
            
    #         for df, name in [
    #             (self.process_customers(spark, self.schemas), "customers"),
    #             (self.process_products(spark, self.schemas), "products"),
    #             (self.process_orders(spark, self.schemas), "orders"),
    #             (self.process_order_items(spark, self.schemas), "order_items")
    #         ]:
    #             logger.info(f"=== Processing {name} ===")
    #             queries.append(
    #                 df.writeStream
    #                 .outputMode("append")
    #                 .format("console")
    #                 .option("truncate", False)
    #                 .option("numRows", 20)
    #                 .trigger(processingTime="10 seconds")  # Process every 10 seconds
    #                 .start()
    #             )

    #         # Wait for all queries to complete
    #         for query in queries:
    #             query.awaitTermination()
            
    #         # # Process customers
    #         # df_customers = self.process_customers(spark, self.schemas)
    #         # queries.append(
    #         #     df_customers.writeStream.outputMode("append")
    #         #     .format("console")
    #         #     .option("truncate", False)
    #         #     .start()
    #         # )

    #         # # Process products
    #         # df_products = self.process_products(spark, self.schemas)
    #         # queries.append(
    #         #     df_products.writeStream.outputMode("append")
    #         #     .format("console")
    #         #     .option("truncate", False)
    #         #     .start()
    #         # )

    #         # # Process orders
    #         # df_orders = self.process_orders(spark, self.schemas)
    #         # queries.append(
    #         #     df_orders.writeStream.outputMode("append").format("console").start()
    #         # )

    #         # # Process order items
    #         # df_order_items = self.process_order_items(spark, self.schemas)
    #         # queries.append(
    #         #     df_order_items.writeStream.outputMode("append").format("console").start()
    #         # )

    #         # # Wait for all queries to complete
    #         # for query in queries:
    #         #     query.awaitTermination()

    #         logger.info("Spark processing pipeline completed successfully.")
    #     except Exception as e:
    #         logger.info(f"Error in Spark processing pipeline: {e}")
    #         raise

    def initialize_spark(self) -> SparkSession:
        """Initialize and configure Spark with Delta Lake and MinIO."""
        try:
            builder = SparkSession.builder.appName("SparkProcessor")
            
            # Configure Delta Lake
            builder = (
                builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
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

            # Parse Avro data with schema
            df_with_schema = df.select(
                from_avro(
                    col("value"), 
                    self.schemas[topic],
                    {"mode": "PERMISSIVE"}
                ).alias("data")
            )

            # Debug Avro parsed schema
            logger.info(f"=== Avro Parsed Schema for {topic} ===")
            df_with_schema.printSchema()

            # Transform data - adjusted path to match actual schema
            df_transformed = df_with_schema.select("data.data.after.*")
            
            # Write to Delta Lake
            table_name = topic.split('.')[-1]  # Get last part of topic name
            self.delta_writer.write_stream_to_delta(
                df_transformed, 
                table_name, 
                partition_by=[partition_by]
            )

            return df_transformed

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

            # Ensure bucket exists
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
                df = process_fn(spark, self.schemas)
                
                query = df.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .option("truncate", False) \
                    .option("numRows", 20) \
                    .option("numRows", 20) \
                    .trigger(processingTime="5 seconds") \
                    .option("checkpointLocation", f"/tmp/checkpoints/{name}") \
                    .option("failOnDataLoss", "false") \
                    .start()
                    
                queries.append(query)

            # Wait for all queries to complete
            for query in queries:
                query.awaitTermination()

            logger.info("Spark processing pipeline completed successfully.")
        except Exception as e:
            logger.error(f"Error in Spark processing pipeline: {e}")
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
