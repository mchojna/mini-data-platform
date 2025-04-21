import os
from typing import List, Dict
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.avro.functions import from_avro

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
    """
    Spark Structured Streaming job to read AVRO messages from Kafka topics,
    deserialize using inline schemas, and perform basic transformations.
    """

    def __init__(self, kafka_bootstrap_servers: str, topics: List, schemas: Dict):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topics = topics
        self.schemas = schemas

    def process_products(self, spark, schemas):
        topic = "debezium.public.products"
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        df_avro = df.select(from_avro(col("value"), schemas[topic]).alias("data"))
        df_transformed = df_avro.select(
            col("data.product_id"),
            col("data.name"),
            col("data.category"),
            col("data.price"),
            col("data.stock"),
        )
        return df_transformed

    def process_orders(self, spark, schemas):
        topic = "debezium.public.orders"
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        df_avro = df.select(from_avro(col("value"), schemas[topic]).alias("data"))
        df_transformed = df_avro.select(
            col("data.order_id"),
            col("data.customer_id"),
            col("data.order_date"),
            col("data.total_amount"),
        )
        return df_transformed

    def process_order_items(self, spark, schemas):
        topic = "debezium.public.order_items"
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        df_avro = df.select(from_avro(col("value"), schemas[topic]).alias("data"))
        df_transformed = df_avro.select(
            col("data.id"),
            col("data.order_id"),
            col("data.product_id"),
            col("data.quantity"),
            col("data.price"),
        )
        return df_transformed

    def analyze_orders_by_window(self, spark, schemas):
        # Example: count orders per 1 minute window
        topic = "debezium.public.orders"
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        df_avro = df.select(from_avro(col("value"), schemas[topic]).alias("data"))
        # Use order_date as event time (cast to timestamp if needed)
        df_with_ts = df_avro.withColumn(
            "order_ts", col("data.order_date").cast("timestamp")
        )
        df_windowed = (
            df_with_ts.groupBy(window(col("order_ts"), "1 minute"))
            .count()
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("count").alias("orders_count"),
            )
        )
        return df_windowed

    def __call__(self):
        spark = SparkSession.builder.appName("SparkProcessor").master("spark://spark-master:7077").getOrCreate()
        queries = []

        # Process products
        df_products = self.process_products(spark, self.schemas)
        queries.append(
            df_products.writeStream.outputMode("append")
            .format("console")
            .option("truncate", False)
            .option("numRows", 10)
            .start()
        )

        # Process orders
        df_orders = self.process_orders(spark, self.schemas)
        queries.append(
            df_orders.writeStream.outputMode("append")
            .format("console")
            .option("truncate", False)
            .option("numRows", 10)
            .start()
        )

        # Process order_items
        df_order_items = self.process_order_items(spark, self.schemas)
        queries.append(
            df_order_items.writeStream.outputMode("append")
            .format("console")
            .option("truncate", False)
            .option("numRows", 10)
            .start()
        )

        # Analyze orders by window
        df_orders_window = self.analyze_orders_by_window(spark, self.schemas)
        queries.append(
            df_orders_window.writeStream.outputMode("complete")
            .format("console")
            .option("truncate", False)
            .option("numRows", 10)
            .start()
        )

        for q in queries:
            q.awaitTermination()


if __name__ == "__main__":
    spark_processor = SparkProcessor(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topics=TOPICS, schemas=SCHEMAS
    )
    spark_processor()
