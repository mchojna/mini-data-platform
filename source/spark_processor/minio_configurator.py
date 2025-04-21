"""MinIO configuration for Spark."""

import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from utilities.logger import Logger

logger = Logger.get_logger(__name__)


class MinioConfigurator:
    """Handles MinIO configuration for Spark."""

    def __init__(
        self,
        endpoint: str = "http://minio:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        region: str = "us-east-1",
    ):
        """Initialize MinIO configuration."""
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

    def configure_spark(self, spark: SparkSession) -> SparkSession:
        """Configure Spark to work with MinIO."""
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

        # Configure S3A FileSystem
        hadoop_conf.set("fs.s3a.endpoint", self.endpoint)
        hadoop_conf.set("fs.s3a.access.key", self.access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.secret_key)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        return spark

    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """Create MinIO bucket if it doesn't exist."""
        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
                config=Config(signature_version="s3v4"),
            )

            try:
                s3_client.head_bucket(Bucket=bucket_name)
                logger.info(f"Bucket {bucket_name} already exists")
            except:
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"Created bucket {bucket_name}")

        except Exception as e:
            logger.error(f"Failed to configure MinIO bucket: {str(e)}")
            raise
