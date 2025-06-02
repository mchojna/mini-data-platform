"""MinIO configuration for Spark."""

import os
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from utilities.logger import Logger

logger = Logger.get_logger(__name__)


class MinioConfigurator:
    """Handles MinIO configuration for Spark."""

    def __init__(
        self,
        endpoint: str = None,
        access_key: str = None,
        secret_key: str = None,
        region: str = "us-east-1",
    ):
        """Initialize MinIO configuration."""
        # Use environment variables or defaults
        self.endpoint = endpoint or f"http://minio:{os.getenv('MINIO_API_PORT', '9000')}"
        self.access_key = access_key or os.getenv('MINIO_ROOT_USER', 'minioadmin')
        self.secret_key = secret_key or os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
        self.region = region
        
        logger.info(f"MinIO Configuration - Endpoint: {self.endpoint}, Access Key: {self.access_key}")

    def configure_spark(self, spark: SparkSession) -> SparkSession:
        """Configure Spark to work with MinIO."""
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

        # Configure S3A FileSystem for MinIO
        hadoop_conf.set("fs.s3a.endpoint", self.endpoint)
        hadoop_conf.set("fs.s3a.access.key", self.access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.secret_key)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Additional MinIO-specific configurations
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.attempts.maximum", "3")
        hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
        hadoop_conf.set("fs.s3a.connection.timeout", "10000")
        hadoop_conf.set("fs.s3a.multipart.size", "104857600")  # 100MB
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        
        # Critical configurations for Delta Lake
        hadoop_conf.set("fs.s3a.committer.name", "directory")
        hadoop_conf.set("fs.s3a.committer.staging.conflict-mode", "append")
        hadoop_conf.set("fs.s3a.committer.staging.tmp.path", "/tmp/staging")
        hadoop_conf.set("fs.s3a.buffer.dir", "/tmp/s3a")
        hadoop_conf.set("fs.s3a.block.size", "134217728")  # 128MB
        hadoop_conf.set("fs.s3a.multipart.threshold", "134217728")  # 128MB
        
        logger.info(f"Configured Spark for MinIO endpoint: {self.endpoint}")

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
            except s3_client.exceptions.NoSuchBucket:
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"Created bucket {bucket_name}")
            except Exception as e:
                logger.warning(f"Error checking bucket {bucket_name}: {e}. Attempting to create...")
                try:
                    s3_client.create_bucket(Bucket=bucket_name)
                    logger.info(f"Created bucket {bucket_name}")
                except Exception as create_error:
                    logger.error(f"Failed to create bucket {bucket_name}: {create_error}")
                    raise

        except Exception as e:
            logger.error(f"Failed to configure MinIO bucket: {str(e)}")
            raise
    
    def test_connectivity(self) -> bool:
        """Test connectivity to MinIO."""
        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
                config=Config(signature_version="s3v4"),
            )
            
            # List buckets to test connectivity
            response = s3_client.list_buckets()
            logger.info(f"MinIO connectivity test successful. Found {len(response['Buckets'])} buckets")
            return True
            
        except Exception as e:
            logger.error(f"MinIO connectivity test failed: {str(e)}")
            return False
