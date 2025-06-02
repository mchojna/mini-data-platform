"""Delta Lake writer for storing processed data in MinIO."""

from typing import Optional, List
from pyspark.sql import DataFrame
from utilities.logger import Logger

logger = Logger.get_logger(__name__)


class DeltaLakeWriter:
    """Handles writing DataFrames to Delta Lake format in MinIO."""

    def __init__(self, bucket: str):
        """Initialize writer with MinIO bucket name."""
        self.bucket = bucket

    def write_stream_to_delta(
        self,
        df: DataFrame,
        table_name: str,
        partition_by: Optional[List[str]] = None,
        checkpoint_location: str = "/tmp/checkpoints",
        output_mode: str = "append",
    ):
        """Write streaming DataFrame to Delta Lake format in MinIO."""
        try:
            path = f"s3a://{self.bucket}/delta/{table_name}"
            writer = df.writeStream.format("delta").outputMode(output_mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer = writer.option("checkpointLocation", f"{checkpoint_location}/{table_name}")
            writer = writer.option("failOnDataLoss", "false")
            # Delta Lake specific options
            writer = writer.option("mergeSchema", "true")
            writer = writer.option("overwriteSchema", "true")
            writer = writer.trigger(processingTime="10 seconds")
            query = writer.start(path)
            logger.info(f"Successfully started streaming write for {table_name} to {path}")
            logger.info(f"Query ID: {query.id}, Status: {query.status}")
            return query

        except Exception as e:
            logger.error(f"Failed to write {table_name} to Delta Lake: {str(e)}")
            raise
    
    def write_batch_to_delta(
        self,
        df: DataFrame,
        table_name: str,
        partition_by: Optional[List[str]] = None,
        mode: str = "append",
    ) -> None:
        """Write batch DataFrame to Delta Lake format in MinIO."""
        try:
            path = f"s3a://{self.bucket}/delta/{table_name}"
            writer = df.write.format("delta").mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            # Delta Lake specific options
            writer = writer.option("mergeSchema", "true")
            writer = writer.option("overwriteSchema", "true")
            
            writer.save(path)
            logger.info(f"Successfully wrote batch data for {table_name} to {path}")
            
        except Exception as e:
            logger.error(f"Failed to write batch {table_name} to Delta Lake: {str(e)}")
            raise
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if Delta table exists in MinIO."""
        try:
            from delta.tables import DeltaTable
            path = f"s3a://{self.bucket}/delta/{table_name}"
            return DeltaTable.isDeltaTable(path)
        except Exception as e:
            logger.debug(f"Table {table_name} does not exist or error checking: {e}")
            return False