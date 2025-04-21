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
    ) -> None:
        """Write streaming DataFrame to Delta Lake format in MinIO."""
        try:
            path = f"s3a://{self.bucket}/delta/{table_name}"
            writer = df.writeStream.format("delta").outputMode(output_mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.option("checkpointLocation", f"{checkpoint_location}/{table_name}")
            writer.start(path)
            logger.info(f"Successfully started streaming write for {table_name} to {path}")

        except Exception as e:
            logger.info(f"Failed to write {table_name} to Delta Lake: {str(e)}")
            raise