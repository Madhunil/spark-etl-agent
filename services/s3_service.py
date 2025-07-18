"""
Enhanced S3 service with production optimizations.
Based on working notebook pattern with no complex Hadoop configs.
"""
from typing import Optional
from loguru import logger
from pyspark.sql import DataFrame
from core.config import get_settings

class S3Service:
    """Zero-config S3 service - just basic Spark operations."""
    
    def __init__(self, spark):
        """Initialize with no configuration - rely on existing setup."""
        self.spark = spark
        self.settings = get_settings()
        logger.info("💾 S3 Service initialized")
        logger.info(f"🪣 S3 Bucket: {self.settings.S3_BUCKET}")
    
    def get_s3_path(self, path_suffix: str) -> str:
        """Construct full S3 path using configured bucket."""
        if path_suffix.startswith('/'):
            path_suffix = path_suffix[1:]
        return f"s3a://{self.settings.S3_BUCKET}/{path_suffix}"
    
    def write_parquet(self, df: DataFrame, s3_path: str, mode: str = "overwrite") -> str:
        """Write DataFrame to S3 - exactly like notebook."""
        try:
            row_count = df.count()
            
            # Handle path construction
            if s3_path.startswith("s3://") or s3_path.startswith("s3a://"):
                if s3_path.startswith("s3://"):
                    s3_path = s3_path.replace("s3://", "s3a://")
                final_path = s3_path
            else:
                final_path = self.get_s3_path(s3_path)
            
            logger.info(f"💾 Writing {row_count:,} rows to S3: {final_path}")
            
            # Exact same as notebook - no extra configuration
            source_conf = {
                "ServerSideEncryption": "AES256"
            }
            
            # Direct write - exactly like your notebook
            df.write.options(**source_conf).mode(mode).format('parquet').save(final_path)
            
            logger.info(f"✅ Successfully wrote to S3: {final_path}")
            return final_path
            
        except Exception as e:
            logger.exception(f"❌ Failed to write to S3: {str(e)}")
            raise RuntimeError(f"S3 write failed: {str(e)}") from e
    
    def read_parquet(self, s3_path: str) -> DataFrame:
        """Read Parquet data from S3."""
        try:
            # Handle path construction
            if s3_path.startswith("s3://") or s3_path.startswith("s3a://"):
                if s3_path.startswith("s3://"):
                    s3_path = s3_path.replace("s3://", "s3a://")
                final_path = s3_path
            else:
                final_path = self.get_s3_path(s3_path)
            
            logger.info(f"📖 Reading from S3: {final_path}")
            
            df = self.spark.read.format('parquet').load(final_path)
            row_count = df.count()
            
            logger.info(f"✅ Successfully read {row_count:,} rows from S3")
            return df
            
        except Exception as e:
            logger.exception(f"❌ Failed to read from S3: {str(e)}")
            raise RuntimeError(f"S3 read failed: {str(e)}") from e
    
    def path_exists(self, s3_path: str) -> bool:
        """Check if S3 path exists."""
        try:
            # Handle path construction
            if s3_path.startswith("s3://") or s3_path.startswith("s3a://"):
                if s3_path.startswith("s3://"):
                    s3_path = s3_path.replace("s3://", "s3a://")
                final_path = s3_path
            else:
                final_path = self.get_s3_path(s3_path)
            
            # Try to read schema to check existence
            self.spark.read.format('parquet').load(final_path).schema
            return True
        except Exception:
            return False
    
    def delete_path(self, s3_path: str) -> None:
        """Use overwrite mode instead of deletion."""
        logger.info("🗑️ Using overwrite mode - no deletion needed")