"""
Enhanced S3 service with production optimizations.
"""
from typing import Optional, List
from loguru import logger
from pyspark.sql import DataFrame
from core.config import get_settings

class S3Service:
    """Production-ready S3 service using Spark."""
    
    def __init__(self, spark):
        """Initialize S3 service with optimized configuration."""
        self.spark = spark
        self.settings = get_settings()
        self._configure_s3_access()
        logger.info("💾 S3 Service initialized")
    
    def _configure_s3_access(self) -> None:
        """Configure Spark for optimized S3 access."""
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            
            # S3A configuration
            hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            hadoop_conf.set("fs.s3a.aws.credentials.provider", 
                           "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
            hadoop_conf.set("fs.s3a.endpoint.region", self.settings.S3_REGION)
            
            # Performance optimizations
            hadoop_conf.set("fs.s3a.block.size", "134217728")  # 128MB
            hadoop_conf.set("fs.s3a.multipart.size", "104857600")  # 100MB
            hadoop_conf.set("fs.s3a.fast.upload", "true")
            hadoop_conf.set("fs.s3a.threads.max", "20")
            
            # Optional credentials
            if self.settings.S3_ACCESS_KEY and self.settings.S3_SECRET_KEY:
                hadoop_conf.set("fs.s3a.access.key", self.settings.S3_ACCESS_KEY)
                hadoop_conf.set("fs.s3a.secret.key", self.settings.S3_SECRET_KEY)
                hadoop_conf.set("fs.s3a.aws.credentials.provider", 
                               "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            
            logger.info("⚙️ S3 access configured with optimizations")
            
        except Exception as e:
            logger.warning(f"⚠️ S3 configuration warning: {str(e)}")
    
    def write_parquet(self, df: DataFrame, s3_path: str, mode: str = "overwrite",
                     partition_by: Optional[List[str]] = None) -> str:
        """Write DataFrame to S3 as optimized Parquet."""
        try:
            row_count = df.count()
            logger.info(f"💾 Writing {row_count:,} rows to S3: {s3_path}")
            
            # Optimize partitions
            if row_count > 100000:
                df = df.repartition(min(20, max(1, row_count // 50000)))
            
            writer = df.write.mode(mode).format('parquet')
            writer = writer.option("compression", "snappy")
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
                logger.info(f"📁 Partitioning by: {partition_by}")
            
            writer.save(s3_path)
            logger.info(f"✅ Successfully wrote to S3: {s3_path}")
            
            return s3_path
            
        except Exception as e:
            logger.exception(f"❌ Failed to write to S3: {str(e)}")
            raise RuntimeError(f"S3 write failed: {str(e)}") from e
    
    def read_parquet(self, s3_path: str) -> DataFrame:
        """Read Parquet data from S3 with optimizations."""
        try:
            logger.info(f"📖 Reading from S3: {s3_path}")
            
            df = self.spark.read.format('parquet').load(s3_path)
            df.cache()
            row_count = df.count()
            
            logger.info(f"✅ Successfully read {row_count:,} rows from S3")
            return df
            
        except Exception as e:
            logger.exception(f"❌ Failed to read from S3: {str(e)}")
            raise RuntimeError(f"S3 read failed: {str(e)}") from e
    
    def path_exists(self, s3_path: str) -> bool:
        """Check if S3 path exists."""
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark.sparkContext._jvm.java.net.URI.create(s3_path), hadoop_conf
            )
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(s3_path)
            return fs.exists(path)
        except Exception:
            return False
    
    def delete_path(self, s3_path: str) -> None:
        """Delete S3 path safely."""
        try:
            if self.path_exists(s3_path):
                hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
                fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                    self.spark.sparkContext._jvm.java.net.URI.create(s3_path), hadoop_conf
                )
                path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(s3_path)
                fs.delete(path, True)
                logger.info(f"🗑️ Deleted S3 path: {s3_path}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to delete S3 path {s3_path}: {str(e)}")