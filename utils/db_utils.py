"""
Database utilities for the Spark ETL application.
Provides connector classes for interacting with Redshift.
"""
from loguru import logger
from core.config import get_settings

class RedshiftConnector:
    """
    Utility class for Redshift operations using Spark JDBC.
    Handles reading from and writing to Redshift tables using Spark.
    """
    
    def __init__(self, spark):
        """
        Initialize the Redshift connector.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.settings = get_settings()
        self.jdbc_url = self.settings.REDSHIFT_JDBC_URL
        self.connection_properties = {
            "user": self.settings.REDSHIFT_USER,
            "password": self.settings.REDSHIFT_PASSWORD,
            "driver": "com.amazon.redshift.jdbc42.Driver"
        }
        
        logger.info(f"RedshiftConnector initialized with connection to {self.jdbc_url}")
    
    def read_table(self, table_name, schema=None, limit=None):
        """
        Read data from a Redshift table.
        
        Args:
            table_name (str): Table name to read
            schema (str, optional): Schema name
            limit (int, optional): Limit the number of rows to read
        
        Returns:
            DataFrame: Spark DataFrame with the query results
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        try:
            logger.info(f"Reading data from Redshift table: {full_table_name}")
            
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=full_table_name,
                properties=self.connection_properties
            )
            
            # Apply limit if specified
            if limit and limit > 0:
                logger.info(f"Applying limit of {limit} rows")
                df = df.limit(limit)
            
            # Count rows for logging
            row_count = df.count()
            logger.info(f"Successfully read {row_count} rows from Redshift table: {full_table_name}")
            
            return df
            
        except Exception as e:
            logger.exception(f"Error reading from Redshift table {full_table_name}: {str(e)}")
            raise RuntimeError(f"Failed to read from Redshift table {full_table_name}: {str(e)}") from e
    
    def write_table(self, df, table_name, schema=None, mode="append"):
        """
        Write data to a Redshift table.
        
        Args:
            df: Spark DataFrame to write
            table_name (str): Target table name
            schema (str, optional): Schema name
            mode (str): Write mode (append, overwrite, etc.)
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        try:
            row_count = df.count()
            logger.info(f"Writing {row_count} rows to Redshift table: {full_table_name} (mode: {mode})")
            
            df.write.jdbc(
                url=self.jdbc_url,
                table=full_table_name,
                mode=mode,
                properties=self.connection_properties
            )
            
            logger.info(f"Successfully wrote {row_count} rows to Redshift table: {full_table_name}")
            
        except Exception as e:
            logger.exception(f"Error writing to Redshift table {full_table_name}: {str(e)}")
            raise RuntimeError(f"Failed to write to Redshift table {full_table_name}: {str(e)}") from e