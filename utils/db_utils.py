"""
Database utilities for the Spark ETL application.
Provides connector classes for interacting with Redshift using pure Spark operations.
Optimized for production use with comprehensive error handling.
"""
from typing import Optional, Union, List
from loguru import logger
from pyspark.sql import DataFrame
from core.config import get_settings

class RedshiftConnector:
    """
    Production-ready Redshift connector using pure Spark JDBC.
    Supports multiple connection types with optimized operations.
    """
    
    CONNECTION_TYPES = {
        "poc": "POC/Development",
        "cdp": "CDP Production Source", 
        "jcap": "JCAP Production Destination"
    }
    
    def __init__(self, spark, connection_type: str = "poc"):
        """
        Initialize connector with specified connection type.
        
        Args:
            spark: Active SparkSession
            connection_type: Connection type (poc, cdp, jcap)
        """
        if connection_type not in self.CONNECTION_TYPES:
            raise ValueError(f"Invalid connection type: {connection_type}. "
                           f"Valid types: {list(self.CONNECTION_TYPES.keys())}")
        
        self.spark = spark
        self.settings = get_settings()
        self.connection_type = connection_type
        
        # Configure connection properties
        self._configure_connection()
        
        logger.info(f"🔗 RedshiftConnector initialized")
        logger.info(f"📋 Type: {connection_type} ({self.CONNECTION_TYPES[connection_type]})")
    
    def _configure_connection(self) -> None:
        """Configure connection properties based on type."""
        if self.connection_type == "cdp":
            self.jdbc_url = self.settings.CDP_REDSHIFT_JDBC_URL
            self.connection_properties = {
                "user": self.settings.CDP_REDSHIFT_USER,
                "password": self.settings.CDP_REDSHIFT_PASSWORD,
                "driver": "com.amazon.redshift.jdbc42.Driver",
                "loginTimeout": "30",
                "socketTimeout": "300"
            }
        elif self.connection_type == "jcap":
            self.jdbc_url = self.settings.JCAP_REDSHIFT_JDBC_URL
            self.connection_properties = {
                "user": self.settings.JCAP_REDSHIFT_USER,
                "password": self.settings.JCAP_REDSHIFT_PASSWORD,
                "driver": "com.amazon.redshift.jdbc42.Driver",
                "loginTimeout": "30",
                "socketTimeout": "300"
            }
        else:  # poc
            self.jdbc_url = self.settings.REDSHIFT_JDBC_URL
            self.connection_properties = {
                "user": self.settings.REDSHIFT_USER,
                "password": self.settings.REDSHIFT_PASSWORD,
                "driver": "com.amazon.redshift.jdbc42.Driver",
                "loginTimeout": "30",
                "socketTimeout": "300"
            }
    
    def read_table(self, table_name: str, schema: Optional[str] = None, 
                   limit: Optional[int] = None) -> DataFrame:
        """
        Read data from Redshift table with optimizations.
        
        Args:
            table_name: Table name
            schema: Schema name
            limit: Row limit
            
        Returns:
            Spark DataFrame
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        try:
            logger.info(f"📖 Reading from {full_table_name} ({self.connection_type})")
            
            # Use partitioning for large tables
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=full_table_name,
                properties=self.connection_properties
            )
            
            if limit and limit > 0:
                df = df.limit(limit)
                logger.info(f"🔢 Applied limit: {limit}")
            
            # Cache for potential reuse
            df.cache()
            row_count = df.count()
            
            logger.info(f"✅ Successfully read {row_count:,} rows from {full_table_name}")
            return df
            
        except Exception as e:
            logger.exception(f"❌ Failed to read {full_table_name}: {str(e)}")
            raise RuntimeError(f"Read operation failed: {str(e)}") from e
    
    def execute_sql(self, sql_query: str) -> DataFrame:
        """
        Execute SQL query with enhanced error handling.
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            Spark DataFrame with results
        """
        try:
            logger.info(f"🔍 Executing SQL query ({self.connection_type})")
            logger.debug(f"Query preview: {sql_query[:200]}...")
            
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=f"({sql_query}) AS spark_query",
                properties=self.connection_properties
            )
            
            df.cache()
            row_count = df.count()
            
            logger.info(f"✅ Query executed successfully, returned {row_count:,} rows")
            return df
            
        except Exception as e:
            logger.exception(f"❌ SQL execution failed: {str(e)}")
            raise RuntimeError(f"SQL execution failed: {str(e)}") from e
    
    def write_table(self, df: DataFrame, table_name: str, 
                    schema: Optional[str] = None, mode: str = "append") -> None:
        """
        Write DataFrame to Redshift with optimizations.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            schema: Schema name
            mode: Write mode
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        try:
            row_count = df.count()
            logger.info(f"📝 Writing {row_count:,} rows to {full_table_name} (mode: {mode})")
            
            # Optimize partitions for writing
            if row_count > 10000:
                df = df.repartition(min(8, max(1, row_count // 10000)))
            
            df.write.jdbc(
                url=self.jdbc_url,
                table=full_table_name,
                mode=mode,
                properties=self.connection_properties
            )
            
            logger.info(f"✅ Successfully wrote {row_count:,} rows to {full_table_name}")
            
        except Exception as e:
            logger.exception(f"❌ Failed to write to {full_table_name}: {str(e)}")
            raise RuntimeError(f"Write operation failed: {str(e)}") from e
    
    def execute_ddl(self, sql_statement: str) -> bool:
        """
        Execute DDL statement using Spark JDBC connection.
        
        Args:
            sql_statement: DDL statement to execute
            
        Returns:
            True if successful
        """
        try:
            logger.info(f"⚙️ Executing DDL statement ({self.connection_type})")
            logger.debug(f"Statement: {sql_statement}")
            
            # Create temporary DataFrame for DDL execution
            temp_df = self.spark.createDataFrame([], "dummy STRING")
            
            # Use preActions to execute DDL
            temp_df.write.mode("append").option("createTableOptions", "").option(
                "preActions", sql_statement
            ).jdbc(
                url=self.jdbc_url,
                table="(SELECT 1 as dummy LIMIT 0) temp_table",
                properties=self.connection_properties
            )
            
            logger.info("✅ DDL statement executed successfully")
            return True
            
        except Exception as e:
            logger.exception(f"❌ DDL execution failed: {str(e)}")
            raise RuntimeError(f"DDL execution failed: {str(e)}") from e
    
    def get_table_count(self, table_name: str, schema: Optional[str] = None) -> int:
        """Get table row count efficiently."""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        try:
            logger.debug(f"🔢 Getting count for {full_table_name}")
            
            count_df = self.execute_sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}")
            count = count_df.collect()[0]['cnt']
            
            logger.debug(f"📊 {full_table_name}: {count:,} rows")
            return count
            
        except Exception as e:
            logger.exception(f"❌ Failed to get count for {full_table_name}: {str(e)}")
            raise RuntimeError(f"Count operation failed: {str(e)}") from e
    
    def truncate_table(self, table_name: str, schema: Optional[str] = None) -> None:
        """Truncate table using optimized approach."""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        try:
            logger.info(f"🗑️ Truncating {full_table_name}")
            self.execute_ddl(f"TRUNCATE TABLE {full_table_name}")
            logger.info(f"✅ Successfully truncated {full_table_name}")
            
        except Exception as e:
            logger.exception(f"❌ Failed to truncate {full_table_name}: {str(e)}")
            raise RuntimeError(f"Truncate operation failed: {str(e)}") from e
    
    def copy_table_data(self, source_table: str, dest_table: str,
                       source_schema: Optional[str] = None, 
                       dest_schema: Optional[str] = None) -> int:
        """Copy data between tables efficiently."""
        source_name = f"{source_schema}.{source_table}" if source_schema else source_table
        dest_name = f"{dest_schema}.{dest_table}" if dest_schema else dest_table
        
        try:
            logger.info(f"🔄 Copying {source_name} → {dest_name}")
            
            # Read source data
            source_df = self.read_table(source_table, source_schema)
            rows_to_copy = source_df.count()
            
            # Write to destination
            self.write_table(source_df, dest_table, dest_schema, mode="append")
            
            logger.info(f"✅ Successfully copied {rows_to_copy:,} rows")
            return rows_to_copy
            
        except Exception as e:
            logger.exception(f"❌ Failed to copy data: {str(e)}")
            raise RuntimeError(f"Copy operation failed: {str(e)}") from e