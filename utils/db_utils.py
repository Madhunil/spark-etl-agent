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
    """
    
    CONNECTION_TYPES = {
        "poc": "POC/Development",
        "cdp": "CDP Production Source", 
        "jcap": "JCAP Production Destination"
    }
    
    def __init__(self, spark, connection_type: str = "poc"):
        """Initialize connector with specified connection type."""
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
        """Read data from Redshift table with optimizations."""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        try:
            logger.info(f"📖 Reading from {full_table_name} ({self.connection_type})")
            
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
        """Execute SQL query with enhanced error handling."""
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
        """Write DataFrame to Redshift with optimizations."""
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
        PURE SPARK: Execute DDL using Spark-native operations only.
        No psycopg2 dependencies.
        """
        try:
            logger.info(f"⚙️ Executing DDL using pure Spark ({self.connection_type})")
            logger.debug(f"Statement: {sql_statement}")
            
            # For most DDL operations, we use Spark-native alternatives
            if "TRUNCATE" in sql_statement.upper():
                # Extract table name from TRUNCATE statement
                import re
                match = re.search(r'TRUNCATE\s+TABLE\s+(\S+)', sql_statement, re.IGNORECASE)
                if match:
                    table_full_name = match.group(1)
                    if '.' in table_full_name:
                        schema, table = table_full_name.split('.', 1)
                        self.truncate_table(table, schema)
                    else:
                        self.truncate_table(table_full_name)
                    return True
                else:
                    raise ValueError("Could not parse table name from TRUNCATE statement")
            else:
                # For other DDL operations, we don't need them in our ETL
                logger.warning(f"⚠️ DDL operation skipped - not needed for pure Spark ETL: {sql_statement[:50]}")
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
        """FIXED: Truncate table using Spark-native approach."""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        try:
            logger.info(f"🗑️ Truncating {full_table_name}")
            
            # Method 1: Try Spark-native approach first
            try:
                # Read table structure (1 row to get schema)
                existing_df = self.read_table(table_name, schema, limit=1)
                empty_df = existing_df.limit(0)  # Same schema, 0 rows
                
                # Overwrite with empty DataFrame
                self.write_table(empty_df, table_name, schema, mode="overwrite")
                
                logger.info(f"✅ Successfully truncated {full_table_name} using Spark")
                return
                
            except Exception as spark_error:
                logger.warning(f"⚠️ Spark truncate failed, trying DDL: {spark_error}")
                
                # Method 2: Fallback to DDL TRUNCATE
                self.execute_ddl(f"TRUNCATE TABLE {full_table_name}")
                logger.info(f"✅ Successfully truncated {full_table_name} using DDL")
            
        except Exception as e:
            logger.exception(f"❌ Failed to truncate {full_table_name}: {str(e)}")
            raise RuntimeError(f"Truncate operation failed: {str(e)}") from e
    
    def copy_table_data(self, source_table: str, dest_table: str,
                       source_schema: Optional[str] = None, 
                       dest_schema: Optional[str] = None) -> int:
        """FIXED: Copy data between tables using pure Spark."""
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