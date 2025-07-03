"""
ETL service for the Spark application.
Handles the specific Control M POC ETL operation.
"""
from typing import Optional, Dict, Any
from datetime import datetime
from loguru import logger
from pyspark.sql.functions import lit
from utils.db_utils import RedshiftConnector

class ETLService:
    """Enhanced ETL service for Control M POC operations."""
    
    def __init__(self, spark):
        """Initialize ETL service."""
        self.spark = spark
        self.redshift = RedshiftConnector(spark, connection_type="poc")
        logger.info("🔧 ETL Service (Control M POC) initialized")
    
    def run_control_m_poc_etl(self, load_date: Optional[str] = None, 
                             limit: int = 10) -> Dict[str, Any]:
        """
        Run Control M POC ETL with enhanced monitoring.
        
        Args:
            load_date: Load date (defaults to current date)
            limit: Number of rows to process
            
        Returns:
            Job execution results
        """
        try:
            logger.info("🚀 Starting Control M POC ETL")
            start_time = datetime.now()
            
            if not load_date:
                load_date = datetime.now().strftime("%Y-%m-%d")
            
            logger.info(f"📅 Load date: {load_date}")
            logger.info(f"🔢 Row limit: {limit}")
            
            # Read source data
            source_table = "dna_actln_dwh.vw_patients_opsumit_cap"
            logger.info(f"📖 Reading from {source_table}")
            
            df = self.redshift.read_table(
                table_name="vw_patients_opsumit_cap",
                schema="dna_actln_dwh",
                limit=limit
            )
            
            # Add load_date column
            df = df.withColumn("load_date", lit(load_date))
            
            # Select required columns
            required_columns = ["load_date", "product", "ac_number", "referral_date"]
            df = df.select(*required_columns)
            
            logger.info("📋 Sample transformed data:")
            df.show(5, truncate=False)
            
            row_count = df.count()
            
            # Write to destination
            dest_table = "dna_actln_dwh.ControlM_New_test"
            logger.info(f"📝 Writing to {dest_table}")
            
            self.redshift.write_table(
                df=df,
                table_name="ControlM_New_test",
                schema="dna_actln_dwh",
                mode="append"
            )
            
            # Calculate metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"✅ Control M POC ETL completed successfully!")
            logger.info(f"📊 Processed {row_count:,} rows in {duration:.2f} seconds")
            
            return {
                "status": "Success",
                "rows_processed": row_count,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "source_table": source_table,
                "destination_table": dest_table,
                "load_date": load_date,
                "limit": limit
            }
            
        except Exception as e:
            logger.exception(f"❌ Control M POC ETL failed: {str(e)}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() if 'start_time' in locals() else 0
            
            return {
                "status": "Failed",
                "error": str(e),
                "start_time": locals().get('start_time', datetime.now()),
                "end_time": end_time,
                "duration_seconds": duration
            }