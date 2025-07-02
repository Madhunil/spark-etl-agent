"""
ETL service for the Spark application.
Handles the specific Control M POC ETL operation.
"""
from loguru import logger
from datetime import datetime
from pyspark.sql.functions import lit
from utils.db_utils import RedshiftConnector

class ETLService:
    """
    Service for ETL operations - implements the exact Control M POC ETL logic.
    """
    
    def __init__(self, spark):
        """
        Initialize the ETL service.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.redshift = RedshiftConnector(spark)
        
        logger.info("ETL Service initialized")
    
    def run_control_m_poc_etl(self, load_date=None, limit=10):
        """
        Run the Control M POC ETL job - implements the exact user requirements:
        
        1. Read from dna_actln_dwh.vw_patients_opsumit_cap with limit
        2. Add load_date column 
        3. Select specific columns: load_date, product, ac_number, referral_date
        4. Write to dna_actln_dwh.ControlM_New_test in append mode
        
        Args:
            load_date (str, optional): Load date to use (default: current date)
            limit (int, optional): Number of rows to extract (default: 10)
        
        Returns:
            dict: Job execution results
        """
        try:
            logger.info("Starting Control M POC ETL job")
            start_time = datetime.now()
            
            # Set load date to today if not provided
            if not load_date:
                load_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"Using load date: {load_date}")
            
            # Step 1: Read from source table with limit
            source_table = "dna_actln_dwh.vw_patients_opsumit_cap"
            logger.info(f"Reading {limit} rows from {source_table}")
            
            df = self.redshift.read_table(
                table_name="vw_patients_opsumit_cap", 
                schema="dna_actln_dwh", 
                limit=limit
            )
            
            # Step 2: Add load_date column
            logger.info(f"Adding load_date column with value: {load_date}")
            df = df.withColumn("load_date", lit(load_date))
            
            # Step 3: Select required columns in exact order
            required_columns = ["load_date", "product", "ac_number", "referral_date"]
            logger.info(f"Selecting columns: {required_columns}")
            df = df.select(*required_columns)
            
            # Log sample data
            logger.info("Sample transformed data:")
            df.show(5, truncate=False)
            
            row_count = df.count()
            logger.info(f"Prepared {row_count} rows for loading")
            
            # Step 4: Write to destination table in append mode
            dest_table = "dna_actln_dwh.ControlM_New_test"
            logger.info(f"Writing to {dest_table} in append mode")
            
            self.redshift.write_table(
                df=df, 
                table_name="ControlM_New_test", 
                schema="dna_actln_dwh", 
                mode="append"
            )
            
            # Calculate job metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"✅ Control M POC ETL job completed successfully!")
            logger.info(f"📊 Processed {row_count} rows in {duration:.2f} seconds")
            
            return {
                "status": "Success",
                "rows_processed": row_count,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "source_table": source_table,
                "destination_table": dest_table,
                "load_date": load_date
            }
            
        except Exception as e:
            logger.exception(f"❌ Control M POC ETL job failed: {str(e)}")
            end_time = datetime.now()
            
            # Calculate duration if start_time is defined
            duration = 0
            if 'start_time' in locals():
                duration = (end_time - start_time).total_seconds()
            
            return {
                "status": "Failed",
                "error": str(e),
                "start_time": locals().get('start_time', datetime.now()),
                "end_time": end_time,
                "duration_seconds": duration
            }