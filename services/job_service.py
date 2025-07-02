"""
Job service for the Spark application.
Handles job orchestration and execution.
"""
from loguru import logger
from datetime import datetime
from services.etl_service import ETLService 

class JobService:
    """
    Service for managing job execution.
    Coordinates job execution and tracks status.
    """
    
    def __init__(self, spark):
        """
        Initialize the job service. 
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.etl_service = ETLService(spark)
        logger.info("Job Service initialized")
    
    def execute_job(self, job_config):
        """
        Execute a job based on its configuration.
        
        Args:
            job_config (dict): Job configuration
                Expected format:
                {
                    "id": "job_001",
                    "name": "Control M POC ETL",
                    "type": "control_m_poc_etl",
                    "load_date": "2025-05-20",
                    "limit": 10
                }
            
        Returns:
            dict: Job execution results
        """
        job_id = job_config.get("id")
        job_name = job_config.get("name", f"job-{job_id}")
        job_type = job_config.get("type", "control_m_poc_etl")
        
        logger.info(f"🚀 Executing job: {job_name} (ID: {job_id}, Type: {job_type})")
        
        start_time = datetime.now()
        
        try:
            # Currently only supports control_m_poc_etl
            if job_type == "control_m_poc_etl":
                # Extract parameters
                load_date = job_config.get("load_date")
                limit = job_config.get("limit", 10)
                
                # Execute the ETL job
                result = self.etl_service.run_control_m_poc_etl(
                    load_date=load_date,
                    limit=limit
                )
            else:
                logger.error(f"❌ Unknown job type: {job_type}")
                result = {
                    "status": "Failed",
                    "error": f"Unknown job type: {job_type}. Only 'control_m_poc_etl' is supported."
                }
            
            # Add job metadata to the result
            result["job_id"] = job_id
            result["job_name"] = job_name
            result["job_type"] = job_type
            
            # Log the result
            if result["status"] == "Success":
                logger.info(f"✅ Job {job_name} completed successfully")
                logger.info(f"📊 Processed {result.get('rows_processed', 0)} rows in {result.get('duration_seconds', 0):.2f} seconds")
            else:
                logger.error(f"❌ Job {job_name} failed: {result.get('error')}")
            
            return result
            
        except Exception as e:
            logger.exception(f"💥 Error executing job {job_name}")
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return {
                "status": "Failed",
                "job_id": job_id,
                "job_name": job_name,
                "job_type": job_type,
                "error": str(e),
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration
            }