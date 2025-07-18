"""
Job service for the Spark application.
Handles job orchestration and execution.
"""
from typing import Dict, Any, List
from datetime import datetime
from loguru import logger
from services.etl_service import ETLService
from services.jcap_pa_etl_service import JcapPaEtlService

class JobService:
    """Enhanced job orchestration service."""
    
    def __init__(self, spark):
        """Initialize job service with all ETL services."""
        self.spark = spark
        
        # Initialize ETL services
        self.etl_service = ETLService(spark)
        self.jcap_pa_etl_service = JcapPaEtlService(spark)
        
        # Define supported job types
        self.supported_job_types = {
            "control_m_poc_etl": {
                "service": self.etl_service,
                "method": "run_control_m_poc_etl",
                "description": "Control M POC ETL - Development/testing with row limits and append mode",
                "parameters": ["load_date", "limit"],
                "environment": "POC"
            },
            "jcap_pa_etl": {
                "service": self.jcap_pa_etl_service,
                "method": "run_jcap_pa_etl",
                "description": "JCAP PA ETL - Production workflow with backup/restore, variance validation, and alerts",
                "parameters": ["load_date"],
                "environment": "Production"
            }
        }
        
        logger.info("🎛️  Job Service initialized")
        logger.info(f"📋 Supported job types: {len(self.supported_job_types)}")
        
        for job_type, config in self.supported_job_types.items():
            logger.info(f"  🔧 {job_type} ({config['environment']}): {config['description']}")
    
    def list_supported_job_types(self) -> Dict[str, str]:
        """Get supported job types with descriptions."""
        return {
            job_type: f"[{config['environment']}] {config['description']}"
            for job_type, config in self.supported_job_types.items()
        }
    
    def execute_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute job with comprehensive monitoring and error handling.
        Now includes job-specific configuration validation.
        """
        job_id = job_config.get("id", "unknown")
        job_name = job_config.get("name", f"job-{job_id}")
        job_type = job_config.get("type", "control_m_poc_etl")
        
        logger.info(f"🚀 Executing job: {job_name} (ID: {job_id}, Type: {job_type})")
        
        # Validate job type
        if job_type not in self.supported_job_types:
            error_msg = (f"Unknown job type: {job_type}. "
                        f"Supported types: {list(self.supported_job_types.keys())}")
            logger.error(f"❌ {error_msg}")
            return self._create_error_result(job_id, job_name, job_type, error_msg)
        
        # Validate configuration for specific job type
        try:
            from core.config import get_settings
            settings = get_settings()
            settings.validate_for_job_type(job_type)
            logger.info(f"✅ Configuration validated for job type: {job_type}")
        except ValueError as e:
            error_msg = f"Configuration validation failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return self._create_error_result(job_id, job_name, job_type, error_msg)
        
        start_time = datetime.now()
        
        try:
            # Get job configuration
            job_info = self.supported_job_types[job_type]
            service = job_info["service"]
            method_name = job_info["method"]
            
            logger.info(f"📋 Job: {job_info['description']}")
            logger.info(f"🌍 Environment: {job_info['environment']}")
            
            # Execute specific job type
            if job_type == "control_m_poc_etl":
                result = self._execute_control_m_poc_etl(service, method_name, job_config)
            elif job_type == "jcap_pa_etl":
                result = self._execute_jcap_pa_etl(service, method_name, job_config)
            else:
                result = self._execute_generic_job(service, method_name, job_config)
            
            # Enhance result with job metadata
            result.update({
                "job_id": job_id,
                "job_name": job_name,
                "job_type": job_type,
                "job_description": job_info["description"],
                "environment": job_info["environment"]
            })
            
            # Log results
            self._log_job_result(result)
            
            return result
            
        except Exception as e:
            logger.exception(f"💥 Job execution failed: {str(e)}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return self._create_error_result(
                job_id, job_name, job_type, str(e),
                start_time=start_time, end_time=end_time, duration=duration
            )
    
    def _execute_control_m_poc_etl(self, service, method_name: str, 
                                  job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Control M POC ETL with parameter extraction."""
        load_date = job_config.get("load_date")
        limit = job_config.get("limit", 10)
        
        logger.info(f"🔧 Control M POC ETL parameters: load_date={load_date}, limit={limit}")
        
        method = getattr(service, method_name)
        return method(load_date=load_date, limit=limit)
    
    def _execute_jcap_pa_etl(self, service, method_name: str, 
                            job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute JCAP PA ETL with parameter extraction."""
        load_date = job_config.get("load_date")
        
        logger.info(f"🏭 JCAP PA ETL parameters: load_date={load_date}")
        
        method = getattr(service, method_name)
        return method(load_date=load_date)
    
    def _execute_generic_job(self, service, method_name: str, 
                           job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute generic job with dynamic parameter handling."""
        logger.info(f"🔧 Executing generic job: {method_name}")
        
        method = getattr(service, method_name)
        
        # Try different parameter combinations
        try:
            return method(
                load_date=job_config.get("load_date"),
                limit=job_config.get("limit", 10)
            )
        except TypeError:
            try:
                return method(load_date=job_config.get("load_date"))
            except TypeError:
                return method()
    
    def _log_job_result(self, result: Dict[str, Any]) -> None:
        """Log job execution results with detailed metrics."""
        status = result.get("status", "Unknown")
        
        if status == "Success":
            rows = result.get("rows_processed", 0)
            duration = result.get("duration_seconds", 0)
            
            logger.info(f"✅ Job completed successfully!")
            logger.info(f"📊 Processed {rows:,} rows in {duration:.2f} seconds")
            
            # Log job-specific metrics
            if "variance_percentage" in result:
                variance = result["variance_percentage"]
                threshold_exceeded = result.get("variance_threshold_exceeded", False)
                logger.info(f"📈 Data variance: {variance:.2f}%")
                
                if threshold_exceeded:
                    email_sent = result.get("email_sent", False)
                    logger.warning(f"⚠️ Variance threshold exceeded - Alert sent: {email_sent}")
        else:
            error = result.get("error", "Unknown error")
            logger.error(f"❌ Job failed: {error}")
    
    def _create_error_result(self, job_id: str, job_name: str, job_type: str,
                           error_message: str, start_time=None, end_time=None,
                           duration: float = 0) -> Dict[str, Any]:
        """Create standardized error result."""
        if not start_time:
            start_time = datetime.now()
        if not end_time:
            end_time = datetime.now()
        
        return {
            "status": "Failed",
            "job_id": job_id,
            "job_name": job_name,
            "job_type": job_type,
            "error": error_message,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration,
            "rows_processed": 0
        }