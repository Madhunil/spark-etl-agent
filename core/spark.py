"""
Spark session management for the ETL application.
Creates and configures a Spark session that works without requiring spark-submit.
Optimized for production use with better error handling and configuration.
"""
import os
import sys
import socket
from typing import Optional
from loguru import logger

# Configure PySpark environment
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[*] pyspark-shell'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from core.config import get_settings

class SparkManager:
    """
    Enhanced Spark session manager with optimized configuration.
    Handles both local and Kubernetes environments efficiently.
    """
    
    def __init__(self, local_mode: Optional[bool] = None):
        """
        Initialize the SparkManager.
        
        Args:
            local_mode: Override automatic environment detection
        """
        self.settings = get_settings()
        self.spark = None
        self.local_mode = local_mode if local_mode is not None else self._detect_environment()
        
        logger.info(f"🔧 SparkManager initialized in {'local' if self.local_mode else 'Kubernetes'} mode")
    
    def _detect_environment(self) -> bool:
        """Automatically detect execution environment."""
        # Check for Kubernetes indicators
        k8s_indicators = [
            os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token"),
            "KUBERNETES_SERVICE_HOST" in os.environ,
            any(pattern in socket.gethostname() for pattern in ['-pod-', 'kubernetes'])
        ]
        
        is_k8s = any(k8s_indicators)
        env_type = "Kubernetes" if is_k8s else "local"
        logger.info(f"🔍 Environment detected: {env_type}")
        
        return not is_k8s
    
    def create_spark_session(self) -> SparkSession:
        """Create optimized Spark session."""
        try:
            logger.info(f"⚡ Creating Spark session in {'local' if self.local_mode else 'Kubernetes'} mode")
            
            if self.local_mode:
                self.spark = self._create_local_session()
            else:
                self.spark = self._create_kubernetes_session()
            
            self._log_session_info()
            self._configure_session()
            
            return self.spark
            
        except Exception as e:
            logger.exception(f"💥 Failed to create Spark session: {str(e)}")
            raise RuntimeError(f"Cannot create Spark session: {str(e)}") from e
    
    def _create_local_session(self) -> SparkSession:
        """Create optimized local Spark session."""
        builder = SparkSession.builder
        builder = builder.appName(self.settings.SPARK_APP_NAME)
        builder = builder.master("local[*]")
        
        # Local optimizations
        builder = builder.config("spark.driver.host", "localhost")
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
        builder = builder.config("spark.driver.memory", self.settings.SPARK_DRIVER_MEMORY)
        builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
        builder = builder.config("spark.sql.adaptive.enabled", "true")
        builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Add JDBC driver if available
        if os.path.exists(self.settings.REDSHIFT_JDBC_DRIVER_PATH):
            builder = builder.config("spark.jars", self.settings.REDSHIFT_JDBC_DRIVER_PATH)
            logger.info(f"📦 JDBC driver loaded: {self.settings.REDSHIFT_JDBC_DRIVER_PATH}")
        else:
            logger.warning(f"⚠️ JDBC driver not found: {self.settings.REDSHIFT_JDBC_DRIVER_PATH}")
        
        return builder.getOrCreate()
    
    def _create_kubernetes_session(self) -> SparkSession:
        """Create optimized Kubernetes Spark session."""
        conf = SparkConf()
        conf.setAppName(self.settings.SPARK_APP_NAME)
        conf.setMaster(f"k8s://{self.settings.K8S_MASTER_URL}")
        
        # Kubernetes configuration
        conf.set("spark.kubernetes.authenticate.driver.serviceAccountName", self.settings.K8S_SERVICE_ACCOUNT)
        conf.set("spark.kubernetes.container.image", self.settings.K8S_CONTAINER_IMAGE)
        conf.set("spark.kubernetes.container.image.pullSecrets", self.settings.K8S_IMAGE_PULL_SECRETS)
        conf.set("spark.kubernetes.namespace", self.settings.K8S_NAMESPACE)
        
        # Resource configuration
        conf.set("spark.executor.instances", "2")
        conf.set("spark.driver.memory", self.settings.SPARK_DRIVER_MEMORY)
        conf.set("spark.executor.memory", self.settings.SPARK_EXECUTOR_MEMORY)
        conf.set("spark.kubernetes.executor.limit.cores", "2")
        conf.set("spark.kubernetes.driver.limit.cores", "2")
        conf.set("spark.kubernetes.driver.request.cores", "0.5")
        conf.set("spark.kubernetes.executor.request.cores", "0.5")
        
        # Network configuration
        conf.set("spark.driver.host", "headless-spark-etl-jph")
        conf.set("spark.driver.port", "2223")
        
        # Performance optimizations
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        # JDBC driver
        if os.path.exists(self.settings.REDSHIFT_JDBC_DRIVER_PATH):
            conf.set("spark.jars", self.settings.REDSHIFT_JDBC_DRIVER_PATH)
        
        builder = SparkSession.builder.config(conf=conf)
        return builder.getOrCreate()
    
    def _configure_session(self) -> None:
        """Configure session-level settings."""
        if self.spark:
            # Set SQL configurations
            self.spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")
            self.spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", "20")
            
            # Set timezone
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")
            
            logger.info("⚙️ Spark session configured with optimizations")
    
    def _log_session_info(self) -> None:
        """Log session information."""
        if self.spark:
            sc = self.spark.sparkContext
            logger.info(f"✅ Spark session created successfully")
            logger.info(f"📊 App ID: {sc.applicationId}")
            logger.info(f"🎯 Master: {sc.master}")
            logger.info(f"🏷️ Version: {sc.version}")
            logger.info(f"💾 Default parallelism: {sc.defaultParallelism}")
    
    def stop_spark_session(self) -> None:
        """Stop Spark session and clean up resources."""
        if self.spark:
            try:
                app_id = self.spark.sparkContext.applicationId
                self.spark.stop()
                self.spark = None
                logger.info(f"🛑 Spark session stopped (ID: {app_id})")
            except Exception as e:
                logger.error(f"❌ Error stopping Spark session: {str(e)}")