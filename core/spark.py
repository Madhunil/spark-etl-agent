"""
Spark session management for the ETL application.
Creates and configures a Spark session that works without requiring spark-submit.
"""
import os
import sys
import socket
from loguru import logger

# Override PySpark's default behavior of looking for spark-submit
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[*] pyspark-shell'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Import PySpark components - use fully qualified imports to avoid namespace issues
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from core.config import get_settings

class SparkManager:
    """
    Manages the creation and configuration of Spark sessions.
    Works in pure Python mode without requiring spark-submit.
    """
    
    def __init__(self, local_mode=None):
        """
        Initialize the SparkManager.
        
        Args:
            local_mode (bool, optional): Override automatic environment detection
        """
        self.settings = get_settings()
        self.spark = None
        
        # Auto-detect environment if not specified
        if local_mode is None:
            self.local_mode = self._detect_environment()
        else:
            self.local_mode = local_mode
            
        logger.info(f"Initializing SparkManager in {'local' if self.local_mode else 'Kubernetes'} mode")
    
    def _detect_environment(self):
        """
        Automatically detect whether we're running in Kubernetes or locally.
        
        Returns:
            bool: True if running locally, False if in Kubernetes
        """
        # Method 1: Check for Kubernetes service account
        if os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token"):
            logger.info("Detected Kubernetes environment (service account token exists)")
            return False
            
        # Method 2: Check hostname pattern (often k8s pods have specific naming)
        hostname = socket.gethostname()
        if any(pattern in hostname for pattern in ['-pod-', 'kubernetes']):
            logger.info(f"Detected Kubernetes environment (hostname: {hostname})")
            return False
            
        # Method 3: Check environment variables
        if "KUBERNETES_SERVICE_HOST" in os.environ:
            logger.info("Detected Kubernetes environment (from environment variables)")
            return False
            
        logger.info("No Kubernetes indicators found, assuming local environment")
        return True
    
    def create_spark_session(self):
        """
        Create and configure a Spark session based on the detected environment.
        Uses a pure Python approach for local development.
        
        Returns:
            SparkSession: Configured Spark session
        """
        logger.info(f"Creating Spark session in {'local' if self.local_mode else 'Kubernetes'} mode")
        
        if self.local_mode:
            # For pure local Python execution
            logger.info("Configuring for pure Python local mode")
            
            # Create the session using builder pattern directly - avoid SparkConf
            builder = SparkSession.builder
            builder = builder.appName(self.settings.SPARK_APP_NAME)
            builder = builder.master("local[*]")
            builder = builder.config("spark.driver.host", "localhost")
            builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
            builder = builder.config("spark.driver.memory", self.settings.SPARK_DRIVER_MEMORY)
            
            # These settings help with Python-only Spark
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
            builder = builder.config("spark.sql.repl.eagerEval.enabled", "true")
            
            # Add JDBC driver if available
            jdbc_path = self.settings.REDSHIFT_JDBC_DRIVER_PATH
            if os.path.exists(jdbc_path):
                logger.info(f"Added JDBC driver from: {jdbc_path}")
                builder = builder.config("spark.jars", jdbc_path)
            else:
                logger.warning(f"JDBC driver not found at {jdbc_path}, using mock data mode")
            
            # Create the session
            try:
                self.spark = builder.getOrCreate()
                
                # Log session information
                sc = self.spark.sparkContext
                logger.info(f"Spark session created successfully")
                logger.info(f"Spark app ID: {sc.applicationId}, master: {sc.master}, version: {sc.version}")
                
                return self.spark
                
            except Exception as e:
                logger.exception(f"Error creating Spark session: {str(e)}")
                
                # Final fallback - create simplest possible session
                logger.warning("Trying minimal Spark session configuration")
                try:
                    minimal_builder = SparkSession.builder.appName("minimal-spark").master("local")
                    self.spark = minimal_builder.getOrCreate()
                    logger.info("Created minimal Spark session as fallback")
                    return self.spark
                except Exception as min_e:
                    logger.exception(f"Even minimal Spark session failed: {str(min_e)}")
                    raise RuntimeError("Failed to create any kind of Spark session") from min_e
        else:
            # Kubernetes mode configuration
            logger.info("Configuring for Kubernetes mode")
            
            # Create a new configuration
            conf = SparkConf()
            conf.setAppName(self.settings.SPARK_APP_NAME)
            conf.setMaster(f"k8s://{self.settings.K8S_MASTER_URL}")
            conf.set("spark.kubernetes.authenticate.driver.serviceAccountName", 
                     self.settings.K8S_SERVICE_ACCOUNT)
            conf.set("spark.kubernetes.container.image", 
                     self.settings.K8S_CONTAINER_IMAGE)
            conf.set("spark.kubernetes.container.image.pullSecrets",
                     self.settings.K8S_IMAGE_PULL_SECRETS)
            conf.set("spark.kubernetes.namespace", 
                     self.settings.K8S_NAMESPACE)
            conf.set("spark.executor.instances", "1")
            conf.set("spark.driver.memory", self.settings.SPARK_DRIVER_MEMORY)
            conf.set("spark.executor.memory", self.settings.SPARK_EXECUTOR_MEMORY)
            conf.set("spark.kubernetes.executor.limit.cores", "1")
            conf.set("spark.kubernetes.driver.limit.cores", "1")
            conf.set("spark.kubernetes.driver.request.cores", "0.2")
            conf.set("spark.kubernetes.executor.request.cores", "0.2")
            conf.set("spark.driver.host", "headless-spark-etl-jph")
            conf.set("spark.driver.port", "2223")
            
            # Try to create a headless service first
            #self._create_headless_service()
            logger.info(f"headless service already created, skipping creation step")

            # Create the session
            try:
                # Use the builder pattern
                builder = SparkSession.builder.config(conf=conf)
                
                # Add JDBC driver if available
                jdbc_path = self.settings.REDSHIFT_JDBC_DRIVER_PATH
                if os.path.exists(jdbc_path):
                    builder = builder.config("spark.jars", jdbc_path)
                
                # Get or create the session
                self.spark = builder.getOrCreate()
                
                # Log session information
                sc = self.spark.sparkContext
                logger.info(f"Spark session created successfully")
                logger.info(f"Spark app ID: {sc.applicationId}, master: {sc.master}, version: {sc.version}")
                
                return self.spark
                
            except Exception as e:
                logger.exception(f"Failed to create Kubernetes Spark session: {str(e)}")
                raise
    
    def _create_headless_service(self):
        """Create a headless service for Spark driver in Kubernetes mode."""
        try:
            import subprocess
            
            logger.info("Creating headless service for Spark driver")
            service_yaml = """
            apiVersion: v1
            kind: Service
            metadata:
            name: headless-spark-jph
            namespace: jcap-jph-migration
            spec:
            clusterIP: None
            clusterIPs:
            - None
            internalTrafficPolicy: Cluster
            ipFamilies:
            - IPv4
            ipFamilyPolicy: SingleStack
            ports:
            - name: driver
                port: 2222
                protocol: TCP
                targetPort: 2222
            - name: blockmanager
                port: 7777
                protocol: TCP
                targetPort: 7777
            selector:
                app: spark-etl-job
            sessionAffinity: None
            type: ClusterIP

            """
            # Write service definition to a temporary file
            with open("/tmp/headless-service.yaml", "w") as f:
                f.write(service_yaml)
            
            # Apply the service definition
            subprocess.run(["kubectl", "apply", "-f", "/tmp/headless-service.yaml"], check=True)
            logger.info("Headless service created successfully")
            
        except Exception as e:
            logger.warning(f"Failed to create headless service: {str(e)}")
            logger.warning("Continuing without headless service, which may affect cluster execution")
    
    def stop_spark_session(self):
        """Stop the Spark session if it exists and release resources."""
        if self.spark:
            logger.info("Stopping Spark session...")
            try:
                # Get application ID before stopping
                app_id = self.spark.sparkContext.applicationId
                
                # Stop the session
                self.spark.stop()
                self.spark = None
                
                logger.info(f"Spark session (ID: {app_id}) stopped successfully")
                
                # Clean up headless service if in Kubernetes mode
                if not self.local_mode:
                    #self._cleanup_headless_service()
                    logger.info(f"headless service should be always up, skipping deletion step")

                    
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}")
    
    def _cleanup_headless_service(self):
        """Clean up the headless service in Kubernetes mode."""
        try:
            import subprocess
            
            logger.info("Cleaning up headless service")
            subprocess.run(["kubectl", "delete", "service", "headless-spark-jph", 
                           "-n", "jcap-jph-migration"], check=False)
        except Exception as e:
            logger.warning(f"Failed to clean up headless service: {str(e)}")