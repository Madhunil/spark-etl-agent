"""
Configuration management for the Spark ETL application.
Loads settings from environment variables and provides them to the application.
Optimized for production & enhanced configuration management using AWS Secrets Manager.
Fixed to make CDP/JCAP/S3/Email fields optional based on job type requirements.
Environment-aware: Uses same variable names for dev/prod environments.

Environment Setup:
- Dev: Set AWS_SECRET_NAME=jph-eks-dev-secret | Prod: Set AWS_SECRET_NAME=jph-eks-prod-secret
- Both secrets contain same JSON structure with environment-specific values
"""
import os
from typing import Optional
from pydantic import Field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
from functools import lru_cache
from loguru import logger

load_dotenv()

class Settings(BaseSettings):
    """
    Application settings with AWS Secrets Manager integration.
    Sensitive credentials retrieved from secrets, non-sensitive from env vars.
    """
    
    # Non-sensitive configuration from environment variables
    SPARK_DRIVER_MEMORY: str = Field(default="1g", description="Spark driver memory")
    SPARK_EXECUTOR_MEMORY: str = Field(default="1g", description="Spark executor memory")
    SPARK_APP_NAME: str = Field(default="jph-etl-spark-agent", description="Spark application name")
    SPARK_DRIVER_HOST: str = Field(default="headless-spark-etl-jph", description="Spark driver host")
    SPARK_DRIVER_PORT: str = Field(default="2223", description="Spark driver port")
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    DATA_VARIANCE_THRESHOLD: float = Field(default=5.0, description="Data variance threshold percentage")
    
    # Kubernetes Configuration (non-sensitive, from env vars)
    K8S_MASTER_URL: str = Field(default="", description="Kubernetes master URL")
    K8S_NAMESPACE: str = Field(default="", description="Kubernetes namespace")
    K8S_SERVICE_ACCOUNT: str = Field(default="", description="Kubernetes service account")
    K8S_CONTAINER_IMAGE: str = Field(default="", description="Container image for Spark executors")
    K8S_IMAGE_PULL_SECRETS: str = Field(default="", description="Image pull secrets")
    
    # Sensitive configuration (will be populated from Secrets Manager)
    REDSHIFT_HOST: str = Field(default="", description="POC Redshift host")
    REDSHIFT_PORT: str = Field(default="5439", description="POC Redshift port")
    REDSHIFT_DATABASE: str = Field(default="", description="POC Redshift database")
    REDSHIFT_USER: str = Field(default="", description="POC Redshift user")
    REDSHIFT_PASSWORD: str = Field(default="", description="POC Redshift password")
    REDSHIFT_JDBC_DRIVER_PATH: str = Field(default="/opt/spark/jars/redshift-jdbc42-2.1.0.29.jar", description="JDBC driver path")
    
    # CDP Redshift Configuration (from Secrets Manager)
    CDP_REDSHIFT_HOST: str = Field(default="", description="CDP Redshift host")
    CDP_REDSHIFT_PORT: str = Field(default="5439", description="CDP Redshift port")
    CDP_REDSHIFT_DATABASE: str = Field(default="", description="CDP Redshift database")
    CDP_REDSHIFT_SCHEMA: str = Field(default="cdp", description="CDP Redshift schema")
    CDP_REDSHIFT_USER: str = Field(default="", description="CDP Redshift user")
    CDP_REDSHIFT_PASSWORD: str = Field(default="", description="CDP Redshift password")
    
    # JCAP Redshift Configuration (from Secrets Manager)
    JCAP_REDSHIFT_HOST: str = Field(default="", description="JCAP Redshift host")
    JCAP_REDSHIFT_PORT: str = Field(default="5439", description="JCAP Redshift port")
    JCAP_REDSHIFT_DATABASE: str = Field(default="", description="JCAP Redshift database")
    JCAP_REDSHIFT_SCHEMA: str = Field(default="jcap_presentation", description="JCAP Redshift schema")
    JCAP_REDSHIFT_USER: str = Field(default="", description="JCAP Redshift user")
    JCAP_REDSHIFT_PASSWORD: str = Field(default="", description="JCAP Redshift password")
    
    # S3 Configuration (from Secrets Manager)
    S3_BUCKET: str = Field(default="itx-ahr-jcap-jph-data/test", description="S3 bucket for data staging")
    S3_REGION: str = Field(default="us-east-1", description="S3 region")
    S3_IAM_ROLE: str = Field(default="", description="IAM role for S3/Redshift operations")
    S3_ACCESS_KEY: Optional[str] = Field(default="", description="S3 access key (optional)")
    S3_SECRET_KEY: Optional[str] = Field(default="", description="S3 secret key (optional)")
    
    # Email Configuration (from Secrets Manager)
    SMTP_SERVER: str = Field(default="smtp.na.jnj.com", description="SMTP server")
    SMTP_PORT: int = Field(default=25, description="SMTP port")
    SMTP_USE_TLS: bool = Field(default=False, description="Use TLS for SMTP")
    SMTP_USERNAME: Optional[str] = Field(default="", description="SMTP username")
    SMTP_PASSWORD: Optional[str] = Field(default="", description="SMTP password")
    EMAIL_FROM: str = Field(default="MPachgha@its.jnj.com", description="From email address")
    EMAIL_TO_DNA_TEAM: str = Field(default="MPachgha@its.jnj.com" , description="DNA team email address")
    
    # AWS configs
    AWS_SECRET_NAME: str = Field(default="", description="AWS Secrets Manager secret name")

    # Computed properties
    REDSHIFT_JDBC_URL: str = ""
    CDP_REDSHIFT_JDBC_URL: str = ""
    JCAP_REDSHIFT_JDBC_URL: str = ""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        validate_assignment=True
    )
    
    def __init__(self, **data):
        """Initialize settings and load from Secrets Manager"""

        # First, initialize with defaults and env vars
        super().__init__(**data)
        
        # Then load sensitive values from Secrets Manager
        self._load_from_secrets_manager()
        
        # Finally, compute derived values
        self._compute_jdbc_urls()
    
    def _load_from_secrets_manager(self):
        """Load sensitive configuration from AWS Secrets Manager"""
        try:
            from utils.secrets_manager import get_secrets_manager
            
            logger.info(f"🔐 Loading configuration from AWS Secrets Manager")
            #secrets_name = self._secrets_name
            #secrets_manager = get_secrets_manager(secret_name=secrets_name)
            secrets_manager = get_secrets_manager()
            logger.info(f"📋 Using secret: {secrets_manager.secret_name}")
            
            # Mapping of secret keys to configuration fields
            secret_mappings = {
                # POC Redshift
                "REDSHIFT_HOST": "REDSHIFT_HOST",
                "REDSHIFT_PORT": "REDSHIFT_PORT",
                "REDSHIFT_DATABASE": "REDSHIFT_DATABASE", 
                "REDSHIFT_USER": "REDSHIFT_USER",
                "REDSHIFT_PASSWORD": "REDSHIFT_PASSWORD",
                
                # CDP Redshift
                "CDP_REDSHIFT_HOST": "CDP_REDSHIFT_HOST",
                "CDP_REDSHIFT_PORT": "CDP_REDSHIFT_PORT",
                "CDP_REDSHIFT_DATABASE": "CDP_REDSHIFT_DATABASE",
                "CDP_REDSHIFT_USER": "CDP_REDSHIFT_USER", 
                "CDP_REDSHIFT_PASSWORD": "CDP_REDSHIFT_PASSWORD",
                "CDP_REDSHIFT_SCHEMA": "CDP_REDSHIFT_SCHEMA",
                
                # JCAP Redshift
                "JCAP_REDSHIFT_HOST": "JCAP_REDSHIFT_HOST",
                "JCAP_REDSHIFT_PORT": "JCAP_REDSHIFT_PORT",
                "JCAP_REDSHIFT_DATABASE": "JCAP_REDSHIFT_DATABASE",
                "JCAP_REDSHIFT_USER": "JCAP_REDSHIFT_USER",
                "JCAP_REDSHIFT_PASSWORD": "JCAP_REDSHIFT_PASSWORD",
                "JCAP_REDSHIFT_SCHEMA": "JCAP_REDSHIFT_SCHEMA",

                # K8s Configuration
                "K8S_MASTER_URL": "K8S_MASTER_URL",
                "K8S_NAMESPACE": "K8S_NAMESPACE",
                "K8S_SERVICE_ACCOUNT": "K8S_SERVICE_ACCOUNT",
                "K8S_CONTAINER_IMAGE": "K8S_CONTAINER_IMAGE",
                "K8S_IMAGE_PULL_SECRETS": "K8S_IMAGE_PULL_SECRETS",

                # S3 Configuration
                "S3_BUCKET": "S3_BUCKET",
                "S3_REGION": "S3_REGION",
                "S3_IAM_ROLE": "S3_IAM_ROLE",
                
                # other configurations
                "AWS_SECRET_NAME": "AWS_SECRET_NAME",
                "SPARK_DRIVER_HOST": "SPARK_DRIVER_HOST",

            }
            
            # Load each secret value
            loaded_count = 0
            for secret_key, config_field in secret_mappings.items():
                secret_value = secrets_manager.get_secret_value(secret_key)
                if secret_value:
                    setattr(self, config_field, secret_value)
                    loaded_count += 1
            
            logger.info(f"✅ Loaded {loaded_count} configuration values from Secrets Manager")
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to load from Secrets Manager, using defaults: {str(e)}")
            logger.warning("Falling back to environment variables if available")
    
    def _compute_jdbc_urls(self):
        """Compute JDBC URLs from individual components"""
        if self.REDSHIFT_HOST:
            self.REDSHIFT_JDBC_URL = f"jdbc:redshift://{self.REDSHIFT_HOST}:{self.REDSHIFT_PORT}/{self.REDSHIFT_DATABASE}"
        
        if self.CDP_REDSHIFT_HOST:
            self.CDP_REDSHIFT_JDBC_URL = f"jdbc:redshift://{self.CDP_REDSHIFT_HOST}:{self.CDP_REDSHIFT_PORT}/{self.CDP_REDSHIFT_DATABASE}"
        
        if self.JCAP_REDSHIFT_HOST:
            self.JCAP_REDSHIFT_JDBC_URL = f"jdbc:redshift://{self.JCAP_REDSHIFT_HOST}:{self.JCAP_REDSHIFT_PORT}/{self.JCAP_REDSHIFT_DATABASE}"
    
    @validator('DATA_VARIANCE_THRESHOLD')
    def validate_variance_threshold(cls, v):
        if v < 0 or v > 100:
            raise ValueError('DATA_VARIANCE_THRESHOLD must be between 0 and 100')
        return v
    
    @validator('LOG_LEVEL')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'LOG_LEVEL must be one of {valid_levels}')
        return v.upper()
    
    def validate_for_job_type(self, job_type: str) -> None:
        """Validate that required configurations are present for specific job types."""
        if job_type == "jcap_pa_etl":
            missing_fields = []
            
            # Check CDP configuration
            if not self.CDP_REDSHIFT_HOST:
                missing_fields.append("CDP_REDSHIFT_HOST")
            if not self.CDP_REDSHIFT_DATABASE:
                missing_fields.append("CDP_REDSHIFT_DATABASE")
            if not self.CDP_REDSHIFT_USER:
                missing_fields.append("CDP_REDSHIFT_USER")
            if not self.CDP_REDSHIFT_PASSWORD:
                missing_fields.append("CDP_REDSHIFT_PASSWORD")
            
            # Check JCAP configuration
            if not self.JCAP_REDSHIFT_HOST:
                missing_fields.append("JCAP_REDSHIFT_HOST")
            if not self.JCAP_REDSHIFT_DATABASE:
                missing_fields.append("JCAP_REDSHIFT_DATABASE")
            if not self.JCAP_REDSHIFT_USER:
                missing_fields.append("JCAP_REDSHIFT_USER")
            if not self.JCAP_REDSHIFT_PASSWORD:
                missing_fields.append("JCAP_REDSHIFT_PASSWORD")
            
            # Check S3 configuration
            if not self.S3_BUCKET:
                missing_fields.append("S3_BUCKET")
            if not self.S3_IAM_ROLE:
                missing_fields.append("S3_IAM_ROLE")
            
            if missing_fields:
                raise ValueError(
                    f"Missing required configuration for {job_type}: {', '.join(missing_fields)}"
                )

def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()