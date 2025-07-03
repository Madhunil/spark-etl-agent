"""
Configuration management for the Spark ETL application.
Loads settings from environment variables and provides them to the application.
Optimized for production use with proper validation and error handling.
"""
import os
from typing import Optional
from pydantic import Field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
from functools import lru_cache
from loguru import logger

# Load environment variables from .env file if present
load_dotenv()

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    Enhanced with validation and better structure.
    """
    
    # Spark Configuration
    SPARK_DRIVER_MEMORY: str = Field(default="5g", description="Spark driver memory")
    SPARK_EXECUTOR_MEMORY: str = Field(default="5g", description="Spark executor memory")
    SPARK_APP_NAME: str = Field(default="jph-etl-spark-app", description="Spark application name")
    
    # Kubernetes Configuration
    K8S_MASTER_URL: str = Field(description="Kubernetes master URL")
    K8S_NAMESPACE: str = Field(description="Kubernetes namespace")
    K8S_SERVICE_ACCOUNT: str = Field(description="Kubernetes service account")
    K8S_CONTAINER_IMAGE: str = Field(description="Container image for Spark executors")
    K8S_IMAGE_PULL_SECRETS: str = Field(description="Image pull secrets")
    
    # Redshift Configuration - POC/Development
    REDSHIFT_HOST: str = Field(description="POC Redshift host")
    REDSHIFT_PORT: str = Field(default="5439", description="POC Redshift port")
    REDSHIFT_DATABASE: str = Field(description="POC Redshift database")
    REDSHIFT_USER: str = Field(description="POC Redshift user")
    REDSHIFT_PASSWORD: str = Field(description="POC Redshift password")
    REDSHIFT_JDBC_DRIVER_PATH: str = Field(description="JDBC driver path")
    
    # CDP Redshift Configuration - Production Source
    CDP_REDSHIFT_HOST: str = Field(description="CDP Redshift host")
    CDP_REDSHIFT_PORT: str = Field(default="5439", description="CDP Redshift port")
    CDP_REDSHIFT_DATABASE: str = Field(description="CDP Redshift database")
    CDP_REDSHIFT_USER: str = Field(description="CDP Redshift user")
    CDP_REDSHIFT_PASSWORD: str = Field(description="CDP Redshift password")
    
    # JCAP Redshift Configuration - Production Destination
    JCAP_REDSHIFT_HOST: str = Field(description="JCAP Redshift host")
    JCAP_REDSHIFT_PORT: str = Field(default="5439", description="JCAP Redshift port")
    JCAP_REDSHIFT_DATABASE: str = Field(description="JCAP Redshift database")
    JCAP_REDSHIFT_USER: str = Field(description="JCAP Redshift user")
    JCAP_REDSHIFT_PASSWORD: str = Field(description="JCAP Redshift password")
    
    # S3 Configuration
    S3_BUCKET: str = Field(description="S3 bucket for data staging")
    S3_REGION: str = Field(default="us-east-1", description="S3 region")
    S3_IAM_ROLE: str = Field(description="IAM role for S3/Redshift operations")
    S3_ACCESS_KEY: Optional[str] = Field(default="", description="S3 access key (optional)")
    S3_SECRET_KEY: Optional[str] = Field(default="", description="S3 secret key (optional)")
    
    # Email Configuration
    SMTP_SERVER: str = Field(default="smtp.na.jnj.com", description="SMTP server")
    SMTP_PORT: int = Field(default=25, description="SMTP port")
    SMTP_USE_TLS: bool = Field(default=False, description="Use TLS for SMTP")
    SMTP_USERNAME: Optional[str] = Field(default="", description="SMTP username")
    SMTP_PASSWORD: Optional[str] = Field(default="", description="SMTP password")
    EMAIL_FROM: str = Field(description="From email address")
    EMAIL_TO_DNA_TEAM: str = Field(description="DNA team email address")
    
    # Application Configuration
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    DATA_VARIANCE_THRESHOLD: float = Field(default=5.0, description="Data variance threshold percentage")
    
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
        """Initialize settings and compute derived values"""
        super().__init__(**data)
        self._compute_jdbc_urls()
        self._validate_settings()
    
    def _compute_jdbc_urls(self):
        """Compute JDBC URLs from individual components"""
        self.REDSHIFT_JDBC_URL = f"jdbc:redshift://{self.REDSHIFT_HOST}:{self.REDSHIFT_PORT}/{self.REDSHIFT_DATABASE}"
        self.CDP_REDSHIFT_JDBC_URL = f"jdbc:redshift://{self.CDP_REDSHIFT_HOST}:{self.CDP_REDSHIFT_PORT}/{self.CDP_REDSHIFT_DATABASE}"
        self.JCAP_REDSHIFT_JDBC_URL = f"jdbc:redshift://{self.JCAP_REDSHIFT_HOST}:{self.JCAP_REDSHIFT_PORT}/{self.JCAP_REDSHIFT_DATABASE}"
    
    def _validate_settings(self):
        """Validate critical settings"""
        required_for_jcap_etl = [
            self.CDP_REDSHIFT_HOST, self.JCAP_REDSHIFT_HOST, 
            self.S3_BUCKET, self.S3_IAM_ROLE
        ]
        
        if any(not setting for setting in required_for_jcap_etl):
            logger.warning("Some JCAP PA ETL settings are missing - this job type may not work")
    
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

@lru_cache()
def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()