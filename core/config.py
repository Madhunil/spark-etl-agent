"""
Configuration management for the Spark ETL application.
Loads settings from environment variables and provides them to the application.
"""
import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
from functools import lru_cache

# Load environment variables from .env file if present
load_dotenv()

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    Uses Pydantic for validation and default values.
    """
    
    # Spark Configuration
    SPARK_DRIVER_MEMORY: str
    SPARK_EXECUTOR_MEMORY: str
    SPARK_APP_NAME: str
    
    # Kubernetes Configuration
    K8S_MASTER_URL: str
    K8S_NAMESPACE: str
    K8S_SERVICE_ACCOUNT: str
    K8S_CONTAINER_IMAGE: str
    K8S_IMAGE_PULL_SECRETS: str
    
    # Redshift Configuration
    REDSHIFT_HOST: str
    REDSHIFT_PORT: str
    REDSHIFT_DATABASE: str
    REDSHIFT_USER: str
    REDSHIFT_PASSWORD: str
    REDSHIFT_JDBC_DRIVER_PATH: str
    
    # Logging Configuration
    LOG_LEVEL: str
    
    # Derived properties calculated at runtime
    REDSHIFT_JDBC_URL: str = ""
    
    # Pydantic v2 model configuration
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )
    
    def __init__(self, **data):
        """Initialize settings and compute derived values"""
        super().__init__(**data)
        # Construct the JDBC URL from host, port, and database
        self.REDSHIFT_JDBC_URL = f"jdbc:redshift://{self.REDSHIFT_HOST}:{self.REDSHIFT_PORT}/{self.REDSHIFT_DATABASE}"

@lru_cache()
def get_settings():
    """
    Get cached application settings.
    
    Returns:
        Settings: Application settings
    """
    return Settings()