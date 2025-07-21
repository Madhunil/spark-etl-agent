"""
AWS Secrets Manager utility for retrieving secure configuration values.
Environment-aware implementation supporting dev/prod with same variable structure.

Usage:
- Dev environment: AWS_SECRET_NAME=jph-eks-dev-secret
- Prod environment: AWS_SECRET_NAME=jph-eks-prod-secret
- Both use same variable names (REDSHIFT_HOST, CDP_REDSHIFT_HOST, K8S_MASTER_URL, etc.)
"""
import os
import json
import boto3
from botocore.exceptions import ClientError
from loguru import logger
from typing import Dict, Any, Optional
from core.config import get_settings

class SecretsManager:
    """
    Utility class for retrieving configuration from AWS Secrets Manager.
    Handles caching and error recovery.
    """
    
    def __init__(self, secret_name: str = None, region_name: str = "us-east-1"):
        """
        Initialize Secrets Manager client.
        
        Args:
            secret_name: Name of the secret in AWS Secrets Manager (auto-detected if None)
            region_name: AWS region where secret is stored
        """
        # Auto-detect secret name based on environment if not provided
        if secret_name is None:
            # Check for environment-specific secret name
            secret_name = os.environ.get('AWS_SECRET_NAME', 'jph-eks-dev-secret')
        
        
        #self.settings = get_settings()
        #self.secret_name = self.settings.AWS_SECRET_NAME #secret_name
        self.secret_name = secret_name
        self.region_name = region_name
        self._cached_secret = None
        
        # Create Secrets Manager client
        session = boto3.session.Session()
        self.client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        
        logger.info(f"🔐 Secrets Manager initialized for secret: {secret_name}")
        logger.info(f"🌍 Region: {region_name}")
    
    def get_secret_values(self) -> Dict[str, Any]:
        """
        Retrieve and parse secret values from AWS Secrets Manager.
        
        Returns:
            Dictionary containing all secret key-value pairs
        """
        if self._cached_secret is not None:
            return self._cached_secret
        
        try:
            logger.info(f"🔍 Retrieving secret: {self.secret_name}")
            
            get_secret_value_response = self.client.get_secret_value(
                SecretId=self.secret_name
            )
            
            secret_string = get_secret_value_response['SecretString']
            secret_dict = json.loads(secret_string)
            
            # Cache the secret to avoid repeated API calls
            self._cached_secret = secret_dict
            
            logger.info(f"✅ Successfully retrieved secret with {len(secret_dict)} keys")
            logger.debug(f"🔑 Secret keys: {list(secret_dict.keys())}")
            return secret_dict
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"❌ Failed to retrieve secret {self.secret_name}: {error_code}")
            
            if error_code == 'DecryptionFailureException':
                logger.error("Secret can't be decrypted using the provided KMS key")
            elif error_code == 'InternalServiceErrorException':
                logger.error("An error occurred on the server side")
            elif error_code == 'InvalidParameterException':
                logger.error("Invalid parameter provided")
            elif error_code == 'InvalidRequestException':
                logger.error("Invalid request parameter")
            elif error_code == 'ResourceNotFoundException':
                logger.error("Secret not found - please verify secret name and permissions")
            elif error_code == 'AccessDeniedException':
                logger.error("Access denied - please verify IAM permissions for Secrets Manager")
            
            raise e
        except json.JSONDecodeError as e:
            logger.error(f"❌ Failed to parse secret JSON: {str(e)}")
            raise e
        except Exception as e:
            logger.error(f"❌ Unexpected error retrieving secret: {str(e)}")
            raise e
    
    def get_secret_value(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a specific value from the secret.
        
        Args:
            key: Key to retrieve from secret
            default: Default value if key not found
            
        Returns:
            Secret value or default
        """
        try:
            secret_dict = self.get_secret_values()
            value = secret_dict.get(key, default)
            if value:
                logger.debug(f"🔑 Retrieved {key}: {'*' * min(len(str(value)), 8)}")
            return value
        except Exception as e:
            logger.warning(f"⚠️ Failed to get secret value for {key}, using default: {str(e)}")
            return default

# Global instance for reuse
_secrets_manager = None

def get_secrets_manager() -> SecretsManager:
    """Get or create global SecretsManager instance."""
    global _secrets_manager
    if _secrets_manager is None:
        _secrets_manager = SecretsManager()
    return _secrets_manager
    #return SecretsManager(secret_name=secret_name)