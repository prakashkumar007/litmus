"""
Chalk and Duster - AWS Secrets Manager Integration

Provides secure credential retrieval from AWS Secrets Manager.
Works with both real AWS and LocalStack.
"""

import json
from typing import Any, Dict, Optional

import aioboto3
import structlog

from chalkandduster.core.config import settings

logger = structlog.get_logger()


async def get_connection_credentials(secret_arn: Optional[str]) -> Dict[str, Any]:
    """
    Retrieve connection credentials from AWS Secrets Manager.
    
    Args:
        secret_arn: The ARN or name of the secret in Secrets Manager.
                   If None, returns empty credentials (uses environment defaults).
    
    Returns:
        Dictionary containing credential fields (user, password, private_key, etc.)
    
    Raises:
        Exception: If secret retrieval fails.
    """
    if not secret_arn:
        # Return default credentials from environment for development
        logger.warning(
            "No secret_arn provided, using environment defaults",
        )
        return {
            "user": settings.SNOWFLAKE_USER,
            "password": settings.SNOWFLAKE_PASSWORD,
        }
    
    session = aioboto3.Session()
    
    # Configure endpoint for LocalStack if needed
    client_kwargs: Dict[str, Any] = {
        "region_name": settings.AWS_DEFAULT_REGION,
    }
    
    if settings.AWS_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = settings.AWS_ENDPOINT_URL
        client_kwargs["aws_access_key_id"] = settings.AWS_ACCESS_KEY_ID
        client_kwargs["aws_secret_access_key"] = settings.AWS_SECRET_ACCESS_KEY
    
    try:
        async with session.client("secretsmanager", **client_kwargs) as client:
            response = await client.get_secret_value(SecretId=secret_arn)
            
            # Parse the secret string as JSON
            secret_string = response.get("SecretString")
            if not secret_string:
                raise ValueError(f"Secret {secret_arn} has no SecretString")
            
            credentials = json.loads(secret_string)
            
            logger.info(
                "Retrieved credentials from Secrets Manager",
                secret_arn=secret_arn,
                has_user=bool(credentials.get("user")),
                has_password=bool(credentials.get("password")),
                has_private_key=bool(credentials.get("private_key")),
            )
            
            return credentials
            
    except Exception as e:
        logger.error(
            "Failed to retrieve credentials from Secrets Manager",
            secret_arn=secret_arn,
            error=str(e),
        )
        raise


async def store_connection_credentials(
    secret_name: str,
    credentials: Dict[str, Any],
) -> str:
    """
    Store connection credentials in AWS Secrets Manager.
    
    Args:
        secret_name: The name for the secret.
        credentials: Dictionary containing credential fields.
    
    Returns:
        The ARN of the created/updated secret.
    """
    session = aioboto3.Session()
    
    client_kwargs: Dict[str, Any] = {
        "region_name": settings.AWS_DEFAULT_REGION,
    }
    
    if settings.AWS_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = settings.AWS_ENDPOINT_URL
        client_kwargs["aws_access_key_id"] = settings.AWS_ACCESS_KEY_ID
        client_kwargs["aws_secret_access_key"] = settings.AWS_SECRET_ACCESS_KEY
    
    try:
        async with session.client("secretsmanager", **client_kwargs) as client:
            secret_string = json.dumps(credentials)
            
            try:
                # Try to create new secret
                response = await client.create_secret(
                    Name=secret_name,
                    SecretString=secret_string,
                )
                secret_arn = response["ARN"]
                logger.info("Created new secret", secret_name=secret_name)
                
            except client.exceptions.ResourceExistsException:
                # Update existing secret
                response = await client.put_secret_value(
                    SecretId=secret_name,
                    SecretString=secret_string,
                )
                secret_arn = response["ARN"]
                logger.info("Updated existing secret", secret_name=secret_name)
            
            return secret_arn
            
    except Exception as e:
        logger.error(
            "Failed to store credentials in Secrets Manager",
            secret_name=secret_name,
            error=str(e),
        )
        raise

