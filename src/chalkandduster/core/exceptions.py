"""
Chalk and Duster - Custom Exceptions
"""

from typing import Any, Dict, Optional


class ChalkAndDusterError(Exception):
    """Base exception for Chalk and Duster."""
    
    def __init__(
        self,
        message: str,
        code: str = "INTERNAL_ERROR",
        details: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(self.message)


class ValidationError(ChalkAndDusterError):
    """Raised when validation fails."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="VALIDATION_ERROR", details=details)


class NotFoundError(ChalkAndDusterError):
    """Raised when a resource is not found."""
    
    def __init__(self, resource: str, identifier: str):
        super().__init__(
            message=f"{resource} not found: {identifier}",
            code="NOT_FOUND",
            details={"resource": resource, "identifier": identifier},
        )


class ConnectionError(ChalkAndDusterError):
    """Raised when a connection fails."""
    
    def __init__(self, message: str, connection_type: str = "unknown"):
        super().__init__(
            message=message,
            code="CONNECTION_ERROR",
            details={"connection_type": connection_type},
        )


class SnowflakeError(ChalkAndDusterError):
    """Raised when Snowflake operations fail."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="SNOWFLAKE_ERROR", details=details)


class LLMError(ChalkAndDusterError):
    """Raised when LLM operations fail."""
    
    def __init__(self, message: str, provider: str = "unknown"):
        super().__init__(
            message=message,
            code="LLM_ERROR",
            details={"provider": provider},
        )


class QualityCheckError(ChalkAndDusterError):
    """Raised when quality check execution fails."""
    
    def __init__(self, message: str, check_name: str = "unknown"):
        super().__init__(
            message=message,
            code="QUALITY_CHECK_ERROR",
            details={"check_name": check_name},
        )


class DriftDetectionError(ChalkAndDusterError):
    """Raised when drift detection fails."""
    
    def __init__(self, message: str, monitor_name: str = "unknown"):
        super().__init__(
            message=message,
            code="DRIFT_DETECTION_ERROR",
            details={"monitor_name": monitor_name},
        )


class RateLimitError(ChalkAndDusterError):
    """Raised when rate limit is exceeded."""
    
    def __init__(self, message: str = "Rate limit exceeded"):
        super().__init__(message, code="RATE_LIMIT_EXCEEDED")


class AuthenticationError(ChalkAndDusterError):
    """Raised when authentication fails."""
    
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message, code="AUTHENTICATION_ERROR")


class AuthorizationError(ChalkAndDusterError):
    """Raised when authorization fails."""
    
    def __init__(self, message: str = "Not authorized"):
        super().__init__(message, code="AUTHORIZATION_ERROR")

