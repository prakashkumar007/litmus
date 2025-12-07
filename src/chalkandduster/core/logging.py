"""
Chalk and Duster - Structured Logging Configuration
"""

import logging
import sys
from typing import Any, Dict

import structlog

from chalkandduster.core.config import settings


def setup_logging() -> None:
    """Configure structured logging with structlog."""
    
    # Determine log level based on environment
    log_level = logging.DEBUG if settings.APP_DEBUG else logging.INFO
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )
    
    # Shared processors for all environments
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]
    
    # HIPAA mode: Filter PHI from logs
    if settings.HIPAA_MODE and settings.PHI_IN_LOGS == "never":
        shared_processors.append(phi_filter_processor)
    
    # Development: Pretty console output
    if settings.APP_ENV == "development":
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    else:
        # Production: JSON output for log aggregation
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def phi_filter_processor(
    logger: Any, method_name: str, event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Filter potential PHI from log events."""
    
    # List of keys that might contain PHI
    phi_keys = {
        "ssn", "social_security", "patient_id", "mrn", "medical_record",
        "first_name", "last_name", "full_name", "name", "email",
        "phone", "address", "dob", "date_of_birth", "diagnosis",
    }
    
    def mask_value(key: str, value: Any) -> Any:
        """Mask PHI values."""
        if isinstance(value, str) and len(value) > 0:
            return f"[REDACTED:{len(value)} chars]"
        return "[REDACTED]"
    
    # Filter PHI keys
    for key in list(event_dict.keys()):
        key_lower = key.lower()
        if any(phi_key in key_lower for phi_key in phi_keys):
            event_dict[key] = mask_value(key, event_dict[key])
    
    return event_dict


def get_logger(name: str = None) -> structlog.stdlib.BoundLogger:
    """Get a configured logger instance."""
    return structlog.get_logger(name)

