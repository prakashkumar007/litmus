"""
Chalk and Duster - Orchestration Factory

Factory for creating orchestration engine instances based on configuration.
"""

from functools import lru_cache
from typing import Optional

import structlog

from chalkandduster.core.config import settings
from chalkandduster.orchestration.base import OrchestrationEngine

logger = structlog.get_logger()


@lru_cache
def get_orchestration_engine(
    engine_type: Optional[str] = None,
) -> OrchestrationEngine:
    """
    Get the configured orchestration engine.
    
    Args:
        engine_type: Override the configured engine type ("airflow" or "lambda")
        
    Returns:
        OrchestrationEngine instance
        
    Raises:
        ValueError: If unknown engine type
    """
    engine = engine_type or settings.ORCHESTRATION_ENGINE
    
    logger.info("Creating orchestration engine", engine_type=engine)
    
    if engine == "airflow":
        from chalkandduster.orchestration.airflow_engine import AirflowOrchestrationEngine
        return AirflowOrchestrationEngine()
    
    elif engine == "lambda":
        from chalkandduster.orchestration.lambda_engine import LambdaOrchestrationEngine
        return LambdaOrchestrationEngine()
    
    else:
        raise ValueError(
            f"Unknown orchestration engine: {engine}. "
            "Valid options are: 'airflow', 'lambda'"
        )

