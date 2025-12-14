"""
Chalk and Duster - Quality Executor Factory

Factory for creating Great Expectations executor instances.
"""

from typing import Any, Dict, Protocol

import structlog

logger = structlog.get_logger()


class QualityExecutorProtocol(Protocol):
    """Protocol for quality executors."""

    async def execute(
        self,
        dataset_id: Any,
        quality_yaml: str,
        table_name: str,
    ) -> Any:
        """Execute quality checks."""
        ...


def get_quality_executor(
    connection_config: Dict[str, Any],
) -> QualityExecutorProtocol:
    """
    Get a Great Expectations quality executor.

    Args:
        connection_config: Database connection configuration

    Returns:
        GreatExpectationsExecutor instance
    """
    from chalkandduster.quality.great_expectations_executor import GreatExpectationsExecutor

    logger.info("Creating Great Expectations quality executor")

    return GreatExpectationsExecutor(connection_config=connection_config)

