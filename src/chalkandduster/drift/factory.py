"""
Chalk and Duster - Drift Detector Factory

Factory for creating Evidently drift detector instances.
"""

from typing import Any, Optional, Protocol

import structlog

logger = structlog.get_logger()


class DriftDetectorProtocol(Protocol):
    """Protocol for drift detectors."""

    async def detect(
        self,
        dataset_id: Any,
        drift_yaml: str,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        run_id: Optional[Any] = None,
    ) -> Any:
        """Detect drift in the data."""
        ...


def get_drift_detector(
    snowflake_connector: Any,
) -> DriftDetectorProtocol:
    """
    Get an Evidently drift detector.

    Args:
        snowflake_connector: Snowflake connector instance

    Returns:
        EvidentlyDriftDetector instance
    """
    from chalkandduster.drift.evidently_detector import EvidentlyDriftDetector

    logger.info("Creating Evidently drift detector")

    return EvidentlyDriftDetector(snowflake_connector=snowflake_connector)

