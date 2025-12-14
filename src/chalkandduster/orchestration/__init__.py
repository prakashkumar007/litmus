"""
Chalk and Duster - Orchestration Module

Provides abstraction layer for different orchestration engines:
- Apache Airflow
- AWS Lambda

The orchestration engine can be configured via ORCHESTRATION_ENGINE setting.
"""

from chalkandduster.orchestration.airflow_engine import AirflowOrchestrationEngine
from chalkandduster.orchestration.base import (
    JobPayload,
    JobResult,
    JobStatus,
    JobType,
    OrchestrationEngine,
)
from chalkandduster.orchestration.factory import get_orchestration_engine
from chalkandduster.orchestration.lambda_engine import LambdaOrchestrationEngine

__all__ = [
    "OrchestrationEngine",
    "AirflowOrchestrationEngine",
    "LambdaOrchestrationEngine",
    "JobPayload",
    "JobResult",
    "JobStatus",
    "JobType",
    "get_orchestration_engine",
]

