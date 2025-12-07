"""Observability module - Prometheus metrics and tracing."""

from chalkandduster.observability.metrics import (
    QUALITY_CHECKS_TOTAL,
    QUALITY_CHECKS_FAILED,
    DRIFT_DETECTIONS_TOTAL,
    DRIFT_DETECTED,
    LLM_REQUESTS_TOTAL,
    LLM_REQUEST_DURATION,
    API_REQUEST_DURATION,
    record_quality_check,
    record_drift_detection,
    record_llm_request,
)

__all__ = [
    "QUALITY_CHECKS_TOTAL",
    "QUALITY_CHECKS_FAILED",
    "DRIFT_DETECTIONS_TOTAL",
    "DRIFT_DETECTED",
    "LLM_REQUESTS_TOTAL",
    "LLM_REQUEST_DURATION",
    "API_REQUEST_DURATION",
    "record_quality_check",
    "record_drift_detection",
    "record_llm_request",
]

