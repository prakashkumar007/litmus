"""
Chalk and Duster - Prometheus Metrics
"""

from prometheus_client import Counter, Histogram, Gauge, Info

# Application info
APP_INFO = Info(
    "chalkandduster_app",
    "Chalk and Duster application information",
)
APP_INFO.info({
    "version": "0.1.0",
    "name": "chalkandduster",
})

# Quality check metrics
QUALITY_CHECKS_TOTAL = Counter(
    "chalkandduster_quality_checks_total",
    "Total number of quality check runs",
    ["tenant_id", "dataset_id", "status"],
)

QUALITY_CHECKS_FAILED = Counter(
    "chalkandduster_quality_checks_failed_total",
    "Total number of failed quality checks",
    ["tenant_id", "dataset_id", "check_name", "severity"],
)

QUALITY_CHECK_DURATION = Histogram(
    "chalkandduster_quality_check_duration_seconds",
    "Duration of quality check runs",
    ["tenant_id", "dataset_id"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

# Drift detection metrics
DRIFT_DETECTIONS_TOTAL = Counter(
    "chalkandduster_drift_detections_total",
    "Total number of drift detection runs",
    ["tenant_id", "dataset_id", "status"],
)

DRIFT_DETECTED = Counter(
    "chalkandduster_drift_detected_total",
    "Total number of drift events detected",
    ["tenant_id", "dataset_id", "drift_type"],
)

DRIFT_DETECTION_DURATION = Histogram(
    "chalkandduster_drift_detection_duration_seconds",
    "Duration of drift detection runs",
    ["tenant_id", "dataset_id"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

# LLM metrics
LLM_REQUESTS_TOTAL = Counter(
    "chalkandduster_llm_requests_total",
    "Total number of LLM requests",
    ["operation", "status"],
)

LLM_REQUEST_DURATION = Histogram(
    "chalkandduster_llm_request_duration_seconds",
    "Duration of LLM requests",
    ["operation"],
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120],
)

LLM_TOKENS_USED = Counter(
    "chalkandduster_llm_tokens_total",
    "Total number of LLM tokens used",
    ["operation", "token_type"],
)

# API metrics
API_REQUEST_DURATION = Histogram(
    "chalkandduster_api_request_duration_seconds",
    "Duration of API requests",
    ["method", "endpoint", "status_code"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
)

API_REQUESTS_TOTAL = Counter(
    "chalkandduster_api_requests_total",
    "Total number of API requests",
    ["method", "endpoint", "status_code"],
)

# Alert metrics
ALERTS_SENT_TOTAL = Counter(
    "chalkandduster_alerts_sent_total",
    "Total number of alerts sent",
    ["tenant_id", "alert_type", "channel"],
)

# Active datasets gauge
ACTIVE_DATASETS = Gauge(
    "chalkandduster_active_datasets",
    "Number of active datasets",
    ["tenant_id"],
)


def record_quality_check(
    tenant_id: str,
    dataset_id: str,
    status: str,
    duration_seconds: float,
    failed_checks: int = 0,
) -> None:
    """Record metrics for a quality check run."""
    QUALITY_CHECKS_TOTAL.labels(
        tenant_id=tenant_id,
        dataset_id=dataset_id,
        status=status,
    ).inc()
    
    QUALITY_CHECK_DURATION.labels(
        tenant_id=tenant_id,
        dataset_id=dataset_id,
    ).observe(duration_seconds)


def record_drift_detection(
    tenant_id: str,
    dataset_id: str,
    status: str,
    duration_seconds: float,
    drift_count: int = 0,
) -> None:
    """Record metrics for a drift detection run."""
    DRIFT_DETECTIONS_TOTAL.labels(
        tenant_id=tenant_id,
        dataset_id=dataset_id,
        status=status,
    ).inc()
    
    DRIFT_DETECTION_DURATION.labels(
        tenant_id=tenant_id,
        dataset_id=dataset_id,
    ).observe(duration_seconds)


def record_llm_request(
    operation: str,
    status: str,
    duration_seconds: float,
    input_tokens: int = 0,
    output_tokens: int = 0,
) -> None:
    """Record metrics for an LLM request."""
    LLM_REQUESTS_TOTAL.labels(
        operation=operation,
        status=status,
    ).inc()
    
    LLM_REQUEST_DURATION.labels(
        operation=operation,
    ).observe(duration_seconds)
    
    if input_tokens > 0:
        LLM_TOKENS_USED.labels(
            operation=operation,
            token_type="input",
        ).inc(input_tokens)
    
    if output_tokens > 0:
        LLM_TOKENS_USED.labels(
            operation=operation,
            token_type="output",
        ).inc(output_tokens)

