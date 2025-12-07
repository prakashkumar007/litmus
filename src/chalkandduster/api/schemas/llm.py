"""
Chalk and Duster - LLM API Schemas
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class YAMLGenerateRequest(BaseModel):
    """Request for generating YAML from natural language."""
    description: str = Field(
        ...,
        min_length=10,
        max_length=5000,
        description="Natural language description of the quality checks and drift monitors needed",
    )
    table_name: Optional[str] = Field(
        None,
        description="Name of the table to generate checks for",
    )
    schema_info: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional schema information (columns, types) for context",
    )
    include_quality: bool = Field(
        default=True,
        description="Generate data quality YAML",
    )
    include_drift: bool = Field(
        default=True,
        description="Generate data drift YAML",
    )


class YAMLGenerateResponse(BaseModel):
    """Response from YAML generation."""
    success: bool
    quality_yaml: Optional[str] = None
    drift_yaml: Optional[str] = None
    explanation: str = Field(
        ...,
        description="Human-readable explanation of the generated checks",
    )
    check_count: int = Field(default=0)
    monitor_count: int = Field(default=0)
    warnings: List[str] = Field(default_factory=list)


class AlertEnhanceRequest(BaseModel):
    """Request for enhancing an alert with LLM."""
    dataset_name: str
    run_id: str
    failures: List[Dict[str, Any]] = Field(
        ...,
        description="List of check failures with details",
    )
    context: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional context (historical data, patterns)",
    )


class AlertEnhanceResponse(BaseModel):
    """Response from alert enhancement."""
    success: bool
    summary: str = Field(
        ...,
        description="Human-readable summary of the issues",
    )
    root_cause_hints: List[str] = Field(
        default_factory=list,
        description="Possible root cause suggestions",
    )
    recommended_actions: List[str] = Field(
        default_factory=list,
        description="Recommended actions to address the issues",
    )
    severity: str = Field(
        default="warning",
        description="Overall severity assessment",
    )
    slack_message: str = Field(
        ...,
        description="Formatted Slack message ready to send",
    )


class DriftExplainRequest(BaseModel):
    """Request for explaining drift detection results."""
    dataset_name: str
    drift_results: List[Dict[str, Any]] = Field(
        ...,
        description="List of detected drift events",
    )
    baseline_info: Optional[Dict[str, Any]] = Field(
        None,
        description="Baseline statistics for comparison",
    )


class DriftExplainResponse(BaseModel):
    """Response from drift explanation."""
    success: bool
    summary: str = Field(
        ...,
        description="Plain English explanation of what changed",
    )
    changes: List[Dict[str, str]] = Field(
        default_factory=list,
        description="List of changes with explanations",
    )
    impact_assessment: str = Field(
        default="",
        description="Assessment of potential impact",
    )
    recommendations: List[str] = Field(
        default_factory=list,
        description="Recommendations for handling the drift",
    )

