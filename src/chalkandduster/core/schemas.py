"""
Chalk and Duster - Core Schemas

Pydantic models for type-safe data validation across the application.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SeverityLevel(str, Enum):
    """Severity levels for alerts and issues."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class ValidationError(BaseModel):
    """A single validation error."""
    type: str = Field(..., description="Error type identifier")
    message: str = Field(..., description="Human-readable error message")
    location: Optional[str] = Field(None, description="Location in YAML where error occurred")


class ValidationWarning(BaseModel):
    """A single validation warning."""
    type: str = Field(..., description="Warning type identifier")
    message: str = Field(..., description="Human-readable warning message")


class DatasetValidation(BaseModel):
    """Result of validating a dataset YAML configuration."""
    valid: bool = Field(..., description="Whether the configuration is valid")
    errors: List[Dict[str, Any]] = Field(default_factory=list, description="List of validation errors")
    warnings: List[Dict[str, Any]] = Field(default_factory=list, description="List of validation warnings")
    check_count: int = Field(default=0, description="Number of quality checks parsed")
    monitor_count: int = Field(default=0, description="Number of drift monitors parsed")

    class Config:
        frozen = False


class ConnectionTestResult(BaseModel):
    """Result of testing a database connection."""
    success: bool = Field(..., description="Whether connection test succeeded")
    message: str = Field(..., description="Status message")
    latency_ms: float = Field(default=0.0, description="Connection latency in milliseconds")
    snowflake_version: Optional[str] = Field(None, description="Snowflake version if connected")
    current_warehouse: Optional[str] = Field(None, description="Current active warehouse")
    current_database: Optional[str] = Field(None, description="Current database name")
    current_role: Optional[str] = Field(None, description="Current user role")


class DriftExplainResponse(BaseModel):
    """Response from LLM drift explanation."""
    success: bool = Field(..., description="Whether explanation generation succeeded")
    summary: str = Field(..., description="Brief summary of detected drift")
    changes: List[str] = Field(default_factory=list, description="List of detected changes")
    impact_assessment: str = Field(default="", description="Assessment of potential impact")
    recommendations: List[str] = Field(default_factory=list, description="Recommended actions")


class AlertEnhanceResponse(BaseModel):
    """Response from LLM alert enhancement."""
    success: bool = Field(..., description="Whether enhancement generation succeeded")
    summary: str = Field(..., description="Enhanced alert summary")
    severity: str = Field(default="warning", description="Alert severity level (info, warning, critical)")
    root_cause_hints: List[str] = Field(default_factory=list, description="Potential root causes")
    recommended_actions: List[str] = Field(default_factory=list, description="Recommended actions")
    slack_message: str = Field(default="", description="Formatted Slack message")

