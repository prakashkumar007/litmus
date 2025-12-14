"""
Chalk and Duster - Core Schemas

Pydantic models and dataclasses used across the application.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DatasetValidation:
    """Result of validating a dataset YAML configuration."""
    valid: bool
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    check_count: int = 0
    monitor_count: int = 0


@dataclass
class ConnectionTestResult:
    """Result of testing a database connection."""
    success: bool
    message: str
    latency_ms: float = 0.0
    snowflake_version: Optional[str] = None
    current_warehouse: Optional[str] = None
    current_database: Optional[str] = None
    current_role: Optional[str] = None


@dataclass
class DriftExplainResponse:
    """Response from LLM drift explanation."""
    success: bool
    summary: str
    changes: List[str] = field(default_factory=list)
    impact_assessment: str = ""
    recommendations: List[str] = field(default_factory=list)


@dataclass
class AlertEnhanceResponse:
    """Response from LLM alert enhancement."""
    success: bool
    summary: str
    severity: str = "medium"
    root_cause_hints: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    affected_systems: List[str] = field(default_factory=list)

