"""
Chalk and Duster - Quality Check Models

Shared data models for quality check execution results.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID


@dataclass
class CheckResult:
    """Result of a single quality check."""
    
    check_name: str
    check_type: str
    status: str  # passed, failed, error
    severity: str = "warning"  # info, warning, critical
    expected: Optional[str] = None
    actual: Optional[str] = None
    failure_count: int = 0
    message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "check_name": self.check_name,
            "check_type": self.check_type,
            "status": self.status,
            "severity": self.severity,
            "expected": self.expected,
            "actual": self.actual,
            "failure_count": self.failure_count,
            "message": self.message,
        }


@dataclass
class QualityRunResult:
    """Result of a quality check run."""

    run_id: UUID
    dataset_id: UUID
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "pending"  # pending, running, completed, failed
    results: List[CheckResult] = field(default_factory=list)
    html_report: Optional[str] = None  # Native Great Expectations HTML report

    @property
    def total_checks(self) -> int:
        return len(self.results)

    @property
    def passed_checks(self) -> int:
        return sum(1 for r in self.results if r.status == "passed")

    @property
    def failed_checks(self) -> int:
        return sum(1 for r in self.results if r.status == "failed")

    @property
    def error_checks(self) -> int:
        return sum(1 for r in self.results if r.status == "error")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "run_id": str(self.run_id),
            "dataset_id": str(self.dataset_id),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "error_checks": self.error_checks,
            "results": [r.to_dict() for r in self.results],
        }

