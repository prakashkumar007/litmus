"""
Chalk and Duster - Drift Detection Models

Shared data models for drift detection results.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID


@dataclass
class DriftResult:
    """Result of a single drift check."""
    
    monitor_name: str
    drift_type: str  # schema, volume, distribution, dataset
    detected: bool
    severity: str = "warning"  # info, warning, critical
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)
    message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "monitor_name": self.monitor_name,
            "drift_type": self.drift_type,
            "detected": self.detected,
            "severity": self.severity,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "details": self.details,
            "message": self.message,
        }


@dataclass
class DriftRunResult:
    """Result of a drift detection run."""

    run_id: UUID
    dataset_id: UUID
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "pending"  # pending, running, completed, failed
    results: List[DriftResult] = field(default_factory=list)
    html_report: Optional[str] = None  # Native Evidently HTML report

    @property
    def total_monitors(self) -> int:
        return len(self.results)

    @property
    def drift_detected_count(self) -> int:
        return sum(1 for r in self.results if r.detected)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "run_id": str(self.run_id),
            "dataset_id": str(self.dataset_id),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "total_monitors": self.total_monitors,
            "drift_detected_count": self.drift_detected_count,
            "results": [r.to_dict() for r in self.results],
        }

