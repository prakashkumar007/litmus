"""
Chalk and Duster - Orchestration Base Classes

Abstract base class for orchestration engines.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID


class JobType(str, Enum):
    """Types of jobs that can be orchestrated."""
    QUALITY_CHECK = "quality_check"
    DRIFT_DETECTION = "drift_detection"


class JobStatus(str, Enum):
    """Status of an orchestrated job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class JobPayload:
    """Payload for an orchestration job."""
    job_type: JobType
    dataset_id: UUID
    tenant_id: UUID
    connection_id: UUID
    table_name: str
    database_name: str
    schema_name: str
    config_yaml: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert payload to dictionary for serialization."""
        return {
            "job_type": self.job_type.value,
            "dataset_id": str(self.dataset_id),
            "tenant_id": str(self.tenant_id),
            "connection_id": str(self.connection_id),
            "table_name": self.table_name,
            "database_name": self.database_name,
            "schema_name": self.schema_name,
            "config_yaml": self.config_yaml,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobPayload":
        """Create payload from dictionary."""
        return cls(
            job_type=JobType(data["job_type"]),
            dataset_id=UUID(data["dataset_id"]),
            tenant_id=UUID(data["tenant_id"]),
            connection_id=UUID(data["connection_id"]),
            table_name=data["table_name"],
            database_name=data["database_name"],
            schema_name=data["schema_name"],
            config_yaml=data["config_yaml"],
            metadata=data.get("metadata", {}),
        )


@dataclass
class JobResult:
    """Result of an orchestration job."""
    job_id: str
    status: JobStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    results: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "results": self.results,
            "error_message": self.error_message,
        }


class OrchestrationEngine(ABC):
    """Abstract base class for orchestration engines."""
    
    @abstractmethod
    async def trigger_job(self, payload: JobPayload) -> str:
        """
        Trigger a job for execution.
        
        Args:
            payload: The job payload
            
        Returns:
            Job ID for tracking
        """
        pass
    
    @abstractmethod
    async def get_job_status(self, job_id: str) -> JobResult:
        """
        Get the status of a job.
        
        Args:
            job_id: The job ID
            
        Returns:
            JobResult with current status
        """
        pass
    
    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running job.
        
        Args:
            job_id: The job ID
            
        Returns:
            True if cancelled successfully
        """
        pass
    
    @abstractmethod
    async def list_jobs(
        self,
        job_type: Optional[JobType] = None,
        status: Optional[JobStatus] = None,
        limit: int = 100,
    ) -> List[JobResult]:
        """
        List jobs with optional filters.
        
        Args:
            job_type: Filter by job type
            status: Filter by status
            limit: Maximum number of results
            
        Returns:
            List of job results
        """
        pass

