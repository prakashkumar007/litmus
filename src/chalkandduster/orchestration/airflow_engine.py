"""
Chalk and Duster - Airflow Orchestration Engine

Provides Airflow-based orchestration via the Airflow REST API.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx
import structlog

from chalkandduster.core.config import settings
from chalkandduster.orchestration.base import (
    JobPayload,
    JobResult,
    JobStatus,
    JobType,
    OrchestrationEngine,
)

logger = structlog.get_logger()


# Mapping from Airflow DAG run states to our JobStatus
AIRFLOW_STATE_MAPPING = {
    "queued": JobStatus.PENDING,
    "running": JobStatus.RUNNING,
    "success": JobStatus.COMPLETED,
    "failed": JobStatus.FAILED,
}


class AirflowOrchestrationEngine(OrchestrationEngine):
    """
    Apache Airflow-based orchestration engine.
    
    Uses the Airflow REST API to trigger DAGs and monitor runs.
    """
    
    def __init__(
        self,
        airflow_url: str = "http://localhost:8080",
        username: str = "admin",
        password: str = "admin",
    ):
        """Initialize the Airflow orchestration engine."""
        self.airflow_url = airflow_url.rstrip("/")
        self.auth = (username, password)
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=f"{self.airflow_url}/api/v1",
                auth=self.auth,
                timeout=30.0,
            )
        return self._client
    
    def _get_dag_id(self, job_type: JobType) -> str:
        """Get DAG ID for job type."""
        if job_type == JobType.QUALITY_CHECK:
            return "chalkandduster_quality_checks"
        elif job_type == JobType.DRIFT_DETECTION:
            return "chalkandduster_drift_detection"
        else:
            raise ValueError(f"Unknown job type: {job_type}")
    
    async def trigger_job(self, payload: JobPayload) -> str:
        """
        Trigger an Airflow DAG run.
        
        Uses the Airflow REST API to trigger DAGs with configuration.
        """
        dag_id = self._get_dag_id(payload.job_type)
        run_id = f"manual__{datetime.utcnow().isoformat()}_{uuid4().hex[:8]}"
        
        logger.info(
            "Triggering Airflow DAG",
            dag_id=dag_id,
            run_id=run_id,
            job_type=payload.job_type.value,
        )
        
        try:
            client = await self._get_client()
            
            response = await client.post(
                f"/dags/{dag_id}/dagRuns",
                json={
                    "dag_run_id": run_id,
                    "conf": payload.to_dict(),
                },
            )
            
            if response.status_code == 200:
                logger.info("Airflow DAG triggered successfully", run_id=run_id)
                return run_id
            else:
                logger.error(
                    "Failed to trigger Airflow DAG",
                    status_code=response.status_code,
                    response=response.text,
                )
                raise RuntimeError(f"Failed to trigger DAG: {response.text}")
                
        except Exception as e:
            logger.error("Airflow trigger failed", error=str(e))
            raise
    
    async def get_job_status(self, job_id: str) -> JobResult:
        """Get the status of an Airflow DAG run."""
        try:
            client = await self._get_client()
            
            # Try quality DAG first, then drift DAG
            for dag_id in ["chalkandduster_quality_checks", "chalkandduster_drift_detection"]:
                response = await client.get(f"/dags/{dag_id}/dagRuns/{job_id}")
                
                if response.status_code == 200:
                    data = response.json()
                    state = data.get("state", "unknown")
                    
                    return JobResult(
                        job_id=job_id,
                        status=AIRFLOW_STATE_MAPPING.get(state, JobStatus.PENDING),
                        started_at=datetime.fromisoformat(data["start_date"].replace("Z", "+00:00")) if data.get("start_date") else None,
                        completed_at=datetime.fromisoformat(data["end_date"].replace("Z", "+00:00")) if data.get("end_date") else None,
                    )
            
            return JobResult(job_id=job_id, status=JobStatus.FAILED, error_message="Job not found")
            
        except Exception as e:
            logger.error("Failed to get Airflow job status", job_id=job_id, error=str(e))
            return JobResult(job_id=job_id, status=JobStatus.FAILED, error_message=str(e))
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel an Airflow DAG run."""
        try:
            client = await self._get_client()
            
            for dag_id in ["chalkandduster_quality_checks", "chalkandduster_drift_detection"]:
                response = await client.patch(
                    f"/dags/{dag_id}/dagRuns/{job_id}",
                    json={"state": "failed"},
                )
                
                if response.status_code == 200:
                    return True
            
            return False
            
        except Exception as e:
            logger.error("Failed to cancel Airflow job", job_id=job_id, error=str(e))
            return False
    
    async def list_jobs(
        self,
        job_type: Optional[JobType] = None,
        status: Optional[JobStatus] = None,
        limit: int = 100,
    ) -> List[JobResult]:
        """List Airflow DAG runs with optional filters."""
        results: List[JobResult] = []
        
        try:
            client = await self._get_client()
            
            dag_ids = []
            if job_type:
                dag_ids = [self._get_dag_id(job_type)]
            else:
                dag_ids = ["chalkandduster_quality_checks", "chalkandduster_drift_detection"]
            
            for dag_id in dag_ids:
                response = await client.get(
                    f"/dags/{dag_id}/dagRuns",
                    params={"limit": limit},
                )
                
                if response.status_code == 200:
                    data = response.json()
                    for run in data.get("dag_runs", []):
                        job_status = AIRFLOW_STATE_MAPPING.get(run.get("state"), JobStatus.PENDING)
                        
                        if status and job_status != status:
                            continue
                        
                        results.append(JobResult(
                            job_id=run["dag_run_id"],
                            status=job_status,
                            started_at=datetime.fromisoformat(run["start_date"].replace("Z", "+00:00")) if run.get("start_date") else None,
                            completed_at=datetime.fromisoformat(run["end_date"].replace("Z", "+00:00")) if run.get("end_date") else None,
                        ))
                        
                        if len(results) >= limit:
                            break
                
                if len(results) >= limit:
                    break
                    
        except Exception as e:
            logger.error("Failed to list Airflow jobs", error=str(e))
        
        return results

