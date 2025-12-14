"""
Chalk and Duster - AWS Lambda Orchestration Engine

Provides Lambda-based orchestration as an alternative to Airflow.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

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


class LambdaOrchestrationEngine(OrchestrationEngine):
    """
    AWS Lambda-based orchestration engine.
    
    Uses Lambda for serverless execution of quality checks and drift detection.
    Supports both synchronous and asynchronous invocation patterns.
    """
    
    def __init__(self):
        """Initialize the Lambda orchestration engine."""
        self._job_store: Dict[str, Dict[str, Any]] = {}
        self._client = None
    
    async def _get_client(self):
        """Get or create Lambda client."""
        if self._client is None:
            import aioboto3
            session = aioboto3.Session()
            self._client = await session.client(
                "lambda",
                endpoint_url=settings.AWS_ENDPOINT_URL,
                region_name=settings.AWS_DEFAULT_REGION,
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            ).__aenter__()
        return self._client
    
    def _get_function_name(self, job_type: JobType) -> str:
        """Get Lambda function name for job type."""
        if job_type == JobType.QUALITY_CHECK:
            return settings.LAMBDA_QUALITY_FUNCTION_NAME
        elif job_type == JobType.DRIFT_DETECTION:
            return settings.LAMBDA_DRIFT_FUNCTION_NAME
        else:
            raise ValueError(f"Unknown job type: {job_type}")
    
    async def trigger_job(self, payload: JobPayload) -> str:
        """
        Trigger a Lambda function for the job.
        
        Uses async invocation for long-running jobs.
        """
        job_id = str(uuid4())
        function_name = self._get_function_name(payload.job_type)
        
        logger.info(
            "Triggering Lambda job",
            job_id=job_id,
            function_name=function_name,
            job_type=payload.job_type.value,
        )
        
        # Store job metadata
        self._job_store[job_id] = {
            "status": JobStatus.PENDING,
            "payload": payload.to_dict(),
            "started_at": datetime.utcnow(),
            "function_name": function_name,
        }
        
        try:
            client = await self._get_client()
            
            # Add job_id to payload for tracking
            invoke_payload = payload.to_dict()
            invoke_payload["job_id"] = job_id
            
            # Invoke Lambda asynchronously (Event invocation type)
            response = await client.invoke(
                FunctionName=function_name,
                InvocationType="Event",  # Async invocation
                Payload=json.dumps(invoke_payload),
            )
            
            if response["StatusCode"] == 202:  # Accepted
                self._job_store[job_id]["status"] = JobStatus.RUNNING
                logger.info("Lambda job triggered successfully", job_id=job_id)
            else:
                self._job_store[job_id]["status"] = JobStatus.FAILED
                self._job_store[job_id]["error_message"] = f"Unexpected status: {response['StatusCode']}"
                
        except Exception as e:
            logger.error("Failed to trigger Lambda job", job_id=job_id, error=str(e))
            self._job_store[job_id]["status"] = JobStatus.FAILED
            self._job_store[job_id]["error_message"] = str(e)
        
        return job_id
    
    async def get_job_status(self, job_id: str) -> JobResult:
        """Get the status of a Lambda job."""
        if job_id not in self._job_store:
            return JobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message="Job not found",
            )
        
        job_data = self._job_store[job_id]
        
        return JobResult(
            job_id=job_id,
            status=job_data["status"],
            started_at=job_data.get("started_at"),
            completed_at=job_data.get("completed_at"),
            results=job_data.get("results"),
            error_message=job_data.get("error_message"),
        )
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a Lambda job (marks as cancelled, Lambda cannot be stopped mid-execution)."""
        if job_id in self._job_store:
            self._job_store[job_id]["status"] = JobStatus.CANCELLED
            return True
        return False
    
    async def list_jobs(
        self,
        job_type: Optional[JobType] = None,
        status: Optional[JobStatus] = None,
        limit: int = 100,
    ) -> List[JobResult]:
        """List Lambda jobs with optional filters."""
        results = []
        
        for job_id, job_data in self._job_store.items():
            # Apply filters
            if job_type and job_data["payload"].get("job_type") != job_type.value:
                continue
            if status and job_data["status"] != status:
                continue
                
            results.append(JobResult(
                job_id=job_id,
                status=job_data["status"],
                started_at=job_data.get("started_at"),
                completed_at=job_data.get("completed_at"),
                results=job_data.get("results"),
                error_message=job_data.get("error_message"),
            ))
            
            if len(results) >= limit:
                break
        
        return results

