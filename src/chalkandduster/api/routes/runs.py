"""
Chalk and Duster - Run Routes
"""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from chalkandduster.api.deps import get_db_session
from chalkandduster.db.postgres import crud

logger = structlog.get_logger()
router = APIRouter()


class CheckResult(BaseModel):
    """Individual check result."""
    check_name: str
    check_type: str
    status: str  # passed, failed, error
    severity: str  # critical, warning, info
    expected: Optional[str] = None
    actual: Optional[str] = None
    failure_count: int = 0
    message: Optional[str] = None


class RunResponse(BaseModel):
    """Schema for a check run response."""
    id: UUID
    dataset_id: UUID
    run_type: str  # quality, drift
    trigger_type: str = "on_demand"  # on_demand, scheduled
    status: str  # pending, running, completed, failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    # Results
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    error_checks: int = 0

    results: List[CheckResult] = Field(default_factory=list)


class RunListResponse(BaseModel):
    """Schema for run list response."""
    items: List[RunResponse]
    total: int
    page: int
    page_size: int


@router.get("", response_model=RunListResponse)
async def list_runs(
    dataset_id: UUID = Query(...),
    run_type: str = Query(None, pattern="^(quality|drift)$"),
    status_filter: str = Query(None, alias="status", pattern="^(pending|running|completed|failed)$"),
    start_date: datetime = Query(None),
    end_date: datetime = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db_session),
) -> RunListResponse:
    """
    List check runs for a dataset.
    """
    runs, total = await crud.list_runs(
        db,
        dataset_id=dataset_id,
        run_type=run_type,
        status=status_filter,
        page=page,
        page_size=page_size,
    )

    items = []
    for run in runs:
        # Parse results for the response
        check_results = []
        if run.results:
            for check in run.results.get("checks", run.results.get("monitors", [])):
                check_results.append(CheckResult(
                    check_name=check.get("check_name", check.get("monitor_name", "unknown")),
                    check_type=check.get("check_type", check.get("drift_type", "unknown")),
                    status=check.get("status", "unknown"),
                    severity=check.get("severity", "info"),
                    expected=check.get("expected"),
                    actual=check.get("actual"),
                    failure_count=check.get("failure_count", 0),
                    message=check.get("message"),
                ))

        items.append(RunResponse(
            id=run.id,
            dataset_id=run.dataset_id,
            run_type=run.run_type,
            status=run.status,
            started_at=run.started_at,
            completed_at=run.completed_at,
            duration_seconds=run.duration_seconds,
            total_checks=run.total_checks,
            passed_checks=run.passed_checks,
            failed_checks=run.failed_checks,
            error_checks=run.error_checks,
            results=check_results,
        ))

    return RunListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{run_id}", response_model=RunResponse)
async def get_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_db_session),
) -> RunResponse:
    """
    Get details of a specific run.
    """
    run = await crud.get_run_by_id(db, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Run not found",
        )

    # Parse results for the response
    check_results = []
    if run.results:
        for check in run.results.get("checks", run.results.get("monitors", [])):
            check_results.append(CheckResult(
                check_name=check.get("check_name", check.get("monitor_name", "unknown")),
                check_type=check.get("check_type", check.get("drift_type", "unknown")),
                status=check.get("status", "unknown"),
                severity=check.get("severity", "info"),
                expected=check.get("expected"),
                actual=check.get("actual"),
                failure_count=check.get("failure_count", 0),
                message=check.get("message"),
            ))

    return RunResponse(
        id=run.id,
        dataset_id=run.dataset_id,
        run_type=run.run_type,
        status=run.status,
        started_at=run.started_at,
        completed_at=run.completed_at,
        duration_seconds=run.duration_seconds,
        total_checks=run.total_checks,
        passed_checks=run.passed_checks,
        failed_checks=run.failed_checks,
        error_checks=run.error_checks,
        results=check_results,
    )


@router.get("/{run_id}/results", response_model=List[CheckResult])
async def get_run_results(
    run_id: UUID,
    severity: str = Query(None, pattern="^(critical|warning|info)$"),
    status: str = Query(None, pattern="^(passed|failed|error)$"),
    db: AsyncSession = Depends(get_db_session),
) -> List[CheckResult]:
    """
    Get detailed results for a specific run.
    """
    # TODO: Implement database query
    return []


@router.post("/{run_id}/retry", response_model=RunResponse)
async def retry_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_db_session),
) -> RunResponse:
    """
    Retry a failed run.
    """
    # TODO: Implement retry logic
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Run not found",
    )

