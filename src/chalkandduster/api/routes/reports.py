"""
Chalk and Duster - Executive Reports API

Provides aggregated quality and drift reports with filtering by tenant, dataset, and check type.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, case, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from chalkandduster.api.deps import get_db_session
from chalkandduster.db.postgres.models import Run, Dataset, Tenant

logger = structlog.get_logger()
router = APIRouter()


# =============================================================================
# Response Schemas
# =============================================================================

class CheckSummary(BaseModel):
    """Summary of a specific check across runs."""
    check_name: str
    check_type: str
    total_runs: int
    passed: int
    failed: int
    error: int
    pass_rate: float
    last_status: str
    last_run_at: Optional[datetime] = None


class DatasetSummary(BaseModel):
    """Summary of a dataset's quality and drift status."""
    dataset_id: UUID
    dataset_name: str
    table_name: str
    total_quality_runs: int = 0
    total_drift_runs: int = 0
    quality_pass_rate: float = 0.0
    drift_detection_rate: float = 0.0
    last_quality_run: Optional[datetime] = None
    last_drift_run: Optional[datetime] = None
    health_score: float = 100.0


class TenantSummary(BaseModel):
    """Summary of a tenant's overall health."""
    tenant_id: UUID
    tenant_name: str
    total_datasets: int = 0
    active_datasets: int = 0
    total_runs: int = 0
    quality_runs: int = 0
    drift_runs: int = 0
    avg_pass_rate: float = 0.0
    avg_health_score: float = 100.0


class QualityReportItem(BaseModel):
    """Individual quality report item."""
    run_id: UUID
    dataset_id: UUID
    dataset_name: str
    tenant_id: UUID
    tenant_name: str
    run_type: str
    trigger_type: str = "on_demand"  # on_demand, scheduled
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    error_checks: int = 0
    pass_rate: float = 0.0
    checks: List[CheckSummary] = Field(default_factory=list)


class DriftReportItem(BaseModel):
    """Individual drift report item."""
    run_id: UUID
    dataset_id: UUID
    dataset_name: str
    tenant_id: UUID
    tenant_name: str
    trigger_type: str = "on_demand"  # on_demand, scheduled
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    schema_drift: bool = False
    volume_drift: bool = False
    distribution_drift: bool = False
    drift_detected: bool = False
    drift_types: List[str] = Field(default_factory=list)


class ExecutiveSummary(BaseModel):
    """High-level executive summary."""
    period_start: datetime
    period_end: datetime

    # Overall metrics
    total_tenants: int = 0
    total_datasets: int = 0
    total_runs: int = 0

    # Quality metrics
    quality_runs: int = 0
    quality_passed: int = 0
    quality_failed: int = 0
    quality_pass_rate: float = 0.0

    # Drift metrics
    drift_runs: int = 0
    drifts_detected: int = 0
    drift_detection_rate: float = 0.0

    # Trends
    avg_run_duration: float = 0.0
    health_score: float = 100.0


class QualityReportResponse(BaseModel):
    """Quality report response."""
    summary: ExecutiveSummary
    items: List[QualityReportItem]
    total: int
    page: int
    page_size: int


class DriftReportResponse(BaseModel):
    """Drift report response."""
    summary: ExecutiveSummary
    items: List[DriftReportItem]
    total: int
    page: int
    page_size: int


class TenantReportResponse(BaseModel):
    """Tenant-level report response."""
    tenants: List[TenantSummary]
    datasets: List[DatasetSummary]
    total_tenants: int
    total_datasets: int


# =============================================================================
# Helper Functions
# =============================================================================

def calculate_pass_rate(passed: int, total: int) -> float:
    """Calculate pass rate as percentage."""
    if total == 0:
        return 100.0
    return round((passed / total) * 100, 2)


def calculate_health_score(pass_rate: float, drift_rate: float) -> float:
    """Calculate overall health score (0-100)."""
    # Weight: 70% quality, 30% drift-free
    quality_score = pass_rate * 0.7
    drift_score = (100 - drift_rate) * 0.3
    return round(quality_score + drift_score, 2)


# =============================================================================
# API Endpoints
# =============================================================================

@router.get("/executive-summary", response_model=ExecutiveSummary)
async def get_executive_summary(
    tenant_id: Optional[UUID] = Query(None, description="Filter by tenant ID"),
    period_days: int = Query(7, ge=1, le=90, description="Report period in days"),
    db: AsyncSession = Depends(get_db_session),
) -> ExecutiveSummary:
    """
    Get high-level executive summary of quality and drift metrics.

    Returns aggregated metrics for the specified period.
    """
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=period_days)

    # Base query for runs
    base_filter = and_(
        Run.created_at >= period_start,
        Run.created_at <= period_end,
    )
    if tenant_id:
        base_filter = and_(base_filter, Run.tenant_id == tenant_id)

    # Get run counts and metrics
    query = select(
        func.count(Run.id).label("total_runs"),
        func.sum(case((Run.run_type == "quality", 1), else_=0)).label("quality_runs"),
        func.sum(case((Run.run_type == "drift", 1), else_=0)).label("drift_runs"),
        func.sum(case((and_(Run.run_type == "quality", Run.status == "completed"), 1), else_=0)).label("quality_completed"),
        func.sum(Run.passed_checks).label("total_passed"),
        func.sum(Run.total_checks).label("total_checks"),
        func.sum(Run.failed_checks).label("total_failed"),
        func.avg(Run.duration_seconds).label("avg_duration"),
    ).where(base_filter)

    result = await db.execute(query)
    row = result.one()

    # Count drifts detected from results
    drift_query = select(Run).where(
        and_(base_filter, Run.run_type == "drift", Run.failed_checks > 0)
    )
    drift_result = await db.execute(drift_query)
    drifts_detected = len(list(drift_result.scalars().all()))

    # Get tenant and dataset counts
    tenant_query = select(func.count(func.distinct(Run.tenant_id))).where(base_filter)
    dataset_query = select(func.count(func.distinct(Run.dataset_id))).where(base_filter)

    tenant_count = (await db.execute(tenant_query)).scalar() or 0
    dataset_count = (await db.execute(dataset_query)).scalar() or 0

    total_runs = row.total_runs or 0
    quality_runs = row.quality_runs or 0
    drift_runs = row.drift_runs or 0
    total_passed = row.total_passed or 0
    total_checks = row.total_checks or 0
    total_failed = row.total_failed or 0

    quality_pass_rate = calculate_pass_rate(total_passed, total_checks)
    drift_detection_rate = calculate_pass_rate(drifts_detected, drift_runs) if drift_runs > 0 else 0.0

    return ExecutiveSummary(
        period_start=period_start,
        period_end=period_end,
        total_tenants=tenant_count,
        total_datasets=dataset_count,
        total_runs=total_runs,
        quality_runs=quality_runs,
        quality_passed=quality_runs - total_failed,
        quality_failed=total_failed,
        quality_pass_rate=quality_pass_rate,
        drift_runs=drift_runs,
        drifts_detected=drifts_detected,
        drift_detection_rate=drift_detection_rate,
        avg_run_duration=round(row.avg_duration or 0, 2),
        health_score=calculate_health_score(quality_pass_rate, drift_detection_rate),
    )



@router.get("/quality", response_model=QualityReportResponse)
async def get_quality_report(
    tenant_id: Optional[UUID] = Query(None, description="Filter by tenant ID"),
    dataset_id: Optional[UUID] = Query(None, description="Filter by dataset ID"),
    check_type: Optional[str] = Query(None, description="Filter by check type"),
    status: Optional[str] = Query(None, pattern="^(passed|failed|completed|error)$"),
    period_days: int = Query(7, ge=1, le=90, description="Report period in days"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db_session),
) -> QualityReportResponse:
    """
    Get quality report with filtering.

    Filter by tenant, dataset, check type, and status.
    """
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=period_days)

    # Build filter conditions
    conditions = [
        Run.run_type == "quality",
        Run.created_at >= period_start,
        Run.created_at <= period_end,
    ]
    if tenant_id:
        conditions.append(Run.tenant_id == tenant_id)
    if dataset_id:
        conditions.append(Run.dataset_id == dataset_id)
    if status:
        conditions.append(Run.status == status)

    # Get total count
    count_query = select(func.count(Run.id)).where(and_(*conditions))
    total = (await db.execute(count_query)).scalar() or 0

    # Get runs with dataset and tenant info
    query = (
        select(Run, Dataset, Tenant)
        .join(Dataset, Run.dataset_id == Dataset.id)
        .join(Tenant, Run.tenant_id == Tenant.id)
        .where(and_(*conditions))
        .order_by(Run.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    result = await db.execute(query)
    rows = result.all()

    items = []
    for run, dataset, tenant in rows:
        # Parse check results
        checks = []
        results_data = run.results if isinstance(run.results, list) else []
        for check in results_data:
            # Skip non-dict entries
            if not isinstance(check, dict):
                continue
            if check_type and check.get("check_type") != check_type:
                continue
            checks.append(CheckSummary(
                check_name=check.get("check_name", "unknown"),
                check_type=check.get("check_type", "unknown"),
                total_runs=1,
                passed=1 if check.get("status") == "passed" else 0,
                failed=1 if check.get("status") == "failed" else 0,
                error=1 if check.get("status") == "error" else 0,
                pass_rate=100.0 if check.get("status") == "passed" else 0.0,
                last_status=check.get("status", "unknown"),
                last_run_at=run.completed_at,
            ))

        # Skip if filtering by check_type and no matching checks
        if check_type and not checks:
            continue

        pass_rate = calculate_pass_rate(run.passed_checks, run.total_checks)
        items.append(QualityReportItem(
            run_id=run.id,
            dataset_id=dataset.id,
            dataset_name=dataset.name,
            tenant_id=tenant.id,
            tenant_name=tenant.name,
            run_type=run.run_type,
            trigger_type=run.trigger_type,
            status=run.status,
            started_at=run.started_at,
            completed_at=run.completed_at,
            duration_seconds=run.duration_seconds,
            total_checks=run.total_checks,
            passed_checks=run.passed_checks,
            failed_checks=run.failed_checks,
            error_checks=run.error_checks,
            pass_rate=pass_rate,
            checks=checks,
        ))

    # Build summary
    summary_query = select(
        func.count(Run.id).label("total"),
        func.sum(Run.passed_checks).label("passed"),
        func.sum(Run.total_checks).label("checks"),
        func.sum(Run.failed_checks).label("failed"),
        func.avg(Run.duration_seconds).label("avg_duration"),
    ).where(and_(*conditions))

    summary_result = await db.execute(summary_query)
    summary_row = summary_result.one()

    summary = ExecutiveSummary(
        period_start=period_start,
        period_end=period_end,
        total_runs=summary_row.total or 0,
        quality_runs=summary_row.total or 0,
        quality_passed=(summary_row.passed or 0),
        quality_failed=(summary_row.failed or 0),
        quality_pass_rate=calculate_pass_rate(summary_row.passed or 0, summary_row.checks or 0),
        avg_run_duration=round(summary_row.avg_duration or 0, 2),
        health_score=calculate_health_score(
            calculate_pass_rate(summary_row.passed or 0, summary_row.checks or 0), 0
        ),
    )

    return QualityReportResponse(
        summary=summary,
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )




@router.get("/drift", response_model=DriftReportResponse)
async def get_drift_report(
    tenant_id: Optional[UUID] = Query(None, description="Filter by tenant ID"),
    dataset_id: Optional[UUID] = Query(None, description="Filter by dataset ID"),
    drift_type: Optional[str] = Query(None, pattern="^(schema|volume|distribution)$"),
    only_drifts: bool = Query(False, description="Show only runs with detected drifts"),
    period_days: int = Query(7, ge=1, le=90, description="Report period in days"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db_session),
) -> DriftReportResponse:
    """
    Get drift detection report with filtering.

    Filter by tenant, dataset, drift type (schema/volume/distribution).
    """
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=period_days)

    # Build filter conditions
    conditions = [
        Run.run_type == "drift",
        Run.created_at >= period_start,
        Run.created_at <= period_end,
    ]
    if tenant_id:
        conditions.append(Run.tenant_id == tenant_id)
    if dataset_id:
        conditions.append(Run.dataset_id == dataset_id)
    if only_drifts:
        conditions.append(Run.failed_checks > 0)

    # Get total count
    count_query = select(func.count(Run.id)).where(and_(*conditions))
    total = (await db.execute(count_query)).scalar() or 0

    # Get runs with dataset and tenant info
    query = (
        select(Run, Dataset, Tenant)
        .join(Dataset, Run.dataset_id == Dataset.id)
        .join(Tenant, Run.tenant_id == Tenant.id)
        .where(and_(*conditions))
        .order_by(Run.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    result = await db.execute(query)
    rows = result.all()

    items = []
    for run, dataset, tenant in rows:
        # Parse drift results
        schema_drift = False
        volume_drift = False
        distribution_drift = False
        drift_types_found = []

        results_data = run.results if isinstance(run.results, list) else []
        for check in results_data:
            # Skip non-dict entries
            if not isinstance(check, dict):
                continue
            check_type_val = check.get("check_type", "")
            is_failed = check.get("status") == "failed"

            if drift_type and check_type_val != drift_type:
                continue

            if check_type_val == "schema" and is_failed:
                schema_drift = True
                drift_types_found.append("schema")
            elif check_type_val == "volume" and is_failed:
                volume_drift = True
                drift_types_found.append("volume")
            elif check_type_val == "distribution" and is_failed:
                distribution_drift = True
                drift_types_found.append("distribution")

        # Skip if filtering by drift_type and no matching drifts
        if drift_type and drift_type not in drift_types_found and only_drifts:
            continue

        drift_detected = schema_drift or volume_drift or distribution_drift

        items.append(DriftReportItem(
            run_id=run.id,
            dataset_id=dataset.id,
            dataset_name=dataset.name,
            tenant_id=tenant.id,
            tenant_name=tenant.name,
            trigger_type=run.trigger_type,
            status=run.status,
            started_at=run.started_at,
            completed_at=run.completed_at,
            duration_seconds=run.duration_seconds,
            schema_drift=schema_drift,
            volume_drift=volume_drift,
            distribution_drift=distribution_drift,
            drift_detected=drift_detected,
            drift_types=drift_types_found,
        ))

    # Build summary
    summary_query = select(
        func.count(Run.id).label("total"),
        func.sum(case((Run.failed_checks > 0, 1), else_=0)).label("drifts"),
        func.avg(Run.duration_seconds).label("avg_duration"),
    ).where(and_(*conditions))

    summary_result = await db.execute(summary_query)
    summary_row = summary_result.one()

    drifts_detected = summary_row.drifts or 0
    total_drift_runs = summary_row.total or 0

    summary = ExecutiveSummary(
        period_start=period_start,
        period_end=period_end,
        total_runs=total_drift_runs,
        drift_runs=total_drift_runs,
        drifts_detected=drifts_detected,
        drift_detection_rate=calculate_pass_rate(drifts_detected, total_drift_runs),
        avg_run_duration=round(summary_row.avg_duration or 0, 2),
        health_score=calculate_health_score(
            100, calculate_pass_rate(drifts_detected, total_drift_runs)
        ),
    )

    return DriftReportResponse(
        summary=summary,
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )



@router.get("/by-tenant", response_model=TenantReportResponse)
async def get_tenant_report(
    tenant_id: Optional[UUID] = Query(None, description="Filter by specific tenant"),
    period_days: int = Query(7, ge=1, le=90, description="Report period in days"),
    db: AsyncSession = Depends(get_db_session),
) -> TenantReportResponse:
    """
    Get report aggregated by tenant.

    Shows health metrics for each tenant and their datasets.
    """
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=period_days)

    # Base tenant query
    tenant_query = select(Tenant).where(Tenant.is_active == True)
    if tenant_id:
        tenant_query = tenant_query.where(Tenant.id == tenant_id)

    tenant_result = await db.execute(tenant_query)
    tenants = list(tenant_result.scalars().all())

    tenant_summaries = []
    all_dataset_summaries = []

    for tenant in tenants:
        # Get datasets for tenant
        dataset_query = select(Dataset).where(
            and_(Dataset.tenant_id == tenant.id, Dataset.is_active == True)
        )
        dataset_result = await db.execute(dataset_query)
        datasets = list(dataset_result.scalars().all())

        # Get run stats for tenant
        run_query = select(
            func.count(Run.id).label("total_runs"),
            func.sum(case((Run.run_type == "quality", 1), else_=0)).label("quality_runs"),
            func.sum(case((Run.run_type == "drift", 1), else_=0)).label("drift_runs"),
            func.sum(Run.passed_checks).label("passed"),
            func.sum(Run.total_checks).label("total_checks"),
        ).where(
            and_(
                Run.tenant_id == tenant.id,
                Run.created_at >= period_start,
                Run.created_at <= period_end,
            )
        )
        run_result = await db.execute(run_query)
        run_row = run_result.one()

        pass_rate = calculate_pass_rate(run_row.passed or 0, run_row.total_checks or 0)

        tenant_summaries.append(TenantSummary(
            tenant_id=tenant.id,
            tenant_name=tenant.name,
            total_datasets=len(datasets),
            active_datasets=len([d for d in datasets if d.is_active]),
            total_runs=run_row.total_runs or 0,
            quality_runs=run_row.quality_runs or 0,
            drift_runs=run_row.drift_runs or 0,
            avg_pass_rate=pass_rate,
            avg_health_score=calculate_health_score(pass_rate, 0),
        ))

        # Build dataset summaries
        for dataset in datasets:
            ds_run_query = select(
                func.count(Run.id).label("total"),
                func.sum(case((Run.run_type == "quality", 1), else_=0)).label("quality"),
                func.sum(case((Run.run_type == "drift", 1), else_=0)).label("drift"),
                func.sum(case((Run.failed_checks > 0, 1), else_=0)).label("drifts_found"),
                func.sum(Run.passed_checks).label("passed"),
                func.sum(Run.total_checks).label("checks"),
                func.max(case((Run.run_type == "quality", Run.completed_at))).label("last_quality"),
                func.max(case((Run.run_type == "drift", Run.completed_at))).label("last_drift"),
            ).where(
                and_(
                    Run.dataset_id == dataset.id,
                    Run.created_at >= period_start,
                    Run.created_at <= period_end,
                )
            )
            ds_run_result = await db.execute(ds_run_query)
            ds_row = ds_run_result.one()

            q_pass_rate = calculate_pass_rate(ds_row.passed or 0, ds_row.checks or 0)
            d_rate = calculate_pass_rate(ds_row.drifts_found or 0, ds_row.drift or 0) if ds_row.drift else 0

            all_dataset_summaries.append(DatasetSummary(
                dataset_id=dataset.id,
                dataset_name=dataset.name,
                table_name=dataset.table_name,
                total_quality_runs=ds_row.quality or 0,
                total_drift_runs=ds_row.drift or 0,
                quality_pass_rate=q_pass_rate,
                drift_detection_rate=d_rate,
                last_quality_run=ds_row.last_quality,
                last_drift_run=ds_row.last_drift,
                health_score=calculate_health_score(q_pass_rate, d_rate),
            ))

    return TenantReportResponse(
        tenants=tenant_summaries,
        datasets=all_dataset_summaries,
        total_tenants=len(tenant_summaries),
        total_datasets=len(all_dataset_summaries),
    )



@router.get("/export/quality")
async def export_quality_report_csv(
    tenant_id: Optional[UUID] = Query(None),
    dataset_id: Optional[UUID] = Query(None),
    period_days: int = Query(7, ge=1, le=90),
    db: AsyncSession = Depends(get_db_session),
) -> StreamingResponse:
    """
    Export quality report as CSV.
    """
    import csv
    import io

    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=period_days)

    conditions = [
        Run.run_type == "quality",
        Run.created_at >= period_start,
        Run.created_at <= period_end,
    ]
    if tenant_id:
        conditions.append(Run.tenant_id == tenant_id)
    if dataset_id:
        conditions.append(Run.dataset_id == dataset_id)

    query = (
        select(Run, Dataset, Tenant)
        .join(Dataset, Run.dataset_id == Dataset.id)
        .join(Tenant, Run.tenant_id == Tenant.id)
        .where(and_(*conditions))
        .order_by(Run.created_at.desc())
    )

    result = await db.execute(query)
    rows = result.all()

    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Run ID", "Tenant", "Dataset", "Table", "Status",
        "Started At", "Completed At", "Duration (s)",
        "Total Checks", "Passed", "Failed", "Error", "Pass Rate %"
    ])

    for run, dataset, tenant in rows:
        pass_rate = calculate_pass_rate(run.passed_checks, run.total_checks)
        writer.writerow([
            str(run.id),
            tenant.name,
            dataset.name,
            dataset.table_name,
            run.status,
            run.started_at.isoformat() if run.started_at else "",
            run.completed_at.isoformat() if run.completed_at else "",
            round(run.duration_seconds, 2) if run.duration_seconds else "",
            run.total_checks,
            run.passed_checks,
            run.failed_checks,
            run.error_checks,
            pass_rate,
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=quality_report_{datetime.now().strftime('%Y%m%d')}.csv"}
    )


@router.get("/export/drift")
async def export_drift_report_csv(
    tenant_id: Optional[UUID] = Query(None),
    dataset_id: Optional[UUID] = Query(None),
    period_days: int = Query(7, ge=1, le=90),
    db: AsyncSession = Depends(get_db_session),
) -> StreamingResponse:
    """
    Export drift report as CSV.
    """
    import csv
    import io

    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=period_days)

    conditions = [
        Run.run_type == "drift",
        Run.created_at >= period_start,
        Run.created_at <= period_end,
    ]
    if tenant_id:
        conditions.append(Run.tenant_id == tenant_id)
    if dataset_id:
        conditions.append(Run.dataset_id == dataset_id)

    query = (
        select(Run, Dataset, Tenant)
        .join(Dataset, Run.dataset_id == Dataset.id)
        .join(Tenant, Run.tenant_id == Tenant.id)
        .where(and_(*conditions))
        .order_by(Run.created_at.desc())
    )

    result = await db.execute(query)
    rows = result.all()

    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Run ID", "Tenant", "Dataset", "Table", "Status",
        "Started At", "Completed At", "Duration (s)",
        "Schema Drift", "Volume Drift", "Distribution Drift", "Any Drift"
    ])

    for run, dataset, tenant in rows:
        schema_drift = volume_drift = distribution_drift = False
        results_data = run.results if isinstance(run.results, list) else []
        for check in results_data:
            if not isinstance(check, dict):
                continue
            if check.get("check_type") == "schema" and check.get("status") == "failed":
                schema_drift = True
            elif check.get("check_type") == "volume" and check.get("status") == "failed":
                volume_drift = True
            elif check.get("check_type") == "distribution" and check.get("status") == "failed":
                distribution_drift = True

        writer.writerow([
            str(run.id),
            tenant.name,
            dataset.name,
            dataset.table_name,
            run.status,
            run.started_at.isoformat() if run.started_at else "",
            run.completed_at.isoformat() if run.completed_at else "",
            round(run.duration_seconds, 2) if run.duration_seconds else "",
            "Yes" if schema_drift else "No",
            "Yes" if volume_drift else "No",
            "Yes" if distribution_drift else "No",
            "Yes" if (schema_drift or volume_drift or distribution_drift) else "No",
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=drift_report_{datetime.now().strftime('%Y%m%d')}.csv"}
    )