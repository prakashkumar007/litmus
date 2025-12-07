"""
Chalk and Duster - HTML Executive Reports

Generates beautiful HTML reports for quality and drift monitoring.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, case, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from chalkandduster.api.deps import get_db_session
from chalkandduster.db.postgres.models import Run, Dataset, Tenant

logger = structlog.get_logger()
router = APIRouter()


# =============================================================================
# HTML Templates
# =============================================================================

def get_base_styles() -> str:
    """Return CSS styles for reports."""
    return """
    <style>
        :root {
            --primary: #2563eb;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --gray-50: #f9fafb;
            --gray-100: #f3f4f6;
            --gray-200: #e5e7eb;
            --gray-600: #4b5563;
            --gray-800: #1f2937;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--gray-50);
            color: var(--gray-800);
            line-height: 1.6;
        }
        .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
        .header {
            background: linear-gradient(135deg, var(--primary) 0%, #1d4ed8 100%);
            color: white;
            padding: 32px;
            border-radius: 12px;
            margin-bottom: 24px;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);
        }
        .header h1 { font-size: 28px; font-weight: 700; margin-bottom: 8px; }
        .header p { opacity: 0.9; font-size: 14px; }
        .filters {
            background: white;
            padding: 20px;
            border-radius: 12px;
            margin-bottom: 24px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            display: flex;
            gap: 16px;
            flex-wrap: wrap;
            align-items: end;
        }
        .filter-group { display: flex; flex-direction: column; gap: 4px; }
        .filter-group label { font-size: 12px; font-weight: 600; color: var(--gray-600); }
        .filter-group select, .filter-group input {
            padding: 8px 12px;
            border: 1px solid var(--gray-200);
            border-radius: 6px;
            font-size: 14px;
            min-width: 180px;
        }
        .btn {
            padding: 10px 20px;
            background: var(--primary);
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 500;
            transition: background 0.2s;
        }
        .btn:hover { background: #1d4ed8; }
        .btn-secondary { background: var(--gray-600); }
        .metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }
        .metric-card {
            background: white;
            padding: 24px;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        .metric-value {
            font-size: 36px;
            font-weight: 700;
            color: var(--gray-800);
        }
        .metric-label {
            font-size: 14px;
            color: var(--gray-600);
            margin-top: 4px;
        }
        .metric-card.success .metric-value { color: var(--success); }
        .metric-card.warning .metric-value { color: var(--warning); }
        .metric-card.danger .metric-value { color: var(--danger); }
        .card {
            background: white;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            margin-bottom: 24px;
            overflow: hidden;
        }
        .card-header {
            padding: 16px 24px;
            border-bottom: 1px solid var(--gray-200);
            font-weight: 600;
            font-size: 16px;
        }
        .card-body { padding: 24px; }
        table { width: 100%; border-collapse: collapse; }
        th, td {
            padding: 12px 16px;
            text-align: left;
            border-bottom: 1px solid var(--gray-100);
        }
        th {
            background: var(--gray-50);
            font-weight: 600;
            font-size: 12px;
            text-transform: uppercase;
            color: var(--gray-600);
        }
        tr:hover { background: var(--gray-50); }
        .badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 9999px;
            font-size: 12px;
            font-weight: 500;
        }
        .badge-success { background: #d1fae5; color: #065f46; }
        .badge-warning { background: #fef3c7; color: #92400e; }
        .badge-danger { background: #fee2e2; color: #991b1b; }
        .badge-info { background: #dbeafe; color: #1e40af; }
        .progress-bar {
            height: 8px;
            background: var(--gray-200);
            border-radius: 4px;
            overflow: hidden;
        }
        .progress-fill {
            height: 100%;
            background: var(--success);
            transition: width 0.3s;
        }
        .health-score {
            width: 60px;
            height: 60px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 18px;
        }
        .health-good { background: #d1fae5; color: #065f46; }
        .health-warning { background: #fef3c7; color: #92400e; }
        .health-bad { background: #fee2e2; color: #991b1b; }
        .nav-tabs {
            display: flex;
            gap: 8px;
            margin-bottom: 24px;
        }
        .nav-tab {
            padding: 12px 24px;
            background: white;
            border: 1px solid var(--gray-200);
            border-radius: 8px;
            text-decoration: none;
            color: var(--gray-600);
            font-weight: 500;
            transition: all 0.2s;
        }
        .nav-tab:hover { border-color: var(--primary); color: var(--primary); }
        .nav-tab.active { background: var(--primary); color: white; border-color: var(--primary); }
        .timestamp { font-size: 12px; color: var(--gray-600); }
        @media print {
            .filters, .nav-tabs { display: none; }
            .container { max-width: 100%; }
        }
    </style>
    """


def get_nav_tabs(active: str) -> str:
    """Return navigation tabs."""
    tabs = [
        ("dashboard", "Dashboard", "/api/v1/html-reports/dashboard"),
        ("quality", "Quality Report", "/api/v1/html-reports/quality"),
        ("drift", "Drift Report", "/api/v1/html-reports/drift"),
    ]
    html = '<div class="nav-tabs">'
    for key, label, url in tabs:
        active_class = "active" if key == active else ""
        html += f'<a href="{url}" class="nav-tab {active_class}">{label}</a>'
    html += '</div>'
    return html


def format_datetime(dt: datetime) -> str:
    """Format datetime for display."""
    if not dt:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M")


def get_health_class(score: float) -> str:
    """Get CSS class for health score."""
    if score >= 80:
        return "health-good"
    elif score >= 60:
        return "health-warning"
    return "health-bad"


def get_status_badge(status: str) -> str:
    """Get badge HTML for status."""
    badge_map = {
        "passed": ("badge-success", "Passed"),
        "completed": ("badge-success", "Completed"),
        "failed": ("badge-danger", "Failed"),
        "error": ("badge-danger", "Error"),
        "pending": ("badge-info", "Pending"),
        "running": ("badge-warning", "Running"),
    }
    css_class, label = badge_map.get(status, ("badge-info", status.title()))
    return f'<span class="badge {css_class}">{label}</span>'


def calculate_pass_rate(passed: int, total: int) -> float:
    """Calculate pass rate."""
    if total == 0:
        return 100.0
    return round((passed / total) * 100, 1)



# =============================================================================
# Dashboard Endpoint
# =============================================================================

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_report(
    tenant_id: Optional[UUID] = Query(None),
    period_days: int = Query(7, ge=1, le=90),
    db: AsyncSession = Depends(get_db_session),
) -> HTMLResponse:
    """Dashboard - Overview of Quality and Drift metrics."""
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=period_days)

    # Build base filter
    base_filter = and_(
        Run.created_at >= period_start,
        Run.created_at <= period_end,
    )
    if tenant_id:
        base_filter = and_(base_filter, Run.tenant_id == tenant_id)

    # Get aggregate metrics
    metrics_query = select(
        func.count(Run.id).label("total_runs"),
        func.sum(case((Run.run_type == "quality", 1), else_=0)).label("quality_runs"),
        func.sum(case((Run.run_type == "drift", 1), else_=0)).label("drift_runs"),
        func.sum(Run.passed_checks).label("passed"),
        func.sum(Run.total_checks).label("total_checks"),
        func.sum(Run.failed_checks).label("failed"),
        func.sum(case((and_(Run.run_type == "drift", Run.failed_checks > 0), 1), else_=0)).label("drifts"),
        func.avg(Run.duration_seconds).label("avg_duration"),
    ).where(base_filter)

    result = await db.execute(metrics_query)
    m = result.one()

    # Get tenants for filter dropdown
    tenants_query = select(Tenant).where(Tenant.is_active == True)
    tenants_result = await db.execute(tenants_query)
    tenants = list(tenants_result.scalars().all())

    # Get recent runs
    recent_query = (
        select(Run, Dataset, Tenant)
        .join(Dataset, Run.dataset_id == Dataset.id)
        .join(Tenant, Run.tenant_id == Tenant.id)
        .where(base_filter)
        .order_by(Run.created_at.desc())
        .limit(10)
    )
    recent_result = await db.execute(recent_query)
    recent_runs = recent_result.all()

    # Calculate metrics
    total_runs = m.total_runs or 0
    quality_runs = m.quality_runs or 0
    drift_runs = m.drift_runs or 0
    passed = m.passed or 0
    total_checks = m.total_checks or 0
    drifts = m.drifts or 0

    pass_rate = calculate_pass_rate(passed, total_checks)
    drift_rate = calculate_pass_rate(drifts, drift_runs) if drift_runs > 0 else 0
    health_score = round((pass_rate * 0.7) + ((100 - drift_rate) * 0.3), 1)

    # Build tenant options
    tenant_options = '<option value="">All Tenants</option>'
    for t in tenants:
        selected = "selected" if tenant_id and t.id == tenant_id else ""
        tenant_options += f'<option value="{t.id}" {selected}>{t.name}</option>'

    # Build recent runs table
    runs_html = ""
    for run, dataset, tenant in recent_runs:
        trigger_badge = '<span class="badge badge-secondary">Scheduled</span>' if run.trigger_type == "scheduled" else '<span class="badge badge-primary">On Demand</span>'
        runs_html += f"""
        <tr>
            <td>{format_datetime(run.created_at)}</td>
            <td>{tenant.name}</td>
            <td>{dataset.name}</td>
            <td><span class="badge badge-info">{run.run_type.title()}</span></td>
            <td>{trigger_badge}</td>
            <td>{get_status_badge(run.status)}</td>
            <td>{run.passed_checks}/{run.total_checks}</td>
            <td>{round(run.duration_seconds, 2) if run.duration_seconds else '-'}s</td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dashboard - Chalk and Duster</title>
        {get_base_styles()}
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Dashboard</h1>
                <p>Data Quality & Drift Monitoring Report | {period_start.strftime('%b %d')} - {period_end.strftime('%b %d, %Y')}</p>
            </div>

            {get_nav_tabs("dashboard")}

            <form class="filters" method="get">
                <div class="filter-group">
                    <label>Tenant</label>
                    <select name="tenant_id">{tenant_options}</select>
                </div>
                <div class="filter-group">
                    <label>Period (Days)</label>
                    <input type="number" name="period_days" value="{period_days}" min="1" max="90">
                </div>
                <button type="submit" class="btn">Apply Filters</button>
                <a href="/api/v1/reports/export/quality?period_days={period_days}" class="btn btn-secondary">Export CSV</a>
            </form>

            <div class="metrics">
                <div class="metric-card">
                    <div class="metric-value">{total_runs}</div>
                    <div class="metric-label">Total Runs</div>
                </div>
                <div class="metric-card success">
                    <div class="metric-value">{quality_runs}</div>
                    <div class="metric-label">Quality Checks</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{drift_runs}</div>
                    <div class="metric-label">Drift Checks</div>
                </div>
                <div class="metric-card {'success' if pass_rate >= 80 else 'warning' if pass_rate >= 60 else 'danger'}">
                    <div class="metric-value">{pass_rate}%</div>
                    <div class="metric-label">Quality Pass Rate</div>
                </div>
                <div class="metric-card {'success' if drift_rate < 20 else 'warning' if drift_rate < 50 else 'danger'}">
                    <div class="metric-value">{drifts}</div>
                    <div class="metric-label">Drifts Detected</div>
                </div>
                <div class="metric-card">
                    <div class="health-score {get_health_class(health_score)}">{health_score}</div>
                    <div class="metric-label">Health Score</div>
                </div>
            </div>

            <div class="card">
                <div class="card-header">📋 Recent Runs</div>
                <div class="card-body" style="padding: 0;">
                    <table>
                        <thead>
                            <tr>
                                <th>Time</th>
                                <th>Tenant</th>
                                <th>Dataset</th>
                                <th>Type</th>
                                <th>Trigger</th>
                                <th>Status</th>
                                <th>Passed/Total</th>
                                <th>Duration</th>
                            </tr>
                        </thead>
                        <tbody>
                            {runs_html if runs_html else '<tr><td colspan="8" style="text-align:center;padding:40px;">No runs found</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </div>

            <div style="text-align:center; color: var(--gray-600); font-size: 12px; margin-top: 40px;">
                Generated by Chalk and Duster | {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
            </div>
        </div>
    </body>
    </html>
    """

    return HTMLResponse(content=html)



# =============================================================================
# Quality Report Endpoint
# =============================================================================

@router.get("/quality", response_class=HTMLResponse)
async def quality_report(
    tenant_id: Optional[UUID] = Query(None),
    dataset_id: Optional[UUID] = Query(None),
    status: Optional[str] = Query(None),
    period_days: int = Query(7, ge=1, le=90),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db_session),
) -> HTMLResponse:
    """Quality Report - Detailed quality check results."""
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=period_days)

    # Build filter
    base_filter = and_(
        Run.run_type == "quality",
        Run.created_at >= period_start,
        Run.created_at <= period_end,
    )
    if tenant_id:
        base_filter = and_(base_filter, Run.tenant_id == tenant_id)
    if dataset_id:
        base_filter = and_(base_filter, Run.dataset_id == dataset_id)
    if status:
        base_filter = and_(base_filter, Run.status == status)

    # Get tenants and datasets for dropdowns
    tenants_result = await db.execute(select(Tenant).where(Tenant.is_active == True))
    tenants = list(tenants_result.scalars().all())

    datasets_query = select(Dataset).where(Dataset.is_active == True)
    if tenant_id:
        datasets_query = datasets_query.where(Dataset.tenant_id == tenant_id)
    datasets_result = await db.execute(datasets_query)
    datasets = list(datasets_result.scalars().all())

    # Count total
    count_result = await db.execute(select(func.count(Run.id)).where(base_filter))
    total = count_result.scalar() or 0

    # Get runs
    offset = (page - 1) * page_size
    runs_query = (
        select(Run, Dataset, Tenant)
        .join(Dataset, Run.dataset_id == Dataset.id)
        .join(Tenant, Run.tenant_id == Tenant.id)
        .where(base_filter)
        .order_by(Run.created_at.desc())
        .offset(offset)
        .limit(page_size)
    )
    runs_result = await db.execute(runs_query)
    runs = runs_result.all()

    total_pages = (total + page_size - 1) // page_size

    # Build dropdown options
    tenant_opts = '<option value="">All Tenants</option>'
    for t in tenants:
        sel = "selected" if tenant_id and t.id == tenant_id else ""
        tenant_opts += f'<option value="{t.id}" {sel}>{t.name}</option>'

    dataset_opts = '<option value="">All Datasets</option>'
    for d in datasets:
        sel = "selected" if dataset_id and d.id == dataset_id else ""
        dataset_opts += f'<option value="{d.id}" {sel}>{d.name}</option>'

    status_opts = '<option value="">All Statuses</option>'
    for s in ["completed", "failed", "pending", "running"]:
        sel = "selected" if status == s else ""
        status_opts += f'<option value="{s}" {sel}>{s.title()}</option>'

    # Build table rows
    rows_html = ""
    for run, dataset, tenant in runs:
        pass_rate = calculate_pass_rate(run.passed_checks, run.total_checks)
        progress_color = "var(--success)" if pass_rate >= 80 else "var(--warning)" if pass_rate >= 60 else "var(--danger)"
        rows_html += f"""
        <tr>
            <td class="timestamp">{format_datetime(run.created_at)}</td>
            <td>{tenant.name}</td>
            <td>{dataset.name}</td>
            <td>{get_status_badge(run.status)}</td>
            <td>{run.passed_checks}</td>
            <td>{run.failed_checks}</td>
            <td>{run.total_checks}</td>
            <td>
                <div style="display:flex;align-items:center;gap:8px;">
                    <div class="progress-bar" style="flex:1;">
                        <div class="progress-fill" style="width:{pass_rate}%;background:{progress_color};"></div>
                    </div>
                    <span style="font-size:12px;font-weight:600;">{pass_rate}%</span>
                </div>
            </td>
            <td>{round(run.duration_seconds, 2) if run.duration_seconds else '-'}s</td>
        </tr>
        """

    # Pagination
    pagination_html = '<div style="display:flex;gap:8px;justify-content:center;margin-top:16px;">'
    base_url = f"/api/v1/html-reports/quality?period_days={period_days}&page_size={page_size}"
    if tenant_id:
        base_url += f"&tenant_id={tenant_id}"
    if dataset_id:
        base_url += f"&dataset_id={dataset_id}"
    if status:
        base_url += f"&status={status}"

    if page > 1:
        pagination_html += f'<a href="{base_url}&page={page-1}" class="btn btn-secondary">← Previous</a>'
    pagination_html += f'<span style="padding:10px;">Page {page} of {total_pages}</span>'
    if page < total_pages:
        pagination_html += f'<a href="{base_url}&page={page+1}" class="btn btn-secondary">Next →</a>'
    pagination_html += '</div>'

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Quality Report - Chalk and Duster</title>
        {get_base_styles()}
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>✅ Quality Report</h1>
                <p>Data Quality Check Results | {period_start.strftime('%b %d')} - {period_end.strftime('%b %d, %Y')} | {total} runs</p>
            </div>

            {get_nav_tabs("quality")}

            <form class="filters" method="get">
                <div class="filter-group">
                    <label>Tenant</label>
                    <select name="tenant_id">{tenant_opts}</select>
                </div>
                <div class="filter-group">
                    <label>Dataset</label>
                    <select name="dataset_id">{dataset_opts}</select>
                </div>
                <div class="filter-group">
                    <label>Status</label>
                    <select name="status">{status_opts}</select>
                </div>
                <div class="filter-group">
                    <label>Period (Days)</label>
                    <input type="number" name="period_days" value="{period_days}" min="1" max="90">
                </div>
                <button type="submit" class="btn">Apply Filters</button>
                <a href="/api/v1/reports/export/quality?period_days={period_days}" class="btn btn-secondary">Export CSV</a>
            </form>

            <div class="card">
                <div class="card-header">📋 Quality Check Runs</div>
                <div class="card-body" style="padding:0;">
                    <table>
                        <thead>
                            <tr>
                                <th>Time</th>
                                <th>Tenant</th>
                                <th>Dataset</th>
                                <th>Trigger</th>
                                <th>Status</th>
                                <th>Passed</th>
                                <th>Failed</th>
                                <th>Total</th>
                                <th>Pass Rate</th>
                                <th>Duration</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows_html if rows_html else '<tr><td colspan="10" style="text-align:center;padding:40px;">No quality runs found</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </div>

            {pagination_html}

            <div style="text-align:center; color: var(--gray-600); font-size: 12px; margin-top: 40px;">
                Generated by Chalk and Duster | {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
            </div>
        </div>
    </body>
    </html>
    """

    return HTMLResponse(content=html)


# =============================================================================
# Drift Report Endpoint
# =============================================================================

@router.get("/drift", response_class=HTMLResponse)
async def drift_report(
    tenant_id: Optional[UUID] = Query(None),
    dataset_id: Optional[UUID] = Query(None),
    only_drifts: bool = Query(False),
    period_days: int = Query(7, ge=1, le=90),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db_session),
) -> HTMLResponse:
    """Drift Report - Detailed drift detection results."""
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=period_days)

    # Build filter
    base_filter = and_(
        Run.run_type == "drift",
        Run.created_at >= period_start,
        Run.created_at <= period_end,
    )
    if tenant_id:
        base_filter = and_(base_filter, Run.tenant_id == tenant_id)
    if dataset_id:
        base_filter = and_(base_filter, Run.dataset_id == dataset_id)
    if only_drifts:
        base_filter = and_(base_filter, Run.failed_checks > 0)

    # Get tenants and datasets
    tenants_result = await db.execute(select(Tenant).where(Tenant.is_active == True))
    tenants = list(tenants_result.scalars().all())

    datasets_query = select(Dataset).where(Dataset.is_active == True)
    if tenant_id:
        datasets_query = datasets_query.where(Dataset.tenant_id == tenant_id)
    datasets_result = await db.execute(datasets_query)
    datasets = list(datasets_result.scalars().all())

    # Count total
    count_result = await db.execute(select(func.count(Run.id)).where(base_filter))
    total = count_result.scalar() or 0

    # Get runs
    offset = (page - 1) * page_size
    runs_query = (
        select(Run, Dataset, Tenant)
        .join(Dataset, Run.dataset_id == Dataset.id)
        .join(Tenant, Run.tenant_id == Tenant.id)
        .where(base_filter)
        .order_by(Run.created_at.desc())
        .offset(offset)
        .limit(page_size)
    )
    runs_result = await db.execute(runs_query)
    runs = runs_result.all()

    total_pages = max(1, (total + page_size - 1) // page_size)

    # Build dropdown options
    tenant_opts = '<option value="">All Tenants</option>'
    for t in tenants:
        sel = "selected" if tenant_id and t.id == tenant_id else ""
        tenant_opts += f'<option value="{t.id}" {sel}>{t.name}</option>'

    dataset_opts = '<option value="">All Datasets</option>'
    for d in datasets:
        sel = "selected" if dataset_id and d.id == dataset_id else ""
        dataset_opts += f'<option value="{d.id}" {sel}>{d.name}</option>'

    # Build table rows
    rows_html = ""
    for run, dataset, tenant in runs:
        drift_detected = run.failed_checks > 0
        # Parse drift types from results
        schema_drift = volume_drift = distribution_drift = False
        results_data = run.results if isinstance(run.results, list) else []
        for check in results_data:
            if not isinstance(check, dict):
                continue
            ct = check.get("check_type", "")
            if check.get("status") == "failed":
                if ct == "schema":
                    schema_drift = True
                elif ct == "volume":
                    volume_drift = True
                elif ct == "distribution":
                    distribution_drift = True

        rows_html += f"""
        <tr>
            <td class="timestamp">{format_datetime(run.created_at)}</td>
            <td>{tenant.name}</td>
            <td>{dataset.name}</td>
            <td>{get_status_badge(run.status)}</td>
            <td>{'<span class="badge badge-danger">Yes</span>' if schema_drift else '<span class="badge badge-success">No</span>'}</td>
            <td>{'<span class="badge badge-danger">Yes</span>' if volume_drift else '<span class="badge badge-success">No</span>'}</td>
            <td>{'<span class="badge badge-danger">Yes</span>' if distribution_drift else '<span class="badge badge-success">No</span>'}</td>
            <td>{'<span class="badge badge-danger">⚠ Drift Detected</span>' if drift_detected else '<span class="badge badge-success">✓ No Drift</span>'}</td>
            <td>{round(run.duration_seconds, 2) if run.duration_seconds else '-'}s</td>
        </tr>
        """

    # Pagination
    pagination_html = '<div style="display:flex;gap:8px;justify-content:center;margin-top:16px;">'
    base_url = f"/api/v1/html-reports/drift?period_days={period_days}&page_size={page_size}"
    if tenant_id:
        base_url += f"&tenant_id={tenant_id}"
    if dataset_id:
        base_url += f"&dataset_id={dataset_id}"
    if only_drifts:
        base_url += "&only_drifts=true"

    if page > 1:
        pagination_html += f'<a href="{base_url}&page={page-1}" class="btn btn-secondary">← Previous</a>'
    pagination_html += f'<span style="padding:10px;">Page {page} of {total_pages}</span>'
    if page < total_pages:
        pagination_html += f'<a href="{base_url}&page={page+1}" class="btn btn-secondary">Next →</a>'
    pagination_html += '</div>'

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Drift Report - Chalk and Duster</title>
        {get_base_styles()}
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔄 Drift Report</h1>
                <p>Data Drift Detection Results | {period_start.strftime('%b %d')} - {period_end.strftime('%b %d, %Y')} | {total} runs</p>
            </div>

            {get_nav_tabs("drift")}

            <form class="filters" method="get" onsubmit="return cleanFormBeforeSubmit(this)">
                <div class="filter-group">
                    <label>Tenant</label>
                    <select name="tenant_id">{tenant_opts}</select>
                </div>
                <div class="filter-group">
                    <label>Dataset</label>
                    <select name="dataset_id">{dataset_opts}</select>
                </div>
                <div class="filter-group">
                    <label>Show Only Drifts</label>
                    <select name="only_drifts">
                        <option value="false" {'selected' if not only_drifts else ''}>All Runs</option>
                        <option value="true" {'selected' if only_drifts else ''}>Only Drifts</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label>Period (Days)</label>
                    <input type="number" name="period_days" value="{period_days}" min="1" max="90">
                </div>
                <button type="submit" class="btn">Apply Filters</button>
                <a href="/api/v1/reports/export/drift?period_days={period_days}" class="btn btn-secondary">Export CSV</a>
            </form>

            <div class="card">
                <div class="card-header">📋 Drift Detection Runs</div>
                <div class="card-body" style="padding:0;">
                    <table>
                        <thead>
                            <tr>
                                <th>Time</th>
                                <th>Tenant</th>
                                <th>Dataset</th>
                                <th>Status</th>
                                <th>Schema</th>
                                <th>Volume</th>
                                <th>Distribution</th>
                                <th>Result</th>
                                <th>Duration</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows_html if rows_html else '<tr><td colspan="9" style="text-align:center;padding:40px;">No drift runs found</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </div>

            {pagination_html}

            <div style="text-align:center; color: var(--gray-600); font-size: 12px; margin-top: 40px;">
                Generated by Chalk and Duster | {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
            </div>
        </div>
        <script>
        function cleanFormBeforeSubmit(form) {{
            // Disable select elements with empty values so they're not submitted
            var selects = form.querySelectorAll('select');
            selects.forEach(function(select) {{
                if (select.value === '') {{
                    select.disabled = true;
                }}
            }});
            return true;
        }}
        </script>
    </body>
    </html>
    """

    return HTMLResponse(content=html)
