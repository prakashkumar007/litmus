"""
Chalk and Duster - Report Generators

Generates HTML reports for drift detection and quality checks.
"""

from datetime import datetime
from typing import Any, Dict, List

import structlog

logger = structlog.get_logger()


def get_base_styles() -> str:
    """Get base CSS styles for reports."""
    return """
    <style>
        :root {
            --primary: #4f46e5;
            --success: #22c55e;
            --warning: #f59e0b;
            --danger: #ef4444;
            --gray-50: #f9fafb;
            --gray-100: #f3f4f6;
            --gray-200: #e5e7eb;
            --gray-600: #4b5563;
            --gray-800: #1f2937;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               background: var(--gray-50); color: var(--gray-800); line-height: 1.5; }
        .container { max-width: 1200px; margin: 0 auto; padding: 24px; }
        .header { text-align: center; margin-bottom: 32px; padding: 24px;
                  background: linear-gradient(135deg, var(--primary), #7c3aed);
                  color: white; border-radius: 12px; }
        .header h1 { font-size: 28px; margin-bottom: 8px; }
        .header p { opacity: 0.9; font-size: 14px; }
        .card { background: white; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                margin-bottom: 24px; overflow: hidden; }
        .card-header { padding: 16px 20px; border-bottom: 1px solid var(--gray-200);
                       font-weight: 600; font-size: 16px; }
        .card-body { padding: 20px; }
        .badge { display: inline-block; padding: 4px 12px; border-radius: 20px;
                 font-size: 12px; font-weight: 600; }
        .badge-success { background: #dcfce7; color: #166534; }
        .badge-warning { background: #fef3c7; color: #92400e; }
        .badge-danger { background: #fee2e2; color: #991b1b; }
        .badge-info { background: #dbeafe; color: #1e40af; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid var(--gray-200); }
        th { background: var(--gray-50); font-weight: 600; font-size: 13px;
             text-transform: uppercase; letter-spacing: 0.5px; }
        tr:hover { background: var(--gray-50); }
        .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; }
        .metric { padding: 20px; background: var(--gray-50); border-radius: 8px; text-align: center; }
        .metric-value { font-size: 32px; font-weight: 700; color: var(--primary); }
        .metric-label { font-size: 13px; color: var(--gray-600); margin-top: 4px; }
        .footer { text-align: center; color: var(--gray-600); font-size: 12px; margin-top: 40px; }
    </style>
    """


class DriftReportGenerator:
    """Generates HTML reports for drift detection results."""

    def generate(
        self,
        run_id: str,
        dataset_name: str,
        table_name: str,
        results: List[Dict[str, Any]],
        started_at: str,
        completed_at: str,
    ) -> str:
        """Generate an HTML drift detection report."""
        
        # Calculate summary metrics
        total_monitors = len(results)
        drifts_detected = sum(1 for r in results if r.get("detected", False))
        no_drift = total_monitors - drifts_detected
        
        # Build results rows
        rows_html = ""
        for r in results:
            severity = r.get("severity", "info")
            badge_class = {
                "info": "badge-info",
                "warning": "badge-warning", 
                "critical": "badge-danger",
            }.get(severity, "badge-info")
            
            status_badge = '<span class="badge badge-danger">Drift Detected</span>' if r.get("detected") else '<span class="badge badge-success">No Drift</span>'
            
            metric_val = r.get("metric_value")
            metric_str = f"{metric_val:.4f}" if metric_val is not None else "N/A"
            threshold = r.get("threshold")
            threshold_str = f"{threshold}" if threshold is not None else "N/A"
            
            rows_html += f"""
            <tr>
                <td>{r.get('monitor_name', 'Unknown')}</td>
                <td>{r.get('drift_type', 'Unknown')}</td>
                <td>{status_badge}</td>
                <td><span class="badge {badge_class}">{severity}</span></td>
                <td>{metric_str}</td>
                <td>{threshold_str}</td>
                <td>{r.get('message', '')}</td>
            </tr>
            """

        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Drift Report - {dataset_name}</title>
            {get_base_styles()}
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📈 Drift Detection Report</h1>
                    <p>Dataset: {dataset_name} | Table: {table_name}</p>
                    <p>Run ID: {run_id}</p>
                </div>

                <div class="card">
                    <div class="card-header">Summary</div>
                    <div class="card-body">
                        <div class="metric-grid">
                            <div class="metric">
                                <div class="metric-value">{total_monitors}</div>
                                <div class="metric-label">Total Monitors</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value" style="color: var(--danger);">{drifts_detected}</div>
                                <div class="metric-label">Drifts Detected</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value" style="color: var(--success);">{no_drift}</div>
                                <div class="metric-label">No Drift</div>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="card">
                    <div class="card-header">Monitor Results</div>
                    <div class="card-body" style="padding: 0;">
                        <table>
                            <thead>
                                <tr>
                                    <th>Monitor</th>
                                    <th>Type</th>
                                    <th>Status</th>
                                    <th>Severity</th>
                                    <th>Metric Value</th>
                                    <th>Threshold</th>
                                    <th>Message</th>
                                </tr>
                            </thead>
                            <tbody>
                                {rows_html}
                            </tbody>
                        </table>
                    </div>
                </div>

                <div class="footer">
                    <p>Generated by Chalk and Duster | Started: {started_at} | Completed: {completed_at}</p>
                </div>
            </div>
        </body>
        </html>
        """
        return html


class QualityReportGenerator:
    """Generates HTML reports for quality check results."""

    def generate(
        self,
        run_id: str,
        dataset_name: str,
        table_name: str,
        results: List[Dict[str, Any]],
        started_at: str,
        completed_at: str,
    ) -> str:
        """Generate an HTML quality check report."""

        # Calculate summary metrics
        total_checks = len(results)
        passed = sum(1 for r in results if r.get("status") == "passed")
        failed = sum(1 for r in results if r.get("status") == "failed")
        errors = sum(1 for r in results if r.get("status") == "error")
        pass_rate = (passed / total_checks * 100) if total_checks > 0 else 0

        # Build results rows
        rows_html = ""
        for r in results:
            status = r.get("status", "unknown")
            status_badge_class = {
                "passed": "badge-success",
                "failed": "badge-danger",
                "warning": "badge-warning",
                "error": "badge-danger",
            }.get(status, "badge-info")

            severity = r.get("severity", "info")
            severity_badge_class = {
                "info": "badge-info",
                "warning": "badge-warning",
                "critical": "badge-danger",
            }.get(severity, "badge-info")

            rows_html += f"""
            <tr>
                <td>{r.get('check_name', 'Unknown')}</td>
                <td>{r.get('check_type', 'Unknown')}</td>
                <td><span class="badge {status_badge_class}">{status}</span></td>
                <td><span class="badge {severity_badge_class}">{severity}</span></td>
                <td>{r.get('expected', 'N/A')}</td>
                <td>{r.get('actual', 'N/A')}</td>
                <td>{r.get('message', '')}</td>
            </tr>
            """

        # Overall status
        overall_status = "Passed" if failed == 0 and errors == 0 else "Failed"
        overall_badge = "badge-success" if overall_status == "Passed" else "badge-danger"

        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Quality Report - {dataset_name}</title>
            {get_base_styles()}
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>✅ Quality Check Report</h1>
                    <p>Dataset: {dataset_name} | Table: {table_name}</p>
                    <p>Run ID: {run_id}</p>
                </div>

                <div class="card">
                    <div class="card-header">Summary <span class="badge {overall_badge}" style="margin-left: 12px;">{overall_status}</span></div>
                    <div class="card-body">
                        <div class="metric-grid">
                            <div class="metric">
                                <div class="metric-value">{total_checks}</div>
                                <div class="metric-label">Total Checks</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value" style="color: var(--success);">{passed}</div>
                                <div class="metric-label">Passed</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value" style="color: var(--danger);">{failed}</div>
                                <div class="metric-label">Failed</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value" style="color: var(--primary);">{pass_rate:.1f}%</div>
                                <div class="metric-label">Pass Rate</div>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="card">
                    <div class="card-header">Check Results</div>
                    <div class="card-body" style="padding: 0;">
                        <table>
                            <thead>
                                <tr>
                                    <th>Check Name</th>
                                    <th>Type</th>
                                    <th>Status</th>
                                    <th>Severity</th>
                                    <th>Expected</th>
                                    <th>Actual</th>
                                    <th>Message</th>
                                </tr>
                            </thead>
                            <tbody>
                                {rows_html}
                            </tbody>
                        </table>
                    </div>
                </div>

                <div class="footer">
                    <p>Generated by Chalk and Duster | Started: {started_at} | Completed: {completed_at}</p>
                </div>
            </div>
        </body>
        </html>
        """
        return html

