"""
Chalk and Duster - Quality Check Handler

Handler for triggering quality checks using Great Expectations.
"""

import asyncio
import os
from datetime import datetime
from typing import Dict, Any, Optional
from uuid import UUID

import streamlit as st

from streamlit_app.utils.database import (
    get_dataset_by_id,
    get_connection_by_id,
    create_run,
    update_run,
)


def trigger_quality_check() -> str:
    """
    Trigger a quality check for the current dataset using Great Expectations.
    
    Returns:
        Response message with check results or error
    """
    if not st.session_state.dataset_id:
        return "❌ No dataset selected. Please create or select a dataset first."
    
    dataset_id = st.session_state.dataset_id
    tenant_id = st.session_state.tenant_id
    
    # Get dataset details
    dataset = get_dataset_by_id(dataset_id)
    if not dataset:
        return "❌ Dataset not found. Please select a valid dataset."
    
    quality_yaml = dataset.get("quality_yaml")
    if not quality_yaml:
        return "❌ No quality rules configured for this dataset. Please add quality rules first."
    
    # Get connection config
    connection_id = dataset.get("connection_id")
    if not connection_id:
        return "❌ No connection configured for this dataset."
    
    connection = get_connection_by_id(connection_id)
    if not connection:
        return "❌ Connection not found."
    
    # Create run record
    run = create_run(
        dataset_id=dataset_id,
        tenant_id=tenant_id,
        run_type="quality",
        trigger_type="on_demand",
        status="running",
    )
    run_id = run["id"]
    
    try:
        result = _execute_quality_check(connection, dataset, quality_yaml)
        return _format_success_response(result, dataset, run_id)
    except Exception as e:
        return _handle_quality_error(e, run_id)


def _get_credentials_from_secrets(secret_arn: str) -> Dict[str, str]:
    """Fetch Snowflake credentials from AWS Secrets Manager / LocalStack."""
    import boto3
    import json
    import os

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    client = boto3.client(
        "secretsmanager",
        endpoint_url=endpoint_url,
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )

    response = client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response["SecretString"])
    return {
        "user": secret.get("user") or secret.get("username", ""),
        "password": secret.get("password", ""),
    }


def _execute_quality_check(
    connection: Dict[str, Any],
    dataset: Dict[str, Any],
    quality_yaml: str,
) -> Any:
    """Execute the quality check using Great Expectations."""
    from chalkandduster.quality.great_expectations_executor import GreatExpectationsExecutor

    # Get credentials from secrets manager
    secret_arn = connection.get("secret_arn")
    if secret_arn:
        credentials = _get_credentials_from_secrets(secret_arn)
    else:
        # Fallback to test credentials for LocalStack
        credentials = {"user": "test", "password": "test"}

    # Build connection config for executor
    connection_config = {
        "account": connection.get("account", ""),
        "user": credentials.get("user", ""),
        "password": credentials.get("password", ""),
        "database": connection.get("database_name", ""),
        "schema": connection.get("schema_name", "PUBLIC"),
        "warehouse": connection.get("warehouse", "COMPUTE_WH"),
        "role": connection.get("role_name"),
    }

    table_name = dataset.get("table_name", dataset.get("name", "unknown_table"))
    dataset_id = UUID(dataset["id"])

    # Create executor and run
    executor = GreatExpectationsExecutor(connection_config=connection_config)

    # Run async executor in sync context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(
            executor.execute(
                dataset_id=dataset_id,
                quality_yaml=quality_yaml,
                table_name=table_name,
            )
        )
    finally:
        loop.close()

    return result


def _format_success_response(result: Any, dataset: Dict[str, Any], run_id: str) -> str:
    """Format successful quality check response."""
    # Count results
    total_checks = len(result.results)
    passed_checks = sum(1 for r in result.results if r.status == "passed")
    failed_checks = sum(1 for r in result.results if r.status == "failed")
    error_checks = sum(1 for r in result.results if r.status == "error")

    # Collect error messages for display
    error_messages = []
    for r in result.results:
        if r.status == "error" and hasattr(r, "message") and r.message:
            error_messages.append(r.message)

    # Save HTML report to disk
    report_path = _save_report(result.html_report, run_id) if result.html_report else None

    # Determine overall status
    if error_checks > 0:
        status = "error"
    elif failed_checks > 0:
        status = "warning"
    else:
        status = "completed"

    # Update run with results
    update_run(
        run_id=run_id,
        status=status,
        total_checks=total_checks,
        passed_checks=passed_checks,
        failed_checks=failed_checks,
        error_checks=error_checks,
        results_summary=f"Completed {total_checks} checks: {passed_checks} passed, {failed_checks} failed, {error_checks} errors.",
    )

    report_info = f"\n\n📄 **Report saved to:** `{report_path}`" if report_path else ""

    # Add error details if any
    error_info = ""
    if error_messages:
        error_info = "\n\n### ⚠️ Error Details\n" + "\n".join(f"- {msg}" for msg in error_messages[:5])

    return f"""✅ **Quality Check Completed (Great Expectations)**

**Dataset:** {dataset['name']}
**Run ID:** `{run_id}`

### Results Summary
| Metric | Value |
|--------|-------|
| Total Checks | {total_checks} |
| ✅ Passed | {passed_checks} |
| ❌ Failed | {failed_checks} |
| ⚠️ Errors | {error_checks} |
{report_info}{error_info}

View detailed results in the **Dashboard** → **Recent Runs** tab."""


def _save_report(html_report: str, run_id: str) -> Optional[str]:
    """Save HTML report to disk."""
    reports_dir = "/app/great_expectations"
    os.makedirs(reports_dir, exist_ok=True)
    report_filename = f"quality_report_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    report_path = os.path.join(reports_dir, report_filename)
    with open(report_path, "w") as f:
        f.write(html_report)
    return report_path


def _handle_quality_error(error: Exception, run_id: str) -> str:
    """Handle quality check error."""
    update_run(
        run_id=run_id,
        status="failed",
        total_checks=0,
        passed_checks=0,
        failed_checks=0,
        error_checks=1,
        results_summary=str(error),
    )
    return f"❌ Quality check failed: {str(error)}"

