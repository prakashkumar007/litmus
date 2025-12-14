"""
Chalk and Duster - Lambda Quality Check Handler

AWS Lambda function for executing data quality checks using Great Expectations.

The handler fetches dataset and connection details from PostgreSQL,
including the quality_yaml configuration, then runs quality checks.
Results are stored in the runs table.
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Any, Dict
from uuid import UUID, uuid4

import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
)
logger = structlog.get_logger()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for quality checks.

    Event payload:
    {
        "dataset_id": "uuid",          # Required - fetches config from PostgreSQL
        "run_id": "uuid"               # Optional - if not provided, creates a new run
    }

    The handler:
    1. Fetches dataset from PostgreSQL (including quality_yaml config)
    2. Fetches connection details from PostgreSQL
    3. Gets credentials from Secrets Manager
    4. Fetches data from Snowflake
    5. Runs Great Expectations quality checks
    6. Stores results in the runs table
    """
    logger.info("Quality check Lambda invoked", payload=event)

    try:
        dataset_id = event.get("dataset_id")
        run_id = event.get("run_id")

        # Validate required fields
        if not dataset_id:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Missing required field: dataset_id",
                }),
            }

        # Run quality checks
        result = asyncio.get_event_loop().run_until_complete(
            run_quality_checks(
                dataset_id=UUID(dataset_id),
                run_id=UUID(run_id) if run_id else None,
            )
        )

        logger.info("Quality check completed", dataset_id=dataset_id, result=result)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "dataset_id": dataset_id,
                "status": "completed",
                "result": result,
                "completed_at": datetime.utcnow().isoformat(),
            }),
        }

    except Exception as e:
        logger.error("Quality check failed", error=str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "dataset_id": event.get("dataset_id"),
            }),
        }


async def get_dataset_with_connection(dataset_id: UUID) -> Dict[str, Any]:
    """Fetch dataset and its connection from PostgreSQL."""
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import selectinload

    from chalkandduster.db.postgres.models import Dataset

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")

    engine = create_async_engine(database_url)
    async with AsyncSession(engine) as session:
        stmt = (
            select(Dataset)
            .options(selectinload(Dataset.connection))
            .where(Dataset.id == dataset_id)
        )
        result = await session.execute(stmt)
        dataset = result.scalar_one_or_none()

        if not dataset:
            raise ValueError(f"Dataset not found: {dataset_id}")

        if not dataset.quality_yaml:
            raise ValueError(f"Dataset {dataset_id} has no quality_yaml configuration")

        return {
            "dataset": {
                "id": str(dataset.id),
                "tenant_id": str(dataset.tenant_id),
                "name": dataset.name,
                "database_name": dataset.database_name,
                "schema_name": dataset.schema_name,
                "table_name": dataset.table_name,
                "quality_yaml": dataset.quality_yaml,
            },
            "connection": {
                "id": str(dataset.connection.id),
                "account": dataset.connection.account,
                "warehouse": dataset.connection.warehouse,
                "database_name": dataset.connection.database_name,
                "schema_name": dataset.connection.schema_name,
                "role_name": dataset.connection.role_name,
                "secret_arn": dataset.connection.secret_arn,
            },
        }


async def get_snowflake_credentials(secret_arn: str) -> Dict[str, str]:
    """Fetch Snowflake credentials from Secrets Manager."""
    import boto3

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    client = boto3.client(
        "secretsmanager",
        endpoint_url=endpoint_url,
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )

    response = client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response["SecretString"])
    return {
        "user": secret.get("user") or secret.get("username"),
        "password": secret.get("password"),
    }


async def create_or_update_run(
    dataset_id: UUID,
    tenant_id: UUID,
    run_id: UUID | None,
    status: str,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    total_checks: int = 0,
    passed_checks: int = 0,
    failed_checks: int = 0,
    results: Dict[str, Any] | None = None,
    error_message: str | None = None,
) -> UUID:
    """Create or update a run record in PostgreSQL."""
    from sqlalchemy import update
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

    from chalkandduster.db.postgres.models import Run, Dataset

    database_url = os.environ.get("DATABASE_URL")
    engine = create_async_engine(database_url)

    async with AsyncSession(engine) as session:
        if run_id:
            # Update existing run
            stmt = (
                update(Run)
                .where(Run.id == run_id)
                .values(
                    status=status,
                    started_at=started_at,
                    completed_at=completed_at,
                    total_checks=total_checks,
                    passed_checks=passed_checks,
                    failed_checks=failed_checks,
                    results=results,
                    error_message=error_message,
                    duration_seconds=(completed_at - started_at).total_seconds() if completed_at and started_at else None,
                )
            )
            await session.execute(stmt)
        else:
            # Create new run
            run_id = uuid4()
            run = Run(
                id=run_id,
                dataset_id=dataset_id,
                tenant_id=tenant_id,
                run_type="quality",
                trigger_type="on_demand",
                status=status,
                started_at=started_at,
            )
            session.add(run)

        # Update dataset last run info
        stmt = (
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(
                last_quality_run_at=completed_at or started_at,
                last_quality_status=status,
            )
        )
        await session.execute(stmt)
        await session.commit()

    return run_id


async def run_quality_checks(
    dataset_id: UUID,
    run_id: UUID | None = None,
) -> Dict[str, Any]:
    """
    Execute quality checks using Great Expectations.

    Results are stored in the runs table.
    """
    started_at = datetime.utcnow()

    # Fetch dataset and connection from PostgreSQL
    data = await get_dataset_with_connection(dataset_id)
    dataset = data["dataset"]
    connection = data["connection"]
    tenant_id = UUID(dataset["tenant_id"])

    # Create/update run as "running"
    run_id = await create_or_update_run(
        dataset_id=dataset_id,
        tenant_id=tenant_id,
        run_id=run_id,
        status="running",
        started_at=started_at,
    )

    try:
        # Get Snowflake credentials from Secrets Manager
        credentials = await get_snowflake_credentials(connection["secret_arn"])

        from chalkandduster.quality.factory import get_quality_executor

        # Build connection config for the executor
        connection_config = {
            "account": connection["account"],
            "user": credentials["user"],
            "password": credentials["password"],
            "database": connection["database_name"],
            "schema": connection["schema_name"],
            "warehouse": connection["warehouse"],
            "role": connection.get("role_name"),
        }

        # Get the Great Expectations executor
        executor = get_quality_executor(connection_config=connection_config)

        # Execute checks
        result = await executor.execute(
            dataset_id=dataset_id,
            quality_yaml=dataset["quality_yaml"],
            table_name=dataset["table_name"],
        )

        completed_at = datetime.utcnow()

        # Count passed/failed
        passed = sum(1 for r in result.results if r.status == "passed") if result.results else 0
        failed = sum(1 for r in result.results if r.status == "failed") if result.results else 0

        # Format results
        results_list = [
            {
                "check_name": r.check_name,
                "check_type": r.check_type,
                "status": r.status,
                "severity": r.severity,
                "expected": getattr(r, 'expected', None),
                "actual": getattr(r, 'actual', None),
                "message": r.message,
            }
            for r in result.results
        ] if result.results else []

        results_data = {
            "run_id": str(result.run_id),
            "dataset_id": str(result.dataset_id),
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "completed_at": result.completed_at.isoformat() if result.completed_at else None,
            "status": result.status,
            "results": results_list,
        }

        # Save native Great Expectations HTML report
        report_location = None
        try:
            from chalkandduster.reports import ReportStorage
            from chalkandduster.core.config import settings

            if getattr(settings, 'REPORTS_ENABLED', True):
                report_storage = ReportStorage()

                # Use native GE report - required, no fallback
                html_report = result.html_report
                if not html_report:
                    raise ValueError(
                        "Native Great Expectations HTML report is required but was not generated. "
                        "Check that the quality check executor is properly generating native reports."
                    )

                logger.info("Using native Great Expectations report")

                save_result = await report_storage.save_report(
                    tenant_id=tenant_id,
                    dataset_id=dataset_id,
                    report_type="quality",
                    run_id=str(run_id),
                    html_content=html_report,
                )
                report_location = save_result.get("location")
                logger.info("Quality report saved", location=report_location)
        except Exception as e:
            logger.warning("Failed to save quality report", error=str(e))

        results_data["report_location"] = report_location

        # Update run as completed
        await create_or_update_run(
            dataset_id=dataset_id,
            tenant_id=tenant_id,
            run_id=run_id,
            status="completed",
            started_at=started_at,
            completed_at=completed_at,
            total_checks=len(result.results) if result.results else 0,
            passed_checks=passed,
            failed_checks=failed,
            results=results_data,
        )

        return results_data

    except Exception as e:
        # Update run as failed
        await create_or_update_run(
            dataset_id=dataset_id,
            tenant_id=tenant_id,
            run_id=run_id,
            status="failed",
            started_at=started_at,
            completed_at=datetime.utcnow(),
            error_message=str(e),
        )
        raise

