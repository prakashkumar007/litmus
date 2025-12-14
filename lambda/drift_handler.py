"""
Chalk and Duster - Lambda Drift Detection Handler

AWS Lambda function for executing drift detection using Evidently.

The handler fetches dataset and connection details from PostgreSQL,
including the drift_yaml configuration, then runs drift detection.
Results are stored in the runs table.

Reference data is automatically fetched using Snowflake Time Travel.
The table is dynamically determined from the datasets table which has
database_name, schema_name, and table_name.

Configure the time travel offset in drift_yaml:
- time_travel_days: Number of days to look back (default: 1)
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
    Lambda handler for drift detection.

    Event payload:
    {
        "dataset_id": "uuid",          # Required - fetches config from PostgreSQL
        "run_id": "uuid"               # Optional - if not provided, creates a new run
    }

    The handler:
    1. Fetches dataset from PostgreSQL (including drift_yaml config)
    2. Fetches connection details from PostgreSQL
    3. Gets credentials from Secrets Manager
    4. Fetches current and reference data from Snowflake
    5. Runs Evidently drift detection
    6. Stores results in the runs table
    """
    logger.info("Drift detection Lambda invoked", payload=event)

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

        # Run drift detection
        result = asyncio.get_event_loop().run_until_complete(
            run_drift_detection(
                dataset_id=UUID(dataset_id),
                run_id=UUID(run_id) if run_id else None,
            )
        )

        logger.info("Drift detection completed", dataset_id=dataset_id, result=result)

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
        logger.error("Drift detection failed", error=str(e))
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

        if not dataset.drift_yaml:
            raise ValueError(f"Dataset {dataset_id} has no drift_yaml configuration")

        return {
            "dataset": {
                "id": str(dataset.id),
                "tenant_id": str(dataset.tenant_id),
                "name": dataset.name,
                "database_name": dataset.database_name,
                "schema_name": dataset.schema_name,
                "table_name": dataset.table_name,
                "drift_yaml": dataset.drift_yaml,
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
                run_type="drift",
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
                last_drift_run_at=completed_at or started_at,
                last_drift_status=status,
            )
        )
        await session.execute(stmt)
        await session.commit()

    return run_id


async def run_drift_detection(
    dataset_id: UUID,
    run_id: UUID | None = None,
) -> Dict[str, Any]:
    """
    Execute drift detection using Evidently.

    Reference and current data are fetched from Snowflake.
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

        from chalkandduster.db.snowflake.connector import SnowflakeConnector
        from chalkandduster.drift.factory import get_drift_detector

        connector = SnowflakeConnector(
            account=connection["account"],
            user=credentials["user"],
            password=credentials["password"],
            database=connection["database_name"],
            schema=connection["schema_name"],
            warehouse=connection["warehouse"],
            role=connection.get("role_name"),
        )

        # Get the Evidently detector
        detector = get_drift_detector(snowflake_connector=connector)

        # Execute detection (reference data from config or split)
        result = await detector.detect(
            dataset_id=dataset_id,
            drift_yaml=dataset["drift_yaml"],
            table_name=dataset["table_name"],
            database=dataset["database_name"],
            schema=dataset["schema_name"],
            tenant_id=tenant_id,
        )

        completed_at = datetime.utcnow()

        # Format results
        results_list = [
            {
                "monitor_name": r.monitor_name,
                "drift_type": r.drift_type,
                "detected": r.detected,
                "severity": r.severity,
                "metric_value": r.metric_value,
                "threshold": r.threshold,
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

        # Save native Evidently HTML report
        report_location = None
        try:
            from chalkandduster.reports import ReportStorage
            from chalkandduster.core.config import settings

            if getattr(settings, 'REPORTS_ENABLED', True) and result.html_report:
                report_storage = ReportStorage()
                save_result = await report_storage.save_report(
                    tenant_id=tenant_id,
                    dataset_id=dataset_id,
                    report_type="drift",
                    run_id=str(run_id),
                    html_content=result.html_report,
                )
                report_location = save_result.get("location")
                logger.info("Native Evidently report saved", location=report_location)
        except Exception as e:
            logger.warning("Failed to save drift report", error=str(e))

        results_data["report_location"] = report_location

        # Update run as completed
        await create_or_update_run(
            dataset_id=dataset_id,
            tenant_id=tenant_id,
            run_id=run_id,
            status="completed",
            started_at=started_at,
            completed_at=completed_at,
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

