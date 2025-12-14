"""
Chalk and Duster - Lambda Baseline Handler

AWS Lambda function for managing Evidently drift detection baselines.
Supports creating, updating, and deleting baseline data in S3.

The handler fetches dataset and connection details from PostgreSQL,
then connects to Snowflake to fetch baseline data.
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

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
    Lambda handler for baseline management.

    Event payload:
    {
        "action": "create" | "update" | "delete" | "get",
        "dataset_id": "uuid",           # Required - fetches config from PostgreSQL
        "columns": ["col1", "col2"],    # Optional - filter columns for baseline
        "sample_size": 10000            # Optional - max rows for baseline
    }

    The handler:
    1. Fetches dataset details from PostgreSQL (table_name, database, schema)
    2. Fetches connection details from PostgreSQL (Snowflake account, warehouse, etc.)
    3. Gets credentials from Secrets Manager using connection.secret_arn
    4. Connects to Snowflake and fetches baseline data
    5. Stores baseline in S3
    """
    logger.info("Baseline Lambda invoked", payload=event)

    try:
        action = event.get("action", "create")
        dataset_id = event.get("dataset_id")
        columns = event.get("columns")
        sample_size = event.get("sample_size", 10000)

        # Validate required fields
        if not dataset_id:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Missing required field: dataset_id",
                }),
            }

        # Execute action
        result = asyncio.get_event_loop().run_until_complete(
            execute_baseline_action(
                action=action,
                dataset_id=UUID(dataset_id),
                columns=columns,
                sample_size=sample_size,
            )
        )

        logger.info("Baseline action completed", action=action, dataset_id=dataset_id, result=result)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "dataset_id": dataset_id,
                "action": action,
                "status": "completed",
                "result": result,
                "completed_at": datetime.utcnow().isoformat(),
            }),
        }

    except Exception as e:
        logger.error("Baseline action failed", error=str(e))
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

    from chalkandduster.db.postgres.models import Dataset, Connection

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")

    engine = create_async_engine(database_url)
    async with AsyncSession(engine) as session:
        # Fetch dataset with connection
        stmt = (
            select(Dataset)
            .options(selectinload(Dataset.connection))
            .where(Dataset.id == dataset_id)
        )
        result = await session.execute(stmt)
        dataset = result.scalar_one_or_none()

        if not dataset:
            raise ValueError(f"Dataset not found: {dataset_id}")

        return {
            "dataset": {
                "id": str(dataset.id),
                "tenant_id": str(dataset.tenant_id),
                "name": dataset.name,
                "database_name": dataset.database_name,
                "schema_name": dataset.schema_name,
                "table_name": dataset.table_name,
                "quality_yaml": dataset.quality_yaml,
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


async def execute_baseline_action(
    action: str,
    dataset_id: UUID,
    columns: Optional[List[str]],
    sample_size: int,
) -> Dict[str, Any]:
    """Execute the baseline action."""
    from chalkandduster.drift.baseline_storage import BaselineStorage

    storage = BaselineStorage()

    # Fetch dataset and connection from PostgreSQL
    data = await get_dataset_with_connection(dataset_id)
    dataset = data["dataset"]
    connection = data["connection"]
    tenant_id = UUID(dataset["tenant_id"])

    if action == "get":
        # Get baseline metadata
        metadata = await storage.get_metadata(tenant_id, dataset_id)
        if metadata:
            return {"exists": True, "metadata": metadata}
        return {"exists": False}

    elif action == "delete":
        # Delete baseline
        success = await storage.delete_baseline(tenant_id, dataset_id)
        return {"deleted": success}

    elif action in ("create", "update"):
        # Get Snowflake credentials from Secrets Manager
        credentials = await get_snowflake_credentials(connection["secret_arn"])

        from chalkandduster.db.snowflake.connector import SnowflakeConnector

        connector = SnowflakeConnector(
            account=connection["account"],
            user=credentials["user"],
            password=credentials["password"],
            database=connection["database_name"],
            schema=connection["schema_name"],
            warehouse=connection["warehouse"],
            role=connection.get("role_name"),
        )

        # Fetch data from Snowflake
        baseline_data = await fetch_baseline_data(
            connector=connector,
            table_name=dataset["table_name"],
            database=dataset["database_name"],
            schema=dataset["schema_name"],
            columns=columns,
            sample_size=sample_size,
        )

        # Save baseline to S3
        result = await storage.save_baseline(
            tenant_id=tenant_id,
            dataset_id=dataset_id,
            data=baseline_data,
            metadata={
                "table_name": dataset["table_name"],
                "database": dataset["database_name"],
                "schema": dataset["schema_name"],
                "sample_size": sample_size,
            },
        )

        return result

    else:
        raise ValueError(f"Unknown action: {action}")


async def fetch_baseline_data(
    connector,
    table_name: str,
    database: Optional[str],
    schema: Optional[str],
    columns: Optional[List[str]],
    sample_size: int,
):
    """Fetch baseline data from Snowflake."""
    import pandas as pd

    # Build fully qualified table name
    parts = []
    if database:
        parts.append(database)
    if schema:
        parts.append(schema)
    parts.append(table_name)
    full_table_name = ".".join(parts)

    # Build column selection
    column_select = ", ".join(columns) if columns else "*"

    # Query with sample size limit
    query = f"SELECT {column_select} FROM {full_table_name} LIMIT {sample_size}"

    result = await connector.execute_query(query)
    return pd.DataFrame(result)

