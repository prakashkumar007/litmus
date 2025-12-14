"""
Chalk and Duster - Database Utilities for Streamlit

Provides synchronous database access for the Streamlit application.
Uses psycopg2 with connection pooling for efficient database operations.
"""

import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional
from uuid import uuid4


def _utc_now() -> datetime:
    """Get current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://chalkandduster:chalkandduster@postgres:5432/chalkandduster"
)

# Convert SQLAlchemy URL to psycopg2 format
SYNC_DATABASE_URL = DATABASE_URL.replace("+asyncpg", "")

# Connection pool configuration
MIN_CONNECTIONS = int(os.getenv("DB_POOL_MIN", "1"))
MAX_CONNECTIONS = int(os.getenv("DB_POOL_MAX", "10"))

# Global connection pool (lazy initialization)
_connection_pool: Optional[ThreadedConnectionPool] = None


def _get_pool() -> ThreadedConnectionPool:
    """Get or create the connection pool."""
    global _connection_pool
    if _connection_pool is None:
        try:
            _connection_pool = ThreadedConnectionPool(
                minconn=MIN_CONNECTIONS,
                maxconn=MAX_CONNECTIONS,
                dsn=SYNC_DATABASE_URL,
            )
            logger.info("Database connection pool initialized")
        except psycopg2.Error as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    return _connection_pool


@contextmanager
def get_db_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Get a database connection from the pool.

    Yields:
        A psycopg2 connection object

    Raises:
        psycopg2.Error: If connection cannot be obtained
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


@contextmanager
def get_db_cursor(
    commit: bool = False
) -> Generator[psycopg2.extensions.cursor, None, None]:
    """
    Get a database cursor with automatic connection management.

    Args:
        commit: Whether to commit the transaction on success

    Yields:
        A RealDictCursor for executing queries

    Raises:
        psycopg2.Error: If database operation fails
    """
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            if commit:
                conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cursor.close()


def get_tenant_by_id(tenant_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a tenant by ID.

    Args:
        tenant_id: UUID of the tenant

    Returns:
        Tenant data dict or None if not found
    """
    try:
        with get_db_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM tenants WHERE id = %s",
                (tenant_id,)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
    except psycopg2.Error as e:
        logger.warning(f"Failed to get tenant {tenant_id}: {e}")
        return None
    return None


def create_tenant(
    name: str,
    slug: str,
    description: Optional[str] = None,
    snowflake_account: Optional[str] = None,
    snowflake_database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new tenant.

    Args:
        name: Organization name
        slug: URL-friendly identifier
        description: Optional description
        snowflake_account: Optional Snowflake account
        snowflake_database: Optional default database

    Returns:
        Created tenant data with id, name, slug

    Raises:
        psycopg2.Error: If database operation fails
    """
    tenant_id = str(uuid4())
    now = _utc_now()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            INSERT INTO tenants (id, name, slug, description, snowflake_account,
                                snowflake_database, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, name, slug
            """,
            (tenant_id, name, slug, description, snowflake_account,
             snowflake_database, True, now, now)
        )
        row = cursor.fetchone()
        return {"id": str(row["id"]), "name": row["name"], "slug": row["slug"]}


def list_connections(tenant_id: str) -> List[Dict[str, Any]]:
    """
    List active connections for a tenant.

    Args:
        tenant_id: UUID of the tenant

    Returns:
        List of connection data dicts
    """
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM connections WHERE tenant_id = %s AND is_active = TRUE",
            (tenant_id,)
        )
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def create_connection(
    tenant_id: str,
    name: str,
    account: str,
    database_name: str,
    schema_name: str = "PUBLIC",
    warehouse: str = "COMPUTE_WH",
    role_name: Optional[str] = None,
    secret_arn: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new Snowflake connection.

    Args:
        tenant_id: UUID of the tenant
        name: Connection display name
        account: Snowflake account identifier
        database_name: Default database
        schema_name: Default schema
        warehouse: Default warehouse
        role_name: Optional role name
        secret_arn: Optional AWS Secrets Manager ARN

    Returns:
        Created connection data

    Raises:
        psycopg2.Error: If database operation fails
    """
    connection_id = str(uuid4())
    now = _utc_now()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            INSERT INTO connections (id, tenant_id, name, account, database_name,
                                    schema_name, warehouse, role_name, secret_arn,
                                    is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, name, account, database_name
            """,
            (connection_id, tenant_id, name, account, database_name, schema_name,
             warehouse, role_name, secret_arn, True, now, now)
        )
        row = cursor.fetchone()
        return {
            "id": str(row["id"]),
            "name": row["name"],
            "account": row["account"],
            "database_name": row["database_name"],
        }


def list_datasets(
    tenant_id: str,
    connection_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    List active datasets for a tenant.

    Args:
        tenant_id: UUID of the tenant
        connection_id: Optional filter by connection

    Returns:
        List of dataset data dicts
    """
    with get_db_cursor() as cursor:
        if connection_id:
            cursor.execute(
                """SELECT * FROM datasets
                   WHERE tenant_id = %s AND connection_id = %s AND is_active = TRUE""",
                (tenant_id, connection_id)
            )
        else:
            cursor.execute(
                "SELECT * FROM datasets WHERE tenant_id = %s AND is_active = TRUE",
                (tenant_id,)
            )
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def create_dataset(
    tenant_id: str,
    connection_id: str,
    name: str,
    database_name: str,
    schema_name: str,
    table_name: str,
    description: Optional[str] = None,
    quality_yaml: Optional[str] = None,
    drift_yaml: Optional[str] = None,
    quality_schedule: Optional[str] = None,
    drift_schedule: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Create a new dataset with quality and drift configuration.

    Args:
        tenant_id: UUID of the tenant
        connection_id: UUID of the connection
        name: Dataset display name
        database_name: Snowflake database
        schema_name: Snowflake schema
        table_name: Table name
        description: Optional description
        quality_yaml: Great Expectations YAML config
        drift_yaml: Evidently YAML config
        quality_schedule: Cron expression for quality checks
        drift_schedule: Cron expression for drift detection
        tags: Optional list of tags

    Returns:
        Created dataset data

    Raises:
        psycopg2.Error: If database operation fails
    """
    dataset_id = str(uuid4())
    now = _utc_now()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            INSERT INTO datasets (id, tenant_id, connection_id, name, description,
                                  database_name, schema_name, table_name,
                                  quality_yaml, drift_yaml, quality_schedule,
                                  drift_schedule, tags, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, name, table_name
            """,
            (dataset_id, tenant_id, connection_id, name, description,
             database_name, schema_name, table_name,
             quality_yaml, drift_yaml, quality_schedule, drift_schedule,
             tags, True, now, now)
        )
        row = cursor.fetchone()
        return {
            "id": str(row["id"]),
            "name": row["name"],
            "table_name": row["table_name"],
        }


def get_dataset_by_id(dataset_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a dataset by ID.

    Args:
        dataset_id: UUID of the dataset

    Returns:
        Dataset data dict or None if not found
    """
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM datasets WHERE id = %s",
            (dataset_id,)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
    return None


def update_dataset(dataset_id: str, **kwargs: Any) -> Dict[str, Any]:
    """
    Update a dataset with the provided fields.

    Args:
        dataset_id: UUID of the dataset
        **kwargs: Fields to update

    Returns:
        Updated dataset data or empty dict if not found
    """
    if not kwargs:
        return {}

    # Build SET clause dynamically
    set_parts: List[str] = []
    values: List[Any] = []
    for key, value in kwargs.items():
        set_parts.append(f"{key} = %s")
        values.append(value)

    set_parts.append("updated_at = %s")
    values.append(_utc_now())
    values.append(dataset_id)

    with get_db_cursor(commit=True) as cursor:
        query = f"UPDATE datasets SET {', '.join(set_parts)} WHERE id = %s RETURNING id, name, table_name"
        cursor.execute(query, values)
        row = cursor.fetchone()
        if row:
            return {
                "id": str(row["id"]),
                "name": row["name"],
                "table_name": row["table_name"],
            }
    return {}


def create_run(
    dataset_id: str,
    tenant_id: str,
    run_type: str,
    trigger_type: str = "on_demand",
    status: str = "pending",
) -> Dict[str, Any]:
    """
    Create a new run record.

    Args:
        dataset_id: UUID of the dataset
        tenant_id: UUID of the tenant
        run_type: Type of run ("quality" or "drift")
        trigger_type: How the run was triggered ("on_demand", "scheduled")
        status: Initial status ("pending", "running")

    Returns:
        Created run data

    Raises:
        psycopg2.Error: If database operation fails
    """
    run_id = str(uuid4())
    now = _utc_now()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            INSERT INTO runs (id, dataset_id, tenant_id, run_type, trigger_type,
                             status, started_at, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, run_type, status
            """,
            (run_id, dataset_id, tenant_id, run_type, trigger_type, status, now, now, now)
        )
        row = cursor.fetchone()
        return {
            "id": str(row["id"]),
            "run_type": row["run_type"],
            "status": row["status"],
        }


def update_run(
    run_id: str,
    status: str,
    total_checks: int = 0,
    passed_checks: int = 0,
    failed_checks: int = 0,
    error_checks: int = 0,
    results_summary: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update a run with results.

    Args:
        run_id: UUID of the run
        status: New status ("completed", "failed", "warning")
        total_checks: Total number of checks executed
        passed_checks: Number of passed checks
        failed_checks: Number of failed checks
        error_checks: Number of checks with errors
        results_summary: Human-readable summary

    Returns:
        Updated run data or empty dict if not found
    """
    now = _utc_now()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            UPDATE runs SET status = %s, completed_at = %s, total_checks = %s,
                           passed_checks = %s, failed_checks = %s, error_checks = %s,
                           results_summary = %s, updated_at = %s
            WHERE id = %s
            RETURNING id, status, total_checks, passed_checks, failed_checks
            """,
            (status, now, total_checks, passed_checks, failed_checks,
             error_checks, results_summary, now, run_id)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
    return {}


def list_runs(
    tenant_id: str,
    dataset_id: Optional[str] = None,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    List runs for a tenant with optional dataset filter.

    Args:
        tenant_id: UUID of the tenant
        dataset_id: Optional filter by dataset
        limit: Maximum number of runs to return

    Returns:
        List of run data dicts with dataset info
    """
    with get_db_cursor() as cursor:
        if dataset_id:
            cursor.execute(
                """
                SELECT r.*, d.name as dataset_name, d.table_name
                FROM runs r
                JOIN datasets d ON r.dataset_id = d.id
                WHERE r.tenant_id = %s AND r.dataset_id = %s
                ORDER BY r.created_at DESC
                LIMIT %s
                """,
                (tenant_id, dataset_id, limit)
            )
        else:
            cursor.execute(
                """
                SELECT r.*, d.name as dataset_name, d.table_name
                FROM runs r
                JOIN datasets d ON r.dataset_id = d.id
                WHERE r.tenant_id = %s
                ORDER BY r.created_at DESC
                LIMIT %s
                """,
                (tenant_id, limit)
            )
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def get_connection_by_id(connection_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a connection by ID.

    Args:
        connection_id: UUID of the connection

    Returns:
        Connection data dict or None if not found
    """
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM connections WHERE id = %s",
            (connection_id,)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
    return None

