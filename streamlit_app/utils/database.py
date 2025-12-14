"""
Litmus - Database Utilities for Streamlit

Provides synchronous database access for the Streamlit application.
Uses psycopg2 directly for synchronous operations to avoid async event loop issues.
"""

import os
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4
from datetime import datetime
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

# Get database URL from environment and convert to psycopg2 format
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://chalkandduster:chalkandduster@postgres:5432/chalkandduster"
)

# Convert SQLAlchemy URL to psycopg2 format
# postgresql+asyncpg://user:pass@host:port/db -> postgresql://user:pass@host:port/db
SYNC_DATABASE_URL = DATABASE_URL.replace("+asyncpg", "")


@contextmanager
def get_db_connection():
    """Get a synchronous database connection."""
    conn = psycopg2.connect(SYNC_DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_db_cursor(commit: bool = False):
    """Get a database cursor with automatic connection management."""
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_tenant_by_id(tenant_id: str) -> Optional[Dict[str, Any]]:
    """Get a tenant by ID."""
    try:
        with get_db_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM tenants WHERE id = %s",
                (tenant_id,)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
    except Exception:
        return None
    return None


def create_tenant(
    name: str,
    slug: str,
    description: Optional[str] = None,
    snowflake_account: Optional[str] = None,
    snowflake_database: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new tenant."""
    tenant_id = str(uuid4())
    now = datetime.utcnow()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            INSERT INTO tenants (id, name, slug, description, snowflake_account, snowflake_database, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, name, slug
            """,
            (tenant_id, name, slug, description, snowflake_account, snowflake_database, True, now, now)
        )
        row = cursor.fetchone()
        return {"id": str(row["id"]), "name": row["name"], "slug": row["slug"]}


def list_connections(tenant_id: str) -> List[Dict[str, Any]]:
    """List connections for a tenant."""
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
    """Create a new connection."""
    connection_id = str(uuid4())
    now = datetime.utcnow()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            INSERT INTO connections (id, tenant_id, name, account, database_name, schema_name, warehouse, role_name, secret_arn, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, name, account, database_name
            """,
            (connection_id, tenant_id, name, account, database_name, schema_name, warehouse, role_name, secret_arn, True, now, now)
        )
        row = cursor.fetchone()
        return {
            "id": str(row["id"]),
            "name": row["name"],
            "account": row["account"],
            "database_name": row["database_name"],
        }


def list_datasets(tenant_id: str, connection_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """List datasets for a tenant."""
    with get_db_cursor() as cursor:
        if connection_id:
            cursor.execute(
                "SELECT * FROM datasets WHERE tenant_id = %s AND connection_id = %s AND is_active = TRUE",
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
    """Create a new dataset."""
    dataset_id = str(uuid4())
    now = datetime.utcnow()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            INSERT INTO datasets (id, tenant_id, connection_id, name, description, database_name, schema_name, table_name,
                                  quality_yaml, drift_yaml, quality_schedule, drift_schedule, tags, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, name, table_name
            """,
            (dataset_id, tenant_id, connection_id, name, description, database_name, schema_name, table_name,
             quality_yaml, drift_yaml, quality_schedule, drift_schedule, tags, True, now, now)
        )
        row = cursor.fetchone()
        return {
            "id": str(row["id"]),
            "name": row["name"],
            "table_name": row["table_name"],
        }


def get_dataset_by_id(dataset_id: str) -> Optional[Dict[str, Any]]:
    """Get a dataset by ID."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM datasets WHERE id = %s",
            (dataset_id,)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
    return None


def update_dataset(dataset_id: str, **kwargs) -> Dict[str, Any]:
    """Update a dataset."""
    if not kwargs:
        return {}

    # Build SET clause dynamically
    set_parts = []
    values = []
    for key, value in kwargs.items():
        set_parts.append(f"{key} = %s")
        values.append(value)

    set_parts.append("updated_at = %s")
    values.append(datetime.utcnow())
    values.append(dataset_id)

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            f"UPDATE datasets SET {', '.join(set_parts)} WHERE id = %s RETURNING id, name, table_name",
            values
        )
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
    run_type: str,  # "quality" or "drift"
    trigger_type: str = "on_demand",
    status: str = "pending",
) -> Dict[str, Any]:
    """Create a new run record."""
    run_id = str(uuid4())
    now = datetime.utcnow()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            INSERT INTO runs (id, dataset_id, tenant_id, run_type, trigger_type, status, started_at, created_at, updated_at)
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
    """Update a run with results."""
    now = datetime.utcnow()

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            UPDATE runs SET status = %s, completed_at = %s, total_checks = %s,
                           passed_checks = %s, failed_checks = %s, error_checks = %s,
                           results_summary = %s, updated_at = %s
            WHERE id = %s
            RETURNING id, status, total_checks, passed_checks, failed_checks
            """,
            (status, now, total_checks, passed_checks, failed_checks, error_checks, results_summary, now, run_id)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
    return {}


def list_runs(tenant_id: str, dataset_id: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
    """List runs for a tenant, optionally filtered by dataset."""
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
    """Get a connection by ID."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM connections WHERE id = %s",
            (connection_id,)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
    return None

