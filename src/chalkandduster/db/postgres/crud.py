"""
Chalk and Duster - CRUD Operations
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from chalkandduster.db.postgres.models import Connection, Dataset, Tenant, Run


# =============================================================================
# Tenant CRUD
# =============================================================================

async def create_tenant(
    db: AsyncSession,
    name: str,
    slug: str,
    description: Optional[str] = None,
    snowflake_account: Optional[str] = None,
    snowflake_database: Optional[str] = None,
    slack_webhook_url: Optional[str] = None,
    slack_channel: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> Tenant:
    """Create a new tenant."""
    tenant = Tenant(
        name=name,
        slug=slug,
        description=description,
        snowflake_account=snowflake_account,
        snowflake_database=snowflake_database,
        slack_webhook_url=slack_webhook_url,
        slack_channel=slack_channel,
        settings=settings or {},
    )
    db.add(tenant)
    await db.commit()
    await db.refresh(tenant)
    return tenant


async def get_tenant_by_id(db: AsyncSession, tenant_id: UUID) -> Optional[Tenant]:
    """Get a tenant by ID."""
    result = await db.execute(select(Tenant).where(Tenant.id == tenant_id))
    return result.scalar_one_or_none()


async def get_tenant_by_slug(db: AsyncSession, slug: str) -> Optional[Tenant]:
    """Get a tenant by slug."""
    result = await db.execute(select(Tenant).where(Tenant.slug == slug))
    return result.scalar_one_or_none()


async def list_tenants(
    db: AsyncSession,
    page: int = 1,
    page_size: int = 20,
    is_active: Optional[bool] = None,
) -> Tuple[List[Tenant], int]:
    """List tenants with pagination."""
    query = select(Tenant)
    count_query = select(func.count()).select_from(Tenant)
    
    if is_active is not None:
        query = query.where(Tenant.is_active == is_active)
        count_query = count_query.where(Tenant.is_active == is_active)
    
    # Get total count
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    # Get paginated results
    offset = (page - 1) * page_size
    query = query.offset(offset).limit(page_size).order_by(Tenant.created_at.desc())
    result = await db.execute(query)
    tenants = result.scalars().all()
    
    return list(tenants), total


async def update_tenant(
    db: AsyncSession,
    tenant: Tenant,
    **kwargs,
) -> Tenant:
    """Update a tenant with provided fields."""
    for field, value in kwargs.items():
        if hasattr(tenant, field) and value is not None:
            setattr(tenant, field, value)

    await db.commit()
    await db.refresh(tenant)
    return tenant


async def soft_delete_tenant(db: AsyncSession, tenant: Tenant) -> None:
    """Soft delete a tenant."""
    tenant.is_active = False
    await db.commit()


# =============================================================================
# Connection CRUD
# =============================================================================

async def create_connection(
    db: AsyncSession,
    tenant_id: UUID,
    name: str,
    account: str,
    database_name: str,
    warehouse: str = "COMPUTE_WH",
    schema_name: str = "PUBLIC",
    role_name: Optional[str] = None,
    secret_arn: Optional[str] = None,
    connection_type: str = "snowflake",
) -> Connection:
    """Create a new connection."""
    connection = Connection(
        tenant_id=tenant_id,
        name=name,
        connection_type=connection_type,
        account=account,
        warehouse=warehouse,
        database_name=database_name,
        schema_name=schema_name,
        role_name=role_name,
        secret_arn=secret_arn,
    )
    db.add(connection)
    await db.commit()
    await db.refresh(connection)
    return connection


async def get_connection_by_id(db: AsyncSession, connection_id: UUID) -> Optional[Connection]:
    """Get a connection by ID."""
    result = await db.execute(select(Connection).where(Connection.id == connection_id))
    return result.scalar_one_or_none()


async def get_connection_by_name(
    db: AsyncSession, tenant_id: UUID, name: str
) -> Optional[Connection]:
    """Get a connection by name within a tenant."""
    result = await db.execute(
        select(Connection).where(
            Connection.tenant_id == tenant_id,
            Connection.name == name,
        )
    )
    return result.scalar_one_or_none()


async def list_connections(
    db: AsyncSession,
    tenant_id: UUID,
    is_active: Optional[bool] = None,
) -> List[Connection]:
    """List connections for a tenant."""
    query = select(Connection).where(Connection.tenant_id == tenant_id)
    
    if is_active is not None:
        query = query.where(Connection.is_active == is_active)
    
    result = await db.execute(query.order_by(Connection.created_at.desc()))
    return list(result.scalars().all())


async def update_connection(
    db: AsyncSession,
    connection: Connection,
    **kwargs,
) -> Connection:
    """Update a connection with provided fields."""
    # Remove credential fields - they should be updated in Secrets Manager
    kwargs.pop("user", None)
    kwargs.pop("private_key", None)
    kwargs.pop("private_key_passphrase", None)
    kwargs.pop("password", None)

    for field, value in kwargs.items():
        if hasattr(connection, field) and value is not None:
            setattr(connection, field, value)

    await db.commit()
    await db.refresh(connection)
    return connection


async def update_connection_test_status(
    db: AsyncSession,
    connection: Connection,
    success: bool,
) -> None:
    """Update connection test status."""
    connection.last_tested_at = datetime.utcnow()
    connection.last_test_status = "success" if success else "failed"
    await db.commit()


async def delete_connection(db: AsyncSession, connection: Connection) -> None:
    """Delete a connection."""
    await db.delete(connection)
    await db.commit()


# =============================================================================
# Dataset CRUD
# =============================================================================

async def create_dataset(
    db: AsyncSession,
    tenant_id: UUID,
    connection_id: UUID,
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
) -> Dataset:
    """Create a new dataset."""
    dataset = Dataset(
        tenant_id=tenant_id,
        connection_id=connection_id,
        name=name,
        description=description,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        quality_yaml=quality_yaml,
        drift_yaml=drift_yaml,
        quality_schedule=quality_schedule,
        drift_schedule=drift_schedule,
        tags=tags or [],
    )
    db.add(dataset)
    await db.commit()
    await db.refresh(dataset)
    return dataset


async def get_dataset_by_id(db: AsyncSession, dataset_id: UUID) -> Optional[Dataset]:
    """Get a dataset by ID."""
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    return result.scalar_one_or_none()


async def list_datasets(
    db: AsyncSession,
    tenant_id: UUID,
    connection_id: Optional[UUID] = None,
    is_active: Optional[bool] = None,
    tags: Optional[List[str]] = None,
) -> List[Dataset]:
    """List datasets for a tenant."""
    query = select(Dataset).where(Dataset.tenant_id == tenant_id)

    if connection_id is not None:
        query = query.where(Dataset.connection_id == connection_id)

    if is_active is not None:
        query = query.where(Dataset.is_active == is_active)

    if tags:
        query = query.where(Dataset.tags.overlap(tags))

    result = await db.execute(query.order_by(Dataset.created_at.desc()))
    return list(result.scalars().all())


async def update_dataset(
    db: AsyncSession,
    dataset: Dataset,
    **kwargs,
) -> Dataset:
    """Update a dataset with provided fields."""
    for field, value in kwargs.items():
        if hasattr(dataset, field) and value is not None:
            setattr(dataset, field, value)

    await db.commit()
    await db.refresh(dataset)
    return dataset


async def delete_dataset(db: AsyncSession, dataset: Dataset) -> None:
    """Delete a dataset."""
    await db.delete(dataset)
    await db.commit()


# =============================================================================
# Run CRUD
# =============================================================================

async def create_run(
    db: AsyncSession,
    dataset_id: UUID,
    tenant_id: UUID,
    run_type: str,
    trigger_type: str = "on_demand",
) -> Run:
    """Create a new run.

    Args:
        db: Database session
        dataset_id: ID of the dataset
        tenant_id: ID of the tenant
        run_type: Type of run ('quality' or 'drift')
        trigger_type: How the run was triggered ('on_demand' or 'scheduled')
    """
    run = Run(
        dataset_id=dataset_id,
        tenant_id=tenant_id,
        run_type=run_type,
        trigger_type=trigger_type,
        status="pending",
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)
    return run


async def get_run_by_id(db: AsyncSession, run_id: UUID) -> Optional[Run]:
    """Get a run by ID."""
    result = await db.execute(select(Run).where(Run.id == run_id))
    return result.scalar_one_or_none()


async def update_run(
    db: AsyncSession,
    run: Run,
    status: str,
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
    duration_seconds: Optional[float] = None,
    total_checks: int = 0,
    passed_checks: int = 0,
    failed_checks: int = 0,
    error_checks: int = 0,
    results: Optional[dict] = None,
    error_message: Optional[str] = None,
) -> Run:
    """Update a run."""
    run.status = status
    if started_at:
        run.started_at = started_at
    if completed_at:
        run.completed_at = completed_at
    if duration_seconds is not None:
        run.duration_seconds = duration_seconds
    run.total_checks = total_checks
    run.passed_checks = passed_checks
    run.failed_checks = failed_checks
    run.error_checks = error_checks
    if results:
        run.results = results
    if error_message:
        run.error_message = error_message

    await db.commit()
    await db.refresh(run)
    return run


async def list_runs(
    db: AsyncSession,
    dataset_id: UUID,
    run_type: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
) -> Tuple[List[Run], int]:
    """List runs for a dataset."""
    query = select(Run).where(Run.dataset_id == dataset_id)

    if run_type:
        query = query.where(Run.run_type == run_type)
    if status:
        query = query.where(Run.status == status)

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_query)).scalar() or 0

    # Apply pagination
    query = query.order_by(Run.created_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    runs = list(result.scalars().all())

    return runs, total
