"""
Chalk and Duster - CRUD Operations
"""

from datetime import datetime
from typing import List, Optional, Tuple
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from chalkandduster.db.postgres.models import Connection, Dataset, Tenant, Run
from chalkandduster.api.schemas.tenant import TenantCreate, TenantUpdate
from chalkandduster.api.schemas.connection import ConnectionCreate, ConnectionUpdate, ConnectionTestResult
from chalkandduster.api.schemas.dataset import DatasetCreate, DatasetUpdate


# =============================================================================
# Tenant CRUD
# =============================================================================

async def create_tenant(db: AsyncSession, tenant_in: TenantCreate) -> Tenant:
    """Create a new tenant."""
    tenant = Tenant(
        name=tenant_in.name,
        slug=tenant_in.slug,
        description=tenant_in.description,
        snowflake_account=tenant_in.snowflake_account,
        snowflake_database=tenant_in.snowflake_database,
        slack_webhook_url=tenant_in.slack_webhook_url,
        slack_channel=tenant_in.slack_channel,
        settings=tenant_in.settings,
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
    db: AsyncSession, tenant: Tenant, tenant_in: TenantUpdate
) -> Tenant:
    """Update a tenant."""
    update_data = tenant_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
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

async def create_connection(db: AsyncSession, conn_in: ConnectionCreate) -> Connection:
    """Create a new connection."""
    # TODO: Store credentials in Secrets Manager and get ARN
    connection = Connection(
        tenant_id=conn_in.tenant_id,
        name=conn_in.name,
        connection_type=conn_in.connection_type,
        account=conn_in.account,
        warehouse=conn_in.warehouse,
        database_name=conn_in.database_name,
        schema_name=conn_in.schema_name,
        role_name=conn_in.role_name,
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
    db: AsyncSession, connection: Connection, conn_in: ConnectionUpdate
) -> Connection:
    """Update a connection."""
    update_data = conn_in.model_dump(exclude_unset=True)
    # Remove credential fields - they should be updated in Secrets Manager
    update_data.pop("user", None)
    update_data.pop("private_key", None)
    update_data.pop("private_key_passphrase", None)
    update_data.pop("password", None)
    
    for field, value in update_data.items():
        setattr(connection, field, value)
    
    await db.commit()
    await db.refresh(connection)
    return connection


async def update_connection_test_status(
    db: AsyncSession, connection: Connection, result: ConnectionTestResult
) -> None:
    """Update connection test status."""
    connection.last_tested_at = datetime.utcnow()
    connection.last_test_status = "success" if result.success else "failed"
    await db.commit()


async def delete_connection(db: AsyncSession, connection: Connection) -> None:
    """Delete a connection."""
    await db.delete(connection)
    await db.commit()


# =============================================================================
# Dataset CRUD
# =============================================================================

async def create_dataset(db: AsyncSession, dataset_in: DatasetCreate) -> Dataset:
    """Create a new dataset."""
    dataset = Dataset(
        tenant_id=dataset_in.tenant_id,
        connection_id=dataset_in.connection_id,
        name=dataset_in.name,
        description=dataset_in.description,
        database_name=dataset_in.database_name,
        schema_name=dataset_in.schema_name,
        table_name=dataset_in.table_name,
        quality_yaml=dataset_in.quality_yaml,
        drift_yaml=dataset_in.drift_yaml,
        quality_schedule=dataset_in.quality_schedule,
        drift_schedule=dataset_in.drift_schedule,
        tags=dataset_in.tags,
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
    db: AsyncSession, dataset: Dataset, dataset_in: DatasetUpdate
) -> Dataset:
    """Update a dataset."""
    update_data = dataset_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
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
