"""
Chalk and Duster - Connection Routes
"""

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from chalkandduster.api.deps import get_db_session, require_tenant
from chalkandduster.api.schemas.connection import (
    ConnectionCreate,
    ConnectionResponse,
    ConnectionUpdate,
    ConnectionTestResult,
)
from chalkandduster.db.postgres import crud
from chalkandduster.db.postgres.models import Tenant

logger = structlog.get_logger()
router = APIRouter()


@router.post("", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(
    connection_in: ConnectionCreate,
    db: AsyncSession = Depends(get_db_session),
) -> ConnectionResponse:
    """
    Create a new Snowflake connection.
    
    Credentials are securely stored in AWS Secrets Manager.
    """
    # Verify tenant exists
    tenant = await crud.get_tenant_by_id(db, connection_in.tenant_id)
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tenant not found",
        )
    
    # Check for duplicate name within tenant
    existing = await crud.get_connection_by_name(
        db, connection_in.tenant_id, connection_in.name
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Connection '{connection_in.name}' already exists for this tenant",
        )
    
    # Create connection (credentials stored in Secrets Manager)
    connection = await crud.create_connection(db, connection_in)
    logger.info(
        "Connection created",
        connection_id=str(connection.id),
        tenant_id=str(connection.tenant_id),
    )
    
    return ConnectionResponse.model_validate(connection)


@router.get("", response_model=list[ConnectionResponse])
async def list_connections(
    tenant_id: UUID = Query(...),
    is_active: bool = Query(None),
    db: AsyncSession = Depends(get_db_session),
) -> list[ConnectionResponse]:
    """
    List connections for a tenant.
    """
    connections = await crud.list_connections(
        db, tenant_id=tenant_id, is_active=is_active
    )
    return [ConnectionResponse.model_validate(c) for c in connections]


@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection(
    connection_id: UUID,
    db: AsyncSession = Depends(get_db_session),
) -> ConnectionResponse:
    """
    Get a connection by ID.
    """
    connection = await crud.get_connection_by_id(db, connection_id)
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found",
        )
    
    return ConnectionResponse.model_validate(connection)


@router.patch("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(
    connection_id: UUID,
    connection_in: ConnectionUpdate,
    db: AsyncSession = Depends(get_db_session),
) -> ConnectionResponse:
    """
    Update a connection.
    """
    connection = await crud.get_connection_by_id(db, connection_id)
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found",
        )
    
    connection = await crud.update_connection(db, connection, connection_in)
    logger.info("Connection updated", connection_id=str(connection.id))
    
    return ConnectionResponse.model_validate(connection)


@router.post("/{connection_id}/test", response_model=ConnectionTestResult)
async def test_connection(
    connection_id: UUID,
    db: AsyncSession = Depends(get_db_session),
) -> ConnectionTestResult:
    """
    Test a Snowflake connection.
    """
    connection = await crud.get_connection_by_id(db, connection_id)
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found",
        )
    
    # Test the connection
    from chalkandduster.db.snowflake.connector import test_snowflake_connection
    
    result = await test_snowflake_connection(connection)
    
    # Update last test status
    await crud.update_connection_test_status(db, connection, result)
    
    return result


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: UUID,
    db: AsyncSession = Depends(get_db_session),
) -> None:
    """
    Delete a connection.
    """
    connection = await crud.get_connection_by_id(db, connection_id)
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found",
        )
    
    await crud.delete_connection(db, connection)
    logger.info("Connection deleted", connection_id=str(connection_id))

