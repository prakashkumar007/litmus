"""
Chalk and Duster - Tenant Routes
"""

from typing import List
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from chalkandduster.api.deps import get_db_session
from chalkandduster.api.schemas.tenant import (
    TenantCreate,
    TenantResponse,
    TenantUpdate,
    TenantListResponse,
)
from chalkandduster.db.postgres import crud

logger = structlog.get_logger()
router = APIRouter()


@router.post("", response_model=TenantResponse, status_code=status.HTTP_201_CREATED)
async def create_tenant(
    tenant_in: TenantCreate,
    db: AsyncSession = Depends(get_db_session),
) -> TenantResponse:
    """
    Create a new tenant.
    
    This registers a new organization in the platform.
    """
    # Check if slug already exists
    existing = await crud.get_tenant_by_slug(db, tenant_in.slug)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Tenant with slug '{tenant_in.slug}' already exists",
        )
    
    tenant = await crud.create_tenant(db, tenant_in)
    logger.info("Tenant created", tenant_id=str(tenant.id), slug=tenant.slug)
    
    return TenantResponse.model_validate(tenant)


@router.get("", response_model=TenantListResponse)
async def list_tenants(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    is_active: bool = Query(None),
    db: AsyncSession = Depends(get_db_session),
) -> TenantListResponse:
    """
    List all tenants with pagination.
    """
    tenants, total = await crud.list_tenants(
        db,
        page=page,
        page_size=page_size,
        is_active=is_active,
    )
    
    pages = (total + page_size - 1) // page_size if page_size > 0 else 0
    
    return TenantListResponse(
        items=[TenantResponse.model_validate(t) for t in tenants],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


@router.get("/{tenant_id}", response_model=TenantResponse)
async def get_tenant(
    tenant_id: UUID,
    db: AsyncSession = Depends(get_db_session),
) -> TenantResponse:
    """
    Get a tenant by ID.
    """
    tenant = await crud.get_tenant_by_id(db, tenant_id)
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tenant not found",
        )
    
    return TenantResponse.model_validate(tenant)


@router.patch("/{tenant_id}", response_model=TenantResponse)
async def update_tenant(
    tenant_id: UUID,
    tenant_in: TenantUpdate,
    db: AsyncSession = Depends(get_db_session),
) -> TenantResponse:
    """
    Update a tenant.
    """
    tenant = await crud.get_tenant_by_id(db, tenant_id)
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tenant not found",
        )
    
    tenant = await crud.update_tenant(db, tenant, tenant_in)
    logger.info("Tenant updated", tenant_id=str(tenant.id))
    
    return TenantResponse.model_validate(tenant)


@router.delete("/{tenant_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tenant(
    tenant_id: UUID,
    db: AsyncSession = Depends(get_db_session),
) -> None:
    """
    Delete a tenant.
    
    This is a soft delete - the tenant is marked as inactive.
    """
    tenant = await crud.get_tenant_by_id(db, tenant_id)
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tenant not found",
        )
    
    await crud.soft_delete_tenant(db, tenant)
    logger.info("Tenant deleted", tenant_id=str(tenant_id))

