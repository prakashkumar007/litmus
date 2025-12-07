"""
Chalk and Duster - Tenant API Tests
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from chalkandduster.db.postgres.models import Tenant


@pytest.mark.asyncio
async def test_create_tenant(client: AsyncClient, db_session: AsyncSession):
    """Test creating a new tenant."""
    response = await client.post(
        "/api/v1/tenants",
        json={
            "name": "New Tenant",
            "description": "A new test tenant",
            "snowflake_account": "new_account",
            "snowflake_database": "NEW_DB",
        },
    )
    
    assert response.status_code == 201
    data = response.json()
    
    assert data["name"] == "New Tenant"
    assert data["slug"] == "new-tenant"
    assert data["is_active"] is True
    assert "id" in data


@pytest.mark.asyncio
async def test_create_tenant_with_custom_slug(client: AsyncClient):
    """Test creating a tenant with a custom slug."""
    response = await client.post(
        "/api/v1/tenants",
        json={
            "name": "Custom Slug Tenant",
            "slug": "custom-slug",
        },
    )
    
    assert response.status_code == 201
    data = response.json()
    
    assert data["slug"] == "custom-slug"


@pytest.mark.asyncio
async def test_create_tenant_duplicate_slug(client: AsyncClient, sample_tenant: Tenant):
    """Test that duplicate slugs are rejected."""
    response = await client.post(
        "/api/v1/tenants",
        json={
            "name": "Another Tenant",
            "slug": sample_tenant.slug,
        },
    )
    
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_list_tenants(client: AsyncClient, sample_tenant: Tenant):
    """Test listing tenants."""
    response = await client.get("/api/v1/tenants")
    
    assert response.status_code == 200
    data = response.json()
    
    assert "items" in data
    assert data["total"] >= 1
    assert any(t["id"] == str(sample_tenant.id) for t in data["items"])


@pytest.mark.asyncio
async def test_get_tenant(client: AsyncClient, sample_tenant: Tenant):
    """Test getting a tenant by ID."""
    response = await client.get(f"/api/v1/tenants/{sample_tenant.id}")
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["id"] == str(sample_tenant.id)
    assert data["name"] == sample_tenant.name


@pytest.mark.asyncio
async def test_get_tenant_not_found(client: AsyncClient):
    """Test getting a non-existent tenant."""
    from uuid import uuid4
    
    response = await client.get(f"/api/v1/tenants/{uuid4()}")
    
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_tenant(client: AsyncClient, sample_tenant: Tenant):
    """Test updating a tenant."""
    response = await client.patch(
        f"/api/v1/tenants/{sample_tenant.id}",
        json={
            "name": "Updated Tenant Name",
            "description": "Updated description",
        },
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["name"] == "Updated Tenant Name"
    assert data["description"] == "Updated description"


@pytest.mark.asyncio
async def test_delete_tenant(client: AsyncClient, sample_tenant: Tenant):
    """Test deleting (soft delete) a tenant."""
    response = await client.delete(f"/api/v1/tenants/{sample_tenant.id}")
    
    assert response.status_code == 204
    
    # Verify tenant is inactive
    get_response = await client.get(f"/api/v1/tenants/{sample_tenant.id}")
    assert get_response.status_code == 200
    assert get_response.json()["is_active"] is False

