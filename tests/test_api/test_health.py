"""
Chalk and Duster - Health Endpoint Tests
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    """Test the health check endpoint."""
    response = await client.get("/health")
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert data["version"] == "0.1.0"
    assert "checks" in data


@pytest.mark.asyncio
async def test_readiness_check(client: AsyncClient):
    """Test the readiness check endpoint."""
    response = await client.get("/ready")
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["status"] == "ready"


@pytest.mark.asyncio
async def test_liveness_check(client: AsyncClient):
    """Test the liveness check endpoint."""
    response = await client.get("/live")
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["status"] == "alive"

