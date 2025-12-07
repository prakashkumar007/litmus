"""
Chalk and Duster - API Dependencies
"""

from typing import AsyncGenerator, Optional
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from chalkandduster.db.postgres.session import async_session_factory
from chalkandduster.db.postgres.models import Tenant


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency to get database session."""
    async with async_session_factory() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_current_tenant(
    x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-ID"),
    db: AsyncSession = Depends(get_db_session),
) -> Optional[Tenant]:
    """
    Get the current tenant from the X-Tenant-ID header.
    
    Returns None if no tenant header is provided.
    """
    if not x_tenant_id:
        return None
    
    try:
        tenant_id = UUID(x_tenant_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid tenant ID format",
        )
    
    from chalkandduster.db.postgres.crud import get_tenant_by_id
    
    tenant = await get_tenant_by_id(db, tenant_id)
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tenant not found",
        )
    
    if not tenant.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Tenant is inactive",
        )
    
    return tenant


async def require_tenant(
    tenant: Optional[Tenant] = Depends(get_current_tenant),
) -> Tenant:
    """
    Require a tenant to be provided.
    
    Raises 401 if no tenant header is provided.
    """
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-Tenant-ID header is required",
        )
    return tenant


class RateLimiter:
    """Simple rate limiter for API endpoints."""
    
    def __init__(self, calls_per_minute: int = 60):
        self.calls_per_minute = calls_per_minute
        self._calls: dict = {}
    
    async def check(self, key: str) -> bool:
        """Check if rate limit is exceeded."""
        # TODO: Implement proper rate limiting with Redis
        return True


def get_rate_limiter(calls_per_minute: int = 60) -> RateLimiter:
    """Get a rate limiter instance."""
    return RateLimiter(calls_per_minute)

