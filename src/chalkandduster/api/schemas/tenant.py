"""
Chalk and Duster - Tenant API Schemas
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class TenantBase(BaseModel):
    """Base tenant schema."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    slack_webhook_url: Optional[str] = None
    slack_channel: Optional[str] = None


class TenantCreate(TenantBase):
    """Schema for creating a tenant."""
    slug: Optional[str] = Field(None, min_length=1, max_length=100)
    snowflake_account: Optional[str] = None
    snowflake_database: Optional[str] = None
    settings: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator("slug")
    @classmethod
    def validate_slug(cls, v: Optional[str], info) -> Optional[str]:
        """Validate and generate slug if not provided."""
        if v is None:
            # Generate slug from name
            name = info.data.get("name", "")
            v = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        
        if not re.match(r"^[a-z0-9-]+$", v):
            raise ValueError("Slug must contain only lowercase letters, numbers, and hyphens")
        
        return v


class TenantUpdate(BaseModel):
    """Schema for updating a tenant."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    slack_webhook_url: Optional[str] = None
    slack_channel: Optional[str] = None
    snowflake_account: Optional[str] = None
    snowflake_database: Optional[str] = None
    settings: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class TenantResponse(TenantBase):
    """Schema for tenant response."""
    id: UUID
    slug: str
    snowflake_account: Optional[str] = None
    snowflake_database: Optional[str] = None
    settings: Dict[str, Any] = Field(default_factory=dict)
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    # Computed fields
    connection_count: int = 0
    dataset_count: int = 0
    
    model_config = {"from_attributes": True}


class TenantListResponse(BaseModel):
    """Schema for tenant list response."""
    items: List[TenantResponse]
    total: int
    page: int
    page_size: int
    pages: int

