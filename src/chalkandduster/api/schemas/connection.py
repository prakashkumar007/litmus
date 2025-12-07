"""
Chalk and Duster - Connection API Schemas
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class ConnectionBase(BaseModel):
    """Base connection schema."""
    name: str = Field(..., min_length=1, max_length=255)
    connection_type: str = Field(default="snowflake")
    
    # Snowflake connection details
    account: str = Field(..., min_length=1)
    warehouse: str = Field(default="COMPUTE_WH")
    database_name: str = Field(..., min_length=1)
    schema_name: str = Field(default="PUBLIC")
    role_name: Optional[str] = None


class ConnectionCreate(ConnectionBase):
    """Schema for creating a connection."""
    tenant_id: UUID
    
    # Credentials (will be stored in Secrets Manager)
    user: str = Field(..., min_length=1)
    private_key: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    password: Optional[str] = None  # Alternative to key-pair


class ConnectionUpdate(BaseModel):
    """Schema for updating a connection."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    warehouse: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    role_name: Optional[str] = None
    is_active: Optional[bool] = None
    
    # Credentials update (optional)
    user: Optional[str] = None
    private_key: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    password: Optional[str] = None


class ConnectionResponse(ConnectionBase):
    """Schema for connection response."""
    id: UUID
    tenant_id: UUID
    
    # Secret reference (not the actual secret)
    secret_arn: Optional[str] = None
    
    # Status
    is_active: bool
    last_tested_at: Optional[datetime] = None
    last_test_status: Optional[str] = None
    
    # Audit
    created_at: datetime
    updated_at: datetime
    
    model_config = {"from_attributes": True}


class ConnectionTest(BaseModel):
    """Schema for connection test request."""
    connection_id: UUID


class ConnectionTestResult(BaseModel):
    """Schema for connection test result."""
    success: bool
    message: str
    latency_ms: Optional[float] = None
    snowflake_version: Optional[str] = None
    current_warehouse: Optional[str] = None
    current_database: Optional[str] = None
    current_role: Optional[str] = None

