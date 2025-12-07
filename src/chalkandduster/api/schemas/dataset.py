"""
Chalk and Duster - Dataset API Schemas
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class DatasetBase(BaseModel):
    """Base dataset schema."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    
    # Table reference
    database_name: str
    schema_name: str
    table_name: str
    
    # Scheduling
    quality_schedule: Optional[str] = None  # Cron expression
    drift_schedule: Optional[str] = None    # Cron expression
    
    # Tags
    tags: List[str] = Field(default_factory=list)


class DatasetCreate(DatasetBase):
    """Schema for creating a dataset."""
    tenant_id: UUID
    connection_id: UUID
    
    # YAML configurations
    quality_yaml: Optional[str] = None
    drift_yaml: Optional[str] = None


class DatasetUpdate(BaseModel):
    """Schema for updating a dataset."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    
    # Scheduling
    quality_schedule: Optional[str] = None
    drift_schedule: Optional[str] = None
    
    # Tags
    tags: Optional[List[str]] = None
    
    # YAML configurations
    quality_yaml: Optional[str] = None
    drift_yaml: Optional[str] = None
    
    # Status
    is_active: Optional[bool] = None


class DatasetResponse(DatasetBase):
    """Schema for dataset response."""
    id: UUID
    tenant_id: UUID
    connection_id: UUID
    
    # YAML configurations
    quality_yaml: Optional[str] = None
    drift_yaml: Optional[str] = None
    
    # Status
    is_active: bool
    
    # Last run info
    last_quality_run_at: Optional[datetime] = None
    last_quality_status: Optional[str] = None
    last_drift_run_at: Optional[datetime] = None
    last_drift_status: Optional[str] = None
    
    # Audit
    created_at: datetime
    updated_at: datetime
    
    model_config = {"from_attributes": True}


class DatasetValidation(BaseModel):
    """Schema for YAML validation result."""
    valid: bool
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, Any]] = Field(default_factory=list)
    check_count: int = 0
    monitor_count: int = 0


class DatasetTriggerRequest(BaseModel):
    """Schema for manually triggering a dataset check."""
    check_type: str = Field(..., pattern="^(quality|drift|both)$")
    force: bool = Field(default=False)


class DatasetTriggerResponse(BaseModel):
    """Schema for dataset trigger response."""
    success: bool
    run_id: UUID
    check_type: str
    message: str

