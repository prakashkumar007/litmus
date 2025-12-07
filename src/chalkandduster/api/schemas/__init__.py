"""API Schemas - Pydantic models for request/response validation."""

from chalkandduster.api.schemas.common import (
    PaginatedResponse,
    ErrorResponse,
    SuccessResponse,
)
from chalkandduster.api.schemas.tenant import (
    TenantCreate,
    TenantUpdate,
    TenantResponse,
)
from chalkandduster.api.schemas.connection import (
    ConnectionCreate,
    ConnectionUpdate,
    ConnectionResponse,
    ConnectionTest,
)
from chalkandduster.api.schemas.dataset import (
    DatasetCreate,
    DatasetUpdate,
    DatasetResponse,
)
from chalkandduster.api.schemas.llm import (
    YAMLGenerateRequest,
    YAMLGenerateResponse,
    AlertEnhanceRequest,
    AlertEnhanceResponse,
)

__all__ = [
    # Common
    "PaginatedResponse",
    "ErrorResponse",
    "SuccessResponse",
    # Tenant
    "TenantCreate",
    "TenantUpdate", 
    "TenantResponse",
    # Connection
    "ConnectionCreate",
    "ConnectionUpdate",
    "ConnectionResponse",
    "ConnectionTest",
    # Dataset
    "DatasetCreate",
    "DatasetUpdate",
    "DatasetResponse",
    # LLM
    "YAMLGenerateRequest",
    "YAMLGenerateResponse",
    "AlertEnhanceRequest",
    "AlertEnhanceResponse",
]

