"""
Chalk and Duster - Chat Handler Types

Type definitions for the chat system.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ConversationState(str, Enum):
    """Valid states for the conversation flow."""
    WELCOME = "welcome"
    AWAITING_TENANT_ID = "awaiting_tenant_id"
    CREATING_TENANT = "creating_tenant"
    TENANT_READY = "tenant_ready"
    CREATING_CONNECTION = "creating_connection"
    CONNECTION_READY = "connection_ready"
    AWAITING_DDL = "awaiting_ddl"
    REVIEWING_YAML = "reviewing_yaml"


class ChatMessage(BaseModel):
    """A single chat message."""
    role: str = Field(..., description="Message role: 'user' or 'assistant'")
    content: str = Field(..., description="Message content")
    timestamp: datetime = Field(default_factory=datetime.now, description="When message was created")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class PendingData(BaseModel):
    """Data being collected during multi-step flows."""
    # Tenant creation
    name: Optional[str] = None
    slug: Optional[str] = None
    
    # Connection creation
    step: Optional[str] = None
    account: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    connection_name: Optional[str] = None
    
    # Dataset creation
    table_name: Optional[str] = None
    ddl: Optional[str] = None
    quality_yaml: Optional[str] = None
    drift_yaml: Optional[str] = None
    
    # Action tracking
    action: Optional[str] = None
    yaml_type: Optional[str] = None


class SessionState(BaseModel):
    """Type-safe session state for the chat."""
    messages: List[ChatMessage] = Field(default_factory=list)
    conversation_state: ConversationState = Field(default=ConversationState.WELCOME)
    pending_data: PendingData = Field(default_factory=PendingData)
    awaiting_confirmation: bool = False
    tenant_id: Optional[str] = None
    connection_id: Optional[str] = None
    dataset_id: Optional[str] = None

