"""
Chalk and Duster - Chat Handlers Module

Modular handlers for different conversation states and actions.
"""

from streamlit_app.handlers.types import (
    ChatMessage,
    ConversationState,
    PendingData,
)
from streamlit_app.handlers.tenant import (
    handle_welcome_state,
    handle_tenant_id_input,
    handle_tenant_creation,
)
from streamlit_app.handlers.connection import (
    handle_tenant_ready_state,
    handle_connection_creation,
)
from streamlit_app.handlers.dataset import (
    handle_connection_ready_state,
    handle_ddl_input,
    handle_yaml_review,
)
from streamlit_app.handlers.confirmation import handle_confirmation
from streamlit_app.handlers.quality import trigger_quality_check
from streamlit_app.handlers.drift import trigger_drift_check
from streamlit_app.handlers.utils import (
    add_message,
    get_welcome_message,
    get_llm_response,
)

__all__ = [
    # Types
    "ChatMessage",
    "ConversationState",
    "PendingData",
    # Tenant handlers
    "handle_welcome_state",
    "handle_tenant_id_input",
    "handle_tenant_creation",
    # Connection handlers
    "handle_tenant_ready_state",
    "handle_connection_creation",
    # Dataset handlers
    "handle_connection_ready_state",
    "handle_ddl_input",
    "handle_yaml_review",
    # Confirmation handler
    "handle_confirmation",
    # Check handlers
    "trigger_quality_check",
    "trigger_drift_check",
    # Utilities
    "add_message",
    "get_welcome_message",
    "get_llm_response",
]

