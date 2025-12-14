"""
Chalk and Duster - Tenant Handlers

Handlers for tenant-related conversation states.
"""

from typing import Dict, Any, Optional

import streamlit as st

from streamlit_app.utils.database import (
    get_tenant_by_id,
    create_tenant,
    list_connections,
)


def handle_welcome_state(user_input: str) -> str:
    """
    Handle the initial welcome state.
    
    Determines if user has an existing tenant or needs to create one.
    
    Args:
        user_input: User's response to the welcome message
        
    Returns:
        Response message for the user
    """
    lower_input = user_input.lower()
    
    if "yes" in lower_input or "have" in lower_input or "existing" in lower_input:
        st.session_state.conversation_state = "awaiting_tenant_id"
        return "Great! Please enter your **tenant ID** (UUID format):"
    
    elif "no" in lower_input or "new" in lower_input or "create" in lower_input or "don't" in lower_input:
        st.session_state.conversation_state = "creating_tenant"
        return """Let's create a new tenant for you!

I'll need a few details:
1. **Organization Name** (e.g., "Acme Corp")
2. **Slug** (short identifier, e.g., "acme-corp")

Please provide your **organization name**:"""
    
    else:
        # Check if they directly provided a UUID
        if len(user_input.replace("-", "")) == 32:
            return handle_tenant_id_input(user_input)
        return "I didn't quite catch that. Do you have an existing tenant ID? (yes/no)"


def handle_tenant_id_input(tenant_id: str) -> str:
    """
    Handle tenant ID input and validation.
    
    Validates the tenant ID and retrieves tenant information.
    
    Args:
        tenant_id: The tenant ID provided by the user
        
    Returns:
        Response message with tenant info or error
    """
    tenant_id = tenant_id.strip()
    
    try:
        tenant = get_tenant_by_id(tenant_id)
        if tenant:
            st.session_state.tenant_id = tenant["id"]
            st.session_state.conversation_state = "tenant_ready"
            
            # Check for existing connections
            connections = list_connections(tenant["id"])
            
            response = f"""✅ Found your tenant: **{tenant['name']}**

"""
            if connections:
                response += f"You have **{len(connections)}** existing connection(s):\n"
                for conn in connections:
                    response += f"- `{conn['name']}` ({conn['database_name']}.{conn['schema_name']})\n"
                response += "\nWhat would you like to do?\n"
                response += "1. **Use existing connection** - enter connection name\n"
                response += "2. **Create new connection** - type 'new connection'\n"
                response += "3. **View datasets** - type 'show datasets'"
            else:
                response += "You don't have any connections yet. Let's create one!\n\n"
                response += "I'll need your **Snowflake connection details**. "
                response += "Let's start with the **account identifier** (e.g., 'xy12345.us-east-1'):"
                st.session_state.conversation_state = "creating_connection"
                st.session_state.pending_data = {"step": "account"}
            
            return response
        else:
            return (
                f"❌ Tenant ID `{tenant_id}` not found. Would you like to:\n"
                "1. **Try again** - enter a different ID\n"
                "2. **Create new tenant** - type 'create new'"
            )
    except Exception:
        return (
            "❌ Invalid tenant ID format. Please enter a valid UUID "
            "or type 'create new' to create a new tenant."
        )


def handle_tenant_creation(user_input: str) -> str:
    """
    Handle tenant creation flow.
    
    Collects tenant name and slug, then confirms creation.
    
    Args:
        user_input: User input for the current step
        
    Returns:
        Response message for the next step or confirmation
    """
    pending = st.session_state.pending_data
    
    if "name" not in pending:
        pending["name"] = user_input.strip()
        st.session_state.pending_data = pending
        return (
            f"Got it! Organization name: **{pending['name']}**\n\n"
            "Now, please provide a **slug** (short identifier, lowercase, no spaces):"
        )
    
    elif "slug" not in pending:
        slug = user_input.strip().lower().replace(" ", "-")
        pending["slug"] = slug
        st.session_state.pending_data = pending
        st.session_state.awaiting_confirmation = True
        
        return f"""Perfect! Here's what I'll create:

📋 **New Tenant**
- **Name:** {pending['name']}
- **Slug:** {pending['slug']}

Do you want me to create this tenant? (yes/no)"""
    
    # Fallback to LLM for unexpected input
    from streamlit_app.handlers.utils import get_llm_response
    return get_llm_response(user_input)

