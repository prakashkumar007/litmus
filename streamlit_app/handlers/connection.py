"""
Chalk and Duster - Connection Handlers

Handlers for Snowflake connection-related conversation states.
"""

from typing import Dict, Any, Optional

import streamlit as st

from streamlit_app.utils.database import list_connections, list_datasets
from streamlit_app.handlers.utils import get_llm_response


def handle_tenant_ready_state(user_input: str) -> str:
    """
    Handle state when tenant is ready.
    
    User can create connections, view datasets, or select existing connections.
    
    Args:
        user_input: User input
        
    Returns:
        Response message
    """
    lower_input = user_input.lower()
    
    if "new connection" in lower_input or "create connection" in lower_input:
        st.session_state.conversation_state = "creating_connection"
        st.session_state.pending_data = {"step": "account"}
        return (
            "Let's create a new Snowflake connection. "
            "Please provide your **account identifier** (e.g., 'xy12345.us-east-1'):"
        )
    
    elif any(kw in lower_input for kw in ["show datasets", "list datasets", "view datasets"]):
        return _list_datasets_response()
    
    else:
        # Check if user is selecting an existing connection
        connections = list_connections(st.session_state.tenant_id)
        for conn in connections:
            if conn["name"].lower() in lower_input:
                st.session_state.connection_id = conn["id"]
                st.session_state.conversation_state = "awaiting_ddl"
                return f"""✅ Using connection: **{conn['name']}**

Now paste your **CREATE TABLE DDL** statement, and I'll generate quality and drift rules for you:"""
        
        return get_llm_response(user_input)


def _list_datasets_response() -> str:
    """Generate response listing all datasets."""
    datasets = list_datasets(st.session_state.tenant_id)
    if datasets:
        response = f"📊 **Your Datasets ({len(datasets)})**\n\n"
        for ds in datasets:
            response += f"- **{ds['name']}** (`{ds['table_name']}`)\n"
            if ds.get('quality_schedule'):
                response += f"  Quality: {ds['quality_schedule']}\n"
            if ds.get('drift_schedule'):
                response += f"  Drift: {ds['drift_schedule']}\n"
        return response
    else:
        return (
            "You don't have any datasets yet. Would you like to create one? "
            "Paste a DDL or type 'new connection' to set up a connection first."
        )


def handle_connection_creation(user_input: str) -> str:
    """
    Handle connection creation flow.
    
    Multi-step flow collecting account, database, schema, warehouse, and name.
    
    Args:
        user_input: User input for the current step
        
    Returns:
        Response message for the next step
    """
    pending = st.session_state.pending_data
    step = pending.get("step", "account")
    
    handlers = {
        "account": _handle_account_step,
        "database": _handle_database_step,
        "schema": _handle_schema_step,
        "warehouse": _handle_warehouse_step,
        "name": _handle_name_step,
    }
    
    handler = handlers.get(step)
    if handler:
        return handler(user_input, pending)
    
    return get_llm_response(user_input)


def _handle_account_step(user_input: str, pending: Dict[str, Any]) -> str:
    """Handle account identifier step."""
    pending["account"] = user_input.strip()
    pending["step"] = "database"
    st.session_state.pending_data = pending
    return f"Account: **{pending['account']}**\n\nNow, please provide the **database name**:"


def _handle_database_step(user_input: str, pending: Dict[str, Any]) -> str:
    """Handle database name step."""
    pending["database"] = user_input.strip().upper()
    pending["step"] = "schema"
    st.session_state.pending_data = pending
    return f"Database: **{pending['database']}**\n\nPlease provide the **schema name** (or press Enter for 'PUBLIC'):"


def _handle_schema_step(user_input: str, pending: Dict[str, Any]) -> str:
    """Handle schema name step."""
    schema = user_input.strip().upper() if user_input.strip() else "PUBLIC"
    pending["schema"] = schema
    pending["step"] = "warehouse"
    st.session_state.pending_data = pending
    return f"Schema: **{pending['schema']}**\n\nPlease provide the **warehouse name** (or press Enter for 'COMPUTE_WH'):"


def _handle_warehouse_step(user_input: str, pending: Dict[str, Any]) -> str:
    """Handle warehouse name step."""
    warehouse = user_input.strip().upper() if user_input.strip() else "COMPUTE_WH"
    pending["warehouse"] = warehouse
    pending["step"] = "name"
    st.session_state.pending_data = pending
    return f"Warehouse: **{pending['warehouse']}**\n\nFinally, give this connection a **name** (e.g., 'Production Snowflake'):"


def _handle_name_step(user_input: str, pending: Dict[str, Any]) -> str:
    """Handle connection name step."""
    pending["name"] = user_input.strip()
    pending["action"] = "create_connection"
    st.session_state.pending_data = pending
    st.session_state.awaiting_confirmation = True
    
    return f"""Perfect! Here's the connection I'll create:

🔗 **New Snowflake Connection**
- **Name:** {pending['name']}
- **Account:** {pending['account']}
- **Database:** {pending['database']}
- **Schema:** {pending['schema']}
- **Warehouse:** {pending['warehouse']}

Do you want me to create this connection? (yes/no)"""

