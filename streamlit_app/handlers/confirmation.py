"""
Chalk and Duster - Confirmation Handler

Handler for confirming pending actions (tenant, connection, dataset creation).
"""

from typing import Dict, Any

import streamlit as st

from streamlit_app.utils.database import (
    create_tenant,
    create_connection,
    create_dataset,
)


def handle_confirmation() -> str:
    """
    Handle confirmation of pending actions.
    
    Processes the pending action based on the action type stored in session state.
    
    Returns:
        Response message with result of the action
    """
    pending = st.session_state.pending_data
    action = pending.get("action", "create_tenant")
    
    st.session_state.awaiting_confirmation = False
    
    # Determine action type
    if action == "create_tenant" or _is_tenant_creation(pending):
        return _create_tenant_action(pending)
    elif action == "create_connection":
        return _create_connection_action(pending)
    elif action == "create_dataset":
        return _create_dataset_action(pending)
    
    return "Action completed."


def _is_tenant_creation(pending: Dict[str, Any]) -> bool:
    """Check if pending data represents tenant creation."""
    return "name" in pending and "slug" in pending and "account" not in pending


def _create_tenant_action(pending: Dict[str, Any]) -> str:
    """Handle tenant creation action."""
    try:
        tenant = create_tenant(
            name=pending["name"],
            slug=pending["slug"],
        )
        st.session_state.tenant_id = tenant["id"]
        st.session_state.conversation_state = "creating_connection"
        st.session_state.pending_data = {"step": "account"}
        
        return f"""✅ Tenant created successfully!

**Tenant ID:** `{tenant['id']}`
**Name:** {tenant['name']}

Now let's set up your Snowflake connection. Please provide your **Snowflake account identifier** (e.g., 'xy12345.us-east-1'):"""
    except Exception as e:
        return f"❌ Failed to create tenant: {str(e)}\n\nPlease try again or contact support."


def _create_connection_action(pending: Dict[str, Any]) -> str:
    """Handle connection creation action."""
    try:
        connection = create_connection(
            tenant_id=st.session_state.tenant_id,
            name=pending["name"],
            account=pending["account"],
            database_name=pending["database"],
            schema_name=pending.get("schema", "PUBLIC"),
            warehouse=pending.get("warehouse", "COMPUTE_WH"),
        )
        st.session_state.connection_id = connection["id"]
        st.session_state.conversation_state = "awaiting_ddl"
        st.session_state.pending_data = {}
        
        return f"""✅ Connection created successfully!

**Connection ID:** `{connection['id']}`
**Name:** {connection['name']}

Now let's set up data quality and drift monitoring. Please paste your **CREATE TABLE DDL** statement, and I'll generate appropriate quality and drift rules for you:"""
    except Exception as e:
        return f"❌ Failed to create connection: {str(e)}\n\nPlease try again."


def _create_dataset_action(pending: Dict[str, Any]) -> str:
    """Handle dataset creation action."""
    try:
        dataset = create_dataset(
            tenant_id=st.session_state.tenant_id,
            connection_id=st.session_state.connection_id,
            name=pending["name"],
            database_name=pending["database"],
            schema_name=pending["schema"],
            table_name=pending["table"],
            quality_yaml=pending.get("quality_yaml"),
            drift_yaml=pending.get("drift_yaml"),
        )
        st.session_state.dataset_id = dataset["id"]
        st.session_state.conversation_state = "connection_ready"
        st.session_state.pending_data = {}
        
        return f"""✅ Dataset created successfully!

**Dataset ID:** `{dataset['id']}`
**Table:** {pending['database']}.{pending['schema']}.{pending['table']}

Your quality and drift rules are now configured. What would you like to do next?
1. **Run quality check** - type 'run quality'
2. **Run drift detection** - type 'run drift'
3. **Add another table** - paste another DDL
4. **Set up schedule** - type 'schedule'"""
    except Exception as e:
        return f"❌ Failed to create dataset: {str(e)}\n\nPlease try again."

