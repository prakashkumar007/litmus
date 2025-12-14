"""
Chalk and Duster - Dataset Handlers

Handlers for dataset-related conversation states including DDL parsing and YAML review.
"""

from typing import Dict, Any, List

import streamlit as st

from streamlit_app.utils.database import list_datasets
from streamlit_app.utils.llm_chat import parse_ddl, generate_quality_rules, generate_drift_rules
from streamlit_app.handlers.utils import get_llm_response


def handle_connection_ready_state(user_input: str) -> str:
    """
    Handle state when connection is ready.
    
    User can run quality/drift checks, schedule, or add DDL.
    
    Args:
        user_input: User input
        
    Returns:
        Response message
    """
    lower_input = user_input.lower()
    
    if "run quality" in lower_input:
        from streamlit_app.handlers.quality import trigger_quality_check
        return trigger_quality_check()
    
    elif "run drift" in lower_input:
        from streamlit_app.handlers.drift import trigger_drift_check
        return trigger_drift_check()
    
    elif "schedule" in lower_input:
        st.session_state.conversation_state = "scheduling"
        return """Let's set up a schedule. What would you like to schedule?

1. **Quality checks** - type 'schedule quality'
2. **Drift detection** - type 'schedule drift'
3. **Both** - type 'schedule both'

Please also specify the frequency (e.g., 'daily at 9am', 'hourly', 'every 6 hours'):"""
    
    elif "create table" in lower_input or "ddl" in lower_input:
        st.session_state.conversation_state = "awaiting_ddl"
        return "Please paste your **CREATE TABLE DDL** statement:"
    
    else:
        # Check if it looks like DDL
        if "CREATE" in user_input.upper() and "TABLE" in user_input.upper():
            return handle_ddl_input(user_input)
        return get_llm_response(user_input)


def handle_ddl_input(user_input: str) -> str:
    """
    Handle DDL input and generate quality/drift rules.
    
    Parses the DDL and generates YAML configurations for quality and drift monitoring.
    
    Args:
        user_input: DDL statement from user
        
    Returns:
        Response with parsed table info and generated rules
    """
    # Parse the DDL
    parsed = parse_ddl(user_input)
    
    if not parsed["table_name"]:
        return "❌ I couldn't parse the DDL. Please make sure it's a valid CREATE TABLE statement."
    
    # Store parsed info
    st.session_state.pending_data = {
        "action": "create_dataset",
        "table": parsed["table_name"],
        "database": parsed.get("database") or "DATABASE",
        "schema": parsed.get("schema") or "PUBLIC",
        "columns": parsed["columns"],
        "ddl": user_input,
    }
    
    # Generate rules programmatically (no LLM hallucination)
    quality_yaml = generate_quality_rules(parsed)
    drift_yaml = generate_drift_rules(parsed)
    
    st.session_state.pending_data["quality_yaml"] = quality_yaml
    st.session_state.pending_data["drift_yaml"] = drift_yaml
    st.session_state.conversation_state = "reviewing_yaml"
    
    # Build detailed column description with constraints
    columns_desc = _format_columns_description(parsed["columns"])
    
    # Format primary keys
    pk_info = ""
    if parsed.get("primary_keys"):
        pk_info = f"\n\n🔑 **Primary Key(s):** {', '.join(parsed['primary_keys'])}"
    
    return f"""📊 **Parsed Table: `{parsed['table_name']}`**
{pk_info}

**Columns Detected:**
{columns_desc}

---

### 📋 Generated Quality Rules (Great Expectations)

```yaml
{quality_yaml}
```

---

### 📈 Generated Drift Monitoring Rules (Evidently)

```yaml
{drift_yaml}
```

---

Would you like me to save these rules? (yes/no)
Or type 'modify' to make changes."""


def _format_columns_description(columns: List[Dict[str, Any]]) -> str:
    """Format column descriptions with constraints."""
    lines = []
    for col in columns:
        constraints = []
        if col.get("primary_key"):
            constraints.append("🔑 PRIMARY KEY")
        if not col.get("nullable"):
            constraints.append("⚠️ NOT NULL")
        if col.get("unique"):
            constraints.append("🔒 UNIQUE")
        if col.get("enum_values"):
            constraints.append(f"📋 ENUM: {col['enum_values']}")
        if col.get("default"):
            constraints.append(f"📌 DEFAULT: {col['default']}")
        
        constraint_str = f" [{', '.join(constraints)}]" if constraints else ""
        lines.append(f"- **{col['name']}**: `{col['type']}`{constraint_str}")
    
    return "\n".join(lines)


def handle_yaml_review(user_input: str) -> str:
    """
    Handle YAML review and modification.
    
    Args:
        user_input: User's response to the YAML review
        
    Returns:
        Response message
    """
    lower_input = user_input.lower()
    
    if lower_input in ["yes", "y", "save", "confirm"]:
        from streamlit_app.handlers.confirmation import handle_confirmation
        pending = st.session_state.pending_data
        pending["name"] = f"{pending['table']}_monitoring"
        st.session_state.awaiting_confirmation = True
        pending["action"] = "create_dataset"
        return handle_confirmation()
    
    elif lower_input in ["no", "n", "cancel"]:
        st.session_state.pending_data = {}
        st.session_state.conversation_state = "awaiting_ddl"
        return "No problem! Paste another DDL or let me know what you'd like to do."
    
    elif "modify" in lower_input:
        return (
            "What would you like to modify? You can:\n"
            "1. Edit quality rules\n"
            "2. Edit drift rules\n"
            "3. Change thresholds\n\n"
            "Describe your changes:"
        )
    
    else:
        return get_llm_response(user_input)

