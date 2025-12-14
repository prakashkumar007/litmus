"""
Chalk and Duster - Chat Assistant Page

AI-powered chatbot for tenant onboarding, connection setup, and rule generation.
"""

import streamlit as st
from datetime import datetime
from typing import Dict, Any, Optional

from streamlit_app.utils.llm_chat import (
    chat_with_ollama,
    parse_ddl,
    extract_yaml_from_response,
    generate_quality_rules,
    generate_drift_rules,
    SYSTEM_PROMPT,
)
from streamlit_app.utils.database import (
    get_tenant_by_id,
    create_tenant,
    list_connections,
    create_connection,
    list_datasets,
    create_dataset,
    update_dataset,
    get_dataset_by_id,
    get_connection_by_id,
    create_run,
    update_run,
)


def add_message(role: str, content: str):
    """Add a message to the conversation history."""
    st.session_state.messages.append({
        "role": role,
        "content": content,
        "timestamp": datetime.now().isoformat(),
    })


def get_welcome_message() -> str:
    """Get the initial welcome message."""
    return """👋 Welcome to **Chalk and Duster**!

I'm your AI assistant for setting up data quality monitoring and drift detection.

Let me help you get started. **Do you have an existing tenant ID?**

- If **yes**, please share your tenant ID
- If **no**, I'll help you create a new tenant"""


def process_user_input(user_input: str) -> str:
    """Process user input and generate appropriate response."""
    state = st.session_state.conversation_state
    pending = st.session_state.pending_data
    
    # Handle confirmation responses
    if st.session_state.awaiting_confirmation:
        if user_input.lower() in ["yes", "y", "confirm", "ok", "sure", "proceed"]:
            return handle_confirmation()
        elif user_input.lower() in ["no", "n", "cancel", "abort"]:
            st.session_state.awaiting_confirmation = False
            st.session_state.pending_data = {}
            return "No problem! Let me know what you'd like to change or if you want to start over."
    
    # State machine for conversation flow
    if state == "welcome":
        return handle_welcome_state(user_input)
    elif state == "awaiting_tenant_id":
        return handle_tenant_id_input(user_input)
    elif state == "creating_tenant":
        return handle_tenant_creation(user_input)
    elif state == "tenant_ready":
        return handle_tenant_ready_state(user_input)
    elif state == "creating_connection":
        return handle_connection_creation(user_input)
    elif state == "connection_ready":
        return handle_connection_ready_state(user_input)
    elif state == "awaiting_ddl":
        return handle_ddl_input(user_input)
    elif state == "reviewing_yaml":
        return handle_yaml_review(user_input)
    else:
        # Use LLM for general conversation
        return get_llm_response(user_input)


def handle_welcome_state(user_input: str) -> str:
    """Handle the initial welcome state."""
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
    """Handle tenant ID input and validation."""
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
                response += "I'll need your **Snowflake connection details**. Let's start with the **account identifier** (e.g., 'xy12345.us-east-1'):"
                st.session_state.conversation_state = "creating_connection"
                st.session_state.pending_data = {"step": "account"}
            
            return response
        else:
            return f"❌ Tenant ID `{tenant_id}` not found. Would you like to:\n1. **Try again** - enter a different ID\n2. **Create new tenant** - type 'create new'"
    except Exception as e:
        return f"❌ Invalid tenant ID format. Please enter a valid UUID or type 'create new' to create a new tenant."


def handle_tenant_creation(user_input: str) -> str:
    """Handle tenant creation flow."""
    pending = st.session_state.pending_data
    
    if "name" not in pending:
        pending["name"] = user_input.strip()
        st.session_state.pending_data = pending
        return f"Got it! Organization name: **{pending['name']}**\n\nNow, please provide a **slug** (short identifier, lowercase, no spaces):"
    
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

    return get_llm_response(user_input)


def handle_confirmation() -> str:
    """Handle confirmation of pending actions."""
    pending = st.session_state.pending_data
    action = pending.get("action", "create_tenant")

    st.session_state.awaiting_confirmation = False

    if action == "create_tenant" or ("name" in pending and "slug" in pending and "account" not in pending):
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

    elif action == "create_connection":
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

    elif action == "create_dataset":
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

    return "Action completed."


def handle_tenant_ready_state(user_input: str) -> str:
    """Handle state when tenant is ready."""
    lower_input = user_input.lower()

    if "new connection" in lower_input or "create connection" in lower_input:
        st.session_state.conversation_state = "creating_connection"
        st.session_state.pending_data = {"step": "account"}
        return "Let's create a new Snowflake connection. Please provide your **account identifier** (e.g., 'xy12345.us-east-1'):"

    elif "show datasets" in lower_input or "list datasets" in lower_input or "view datasets" in lower_input:
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
            return "You don't have any datasets yet. Would you like to create one? Paste a DDL or type 'new connection' to set up a connection first."

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


def handle_connection_creation(user_input: str) -> str:
    """Handle connection creation flow."""
    pending = st.session_state.pending_data
    step = pending.get("step", "account")

    if step == "account":
        pending["account"] = user_input.strip()
        pending["step"] = "database"
        st.session_state.pending_data = pending
        return f"Account: **{pending['account']}**\n\nNow, please provide the **database name**:"

    elif step == "database":
        pending["database"] = user_input.strip().upper()
        pending["step"] = "schema"
        st.session_state.pending_data = pending
        return f"Database: **{pending['database']}**\n\nPlease provide the **schema name** (or press Enter for 'PUBLIC'):"

    elif step == "schema":
        schema = user_input.strip().upper() if user_input.strip() else "PUBLIC"
        pending["schema"] = schema
        pending["step"] = "warehouse"
        st.session_state.pending_data = pending
        return f"Schema: **{pending['schema']}**\n\nPlease provide the **warehouse name** (or press Enter for 'COMPUTE_WH'):"

    elif step == "warehouse":
        warehouse = user_input.strip().upper() if user_input.strip() else "COMPUTE_WH"
        pending["warehouse"] = warehouse
        pending["step"] = "name"
        st.session_state.pending_data = pending
        return f"Warehouse: **{pending['warehouse']}**\n\nFinally, give this connection a **name** (e.g., 'Production Snowflake'):"

    elif step == "name":
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

    return get_llm_response(user_input)


def handle_connection_ready_state(user_input: str) -> str:
    """Handle state when connection is ready."""
    lower_input = user_input.lower()

    if "run quality" in lower_input:
        return trigger_quality_check()
    elif "run drift" in lower_input:
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
    """Handle DDL input and generate quality/drift rules."""
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
    columns_desc_lines = []
    for col in parsed["columns"]:
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
        columns_desc_lines.append(f"- **{col['name']}**: `{col['type']}`{constraint_str}")

    columns_desc = "\n".join(columns_desc_lines)

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


def handle_yaml_review(user_input: str) -> str:
    """Handle YAML review and modification."""
    lower_input = user_input.lower()

    if lower_input in ["yes", "y", "save", "confirm"]:
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
        return "What would you like to modify? You can:\n1. Edit quality rules\n2. Edit drift rules\n3. Change thresholds\n\nDescribe your changes:"
    else:
        # Use LLM to modify based on user input
        return get_llm_response(user_input)


def trigger_quality_check() -> str:
    """Trigger a quality check for the current dataset using Great Expectations."""
    import asyncio
    import os
    from datetime import datetime
    from uuid import UUID

    if not st.session_state.dataset_id:
        return "❌ No dataset selected. Please create or select a dataset first."

    dataset_id = st.session_state.dataset_id
    tenant_id = st.session_state.tenant_id

    # Get dataset details
    dataset = get_dataset_by_id(dataset_id)
    if not dataset:
        return "❌ Dataset not found. Please select a valid dataset."

    quality_yaml = dataset.get("quality_yaml")
    if not quality_yaml:
        return "❌ No quality rules configured for this dataset. Please add quality rules first."

    # Get connection config
    connection_id = dataset.get("connection_id")
    if not connection_id:
        return "❌ No connection configured for this dataset."

    connection = get_connection_by_id(connection_id)
    if not connection:
        return "❌ Connection not found."

    # Create run record
    run = create_run(
        dataset_id=dataset_id,
        tenant_id=tenant_id,
        run_type="quality",
        trigger_type="on_demand",
        status="running",
    )
    run_id = run["id"]

    # Execute quality checks using Great Expectations
    try:
        from chalkandduster.quality.great_expectations_executor import GreatExpectationsExecutor

        # Build connection config for executor
        connection_config = {
            "account": connection.get("account", ""),
            "user": connection.get("username", ""),
            "password": connection.get("password", ""),
            "database": connection.get("database", ""),
            "schema": connection.get("schema", "PUBLIC"),
            "warehouse": connection.get("warehouse", ""),
        }

        # Get table name from dataset
        table_name = dataset.get("name", "unknown_table")

        # Create executor and run
        executor = GreatExpectationsExecutor(connection_config=connection_config)

        # Run async executor in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                executor.execute(
                    dataset_id=UUID(dataset_id),
                    quality_yaml=quality_yaml,
                    table_name=table_name,
                )
            )
        finally:
            loop.close()

        # Count results
        total_checks = len(result.results)
        passed_checks = sum(1 for r in result.results if r.status == "passed")
        failed_checks = sum(1 for r in result.results if r.status == "failed")
        error_checks = sum(1 for r in result.results if r.status == "error")

        # Save HTML report to disk
        report_path = None
        if result.html_report:
            reports_dir = "/app/great_expectations"
            os.makedirs(reports_dir, exist_ok=True)
            report_filename = f"quality_report_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            report_path = os.path.join(reports_dir, report_filename)
            with open(report_path, "w") as f:
                f.write(result.html_report)

        # Update run with results
        update_run(
            run_id=run_id,
            status="completed",
            total_checks=total_checks,
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            error_checks=error_checks,
            results_summary=f"Completed {total_checks} checks: {passed_checks} passed, {failed_checks} failed.",
        )

        report_info = f"\n\n📄 **Report saved to:** `{report_path}`" if report_path else ""

        return f"""✅ **Quality Check Completed (Great Expectations)**

**Dataset:** {dataset['name']}
**Run ID:** `{run_id}`

### Results Summary
| Metric | Value |
|--------|-------|
| Total Checks | {total_checks} |
| ✅ Passed | {passed_checks} |
| ❌ Failed | {failed_checks} |
| ⚠️ Errors | {error_checks} |
{report_info}

View detailed results in the **Dashboard** → **Recent Runs** tab."""

    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        # Update run as failed
        update_run(
            run_id=run_id,
            status="failed",
            total_checks=0,
            passed_checks=0,
            failed_checks=0,
            error_checks=1,
            results_summary=str(e),
        )
        return f"❌ Quality check failed: {str(e)}"


def trigger_drift_check() -> str:
    """Trigger a drift check for the current dataset using Evidently."""
    import asyncio
    import os
    from datetime import datetime
    from uuid import UUID

    if not st.session_state.dataset_id:
        return "❌ No dataset selected. Please create or select a dataset first."

    dataset_id = st.session_state.dataset_id
    tenant_id = st.session_state.tenant_id

    # Get dataset details
    dataset = get_dataset_by_id(dataset_id)
    if not dataset:
        return "❌ Dataset not found. Please select a valid dataset."

    drift_yaml = dataset.get("drift_yaml")
    if not drift_yaml:
        return "❌ No drift rules configured for this dataset. Please add drift rules first."

    # Get connection config
    connection_id = dataset.get("connection_id")
    if not connection_id:
        return "❌ No connection configured for this dataset."

    connection = get_connection_by_id(connection_id)
    if not connection:
        return "❌ Connection not found."

    # Create run record
    run = create_run(
        dataset_id=dataset_id,
        tenant_id=tenant_id,
        run_type="drift",
        trigger_type="on_demand",
        status="running",
    )
    run_id = run["id"]

    # Execute drift detection using Evidently
    try:
        from chalkandduster.drift.evidently_detector import EvidentlyDriftDetector

        # Build connection config for detector
        connection_config = {
            "account": connection.get("account", ""),
            "user": connection.get("username", ""),
            "password": connection.get("password", ""),
            "database": connection.get("database", ""),
            "schema": connection.get("schema", "PUBLIC"),
            "warehouse": connection.get("warehouse", ""),
        }

        # Get table name from dataset
        table_name = dataset.get("name", "unknown_table")
        database = connection.get("database", "")
        schema = connection.get("schema", "PUBLIC")

        # Create detector and run
        detector = EvidentlyDriftDetector(connection_config=connection_config)

        # Run async detector in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                detector.detect(
                    dataset_id=UUID(dataset_id),
                    drift_yaml=drift_yaml,
                    table_name=table_name,
                    database=database,
                    schema=schema,
                )
            )
        finally:
            loop.close()

        # Count results
        total_monitors = len(result.results)
        drift_detected = sum(1 for r in result.results if r.detected)
        no_drift = total_monitors - drift_detected

        # Save HTML report to disk
        report_path = None
        if result.html_report:
            reports_dir = "/app/evidently_reports"
            os.makedirs(reports_dir, exist_ok=True)
            report_filename = f"drift_report_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            report_path = os.path.join(reports_dir, report_filename)
            with open(report_path, "w") as f:
                f.write(result.html_report)

        # Update run with results
        status = "completed" if drift_detected == 0 else "warning"
        update_run(
            run_id=run_id,
            status=status,
            total_checks=total_monitors,
            passed_checks=no_drift,
            failed_checks=drift_detected,
            error_checks=0,
            results_summary=f"Analyzed {total_monitors} monitors. {drift_detected} drift detected.",
        )

        report_info = f"\n\n📄 **Report saved to:** `{report_path}`" if report_path else ""

        return f"""✅ **Drift Detection Completed (Evidently)**

**Dataset:** {dataset['name']}
**Run ID:** `{run_id}`

### Results Summary
| Metric | Value |
|--------|-------|
| Total Monitors | {total_monitors} |
| 🟢 No Drift | {no_drift} |
| 🔴 Drift Detected | {drift_detected} |
{report_info}

View detailed results in the **Dashboard** → **Recent Runs** tab."""

    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        # Update run as failed
        update_run(
            run_id=run_id,
            status="failed",
            total_checks=0,
            passed_checks=0,
            failed_checks=0,
            error_checks=1,
            results_summary=str(e),
        )
        return f"❌ Drift detection failed: {str(e)}"


def get_llm_response(user_input: str) -> str:
    """Get a response from the LLM for general conversation."""
    # Build conversation history
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    # Add recent conversation history
    for msg in st.session_state.messages[-10:]:
        messages.append({
            "role": msg["role"],
            "content": msg["content"],
        })

    # Add current user input
    messages.append({"role": "user", "content": user_input})

    return chat_with_ollama(messages)


def render_chat_page():
    """Render the chat assistant page."""
    st.markdown("### 💬 Chat Assistant")

    # Display chat messages
    chat_container = st.container()

    with chat_container:
        # Show welcome message if no messages
        if not st.session_state.messages:
            welcome = get_welcome_message()
            add_message("assistant", welcome)

        # Display all messages
        for message in st.session_state.messages:
            role = message["role"]
            content = message["content"]

            if role == "user":
                with st.chat_message("user", avatar="👤"):
                    st.markdown(content)
            else:
                with st.chat_message("assistant", avatar="🎓"):
                    st.markdown(content)

    # Chat input
    if prompt := st.chat_input("Type your message..."):
        # Add user message
        add_message("user", prompt)

        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)

        # Process and get response
        with st.chat_message("assistant", avatar="🎓"):
            with st.spinner("Thinking..."):
                response = process_user_input(prompt)
            st.markdown(response)

        # Add assistant response
        add_message("assistant", response)

        st.rerun()

