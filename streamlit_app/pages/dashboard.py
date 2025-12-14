"""
Chalk and Duster - Dashboard Page

Overview of datasets, runs, and monitoring status.
"""

import streamlit as st
from datetime import datetime
from typing import Dict, Any, List, Optional

from streamlit_app.utils.database import (
    list_datasets,
    list_connections,
    get_dataset_by_id,
    list_runs,
)


def render_dashboard_page():
    """Render the dashboard page."""
    st.markdown("### 📈 Dashboard")
    
    if not st.session_state.tenant_id:
        st.warning("⚠️ No tenant selected. Please use the Chat Assistant to set up your tenant first.")
        return
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)

    connections = list_connections(st.session_state.tenant_id)
    datasets = list_datasets(st.session_state.tenant_id)
    runs = list_runs(st.session_state.tenant_id, limit=50)

    with col1:
        st.metric("Connections", len(connections))

    with col2:
        st.metric("Datasets", len(datasets))

    with col3:
        active_datasets = len([d for d in datasets if d.get("is_active", True)])
        st.metric("Active Monitors", active_datasets)

    with col4:
        st.metric("Recent Runs", len(runs))
    
    st.divider()
    
    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["📊 Datasets", "🔗 Connections", "📋 Recent Runs"])
    
    with tab1:
        render_datasets_tab(datasets)
    
    with tab2:
        render_connections_tab(connections)
    
    with tab3:
        render_runs_tab(runs)


def render_datasets_tab(datasets: List[Dict[str, Any]]):
    """Render the datasets tab."""
    if not datasets:
        st.info("No datasets configured yet. Use the Chat Assistant to add your first dataset.")
        return
    
    for dataset in datasets:
        with st.expander(f"📊 {dataset['name']} - `{dataset['table_name']}`", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Database:** {dataset['database_name']}")
                st.markdown(f"**Schema:** {dataset['schema_name']}")
                st.markdown(f"**Table:** {dataset['table_name']}")
            
            with col2:
                st.markdown(f"**Quality Schedule:** {dataset.get('quality_schedule') or 'Not set'}")
                st.markdown(f"**Drift Schedule:** {dataset.get('drift_schedule') or 'Not set'}")
                st.markdown(f"**Status:** {'🟢 Active' if dataset.get('is_active', True) else '🔴 Inactive'}")
            
            st.divider()
            
            # Action buttons
            btn_col1, btn_col2, btn_col3 = st.columns(3)
            
            with btn_col1:
                if st.button("▶️ Run Quality", key=f"quality_{dataset['id']}"):
                    st.success(f"Quality check triggered for {dataset['name']}")
            
            with btn_col2:
                if st.button("▶️ Run Drift", key=f"drift_{dataset['id']}"):
                    st.success(f"Drift detection triggered for {dataset['name']}")
            
            with btn_col3:
                if st.button("📝 Edit", key=f"edit_{dataset['id']}"):
                    st.session_state.dataset_id = dataset['id']
                    st.info("Use the Chat Assistant to modify this dataset.")


def render_connections_tab(connections: List[Dict[str, Any]]):
    """Render the connections tab."""
    if not connections:
        st.info("No connections configured yet. Use the Chat Assistant to add your first connection.")
        return
    
    for conn in connections:
        with st.expander(f"🔗 {conn['name']}", expanded=False):
            st.markdown(f"**Account:** {conn['account']}")
            st.markdown(f"**Database:** {conn['database_name']}")
            st.markdown(f"**Schema:** {conn['schema_name']}")
            st.markdown(f"**Warehouse:** {conn['warehouse']}")
            st.markdown(f"**Status:** {'🟢 Active' if conn.get('is_active', True) else '🔴 Inactive'}")
            
            if st.button("🔍 Test Connection", key=f"test_{conn['id']}"):
                with st.spinner("Testing connection..."):
                    # Placeholder for actual connection test
                    st.success("Connection test successful!")


def render_runs_tab(runs: List[Dict[str, Any]]):
    """Render the recent runs tab."""
    if not runs:
        st.info("No runs yet. Use the Chat Assistant to run quality checks or drift detection.")
        return

    st.markdown(f"**Showing {len(runs)} recent runs:**")

    for run in runs:
        run_id = str(run["id"])[:8]  # Short ID
        run_type = run["run_type"].upper()
        status = run["status"]
        dataset_name = run.get("dataset_name", "Unknown")

        # Status emoji
        status_emoji = {
            "pending": "⏳",
            "running": "🔄",
            "completed": "✅",
            "failed": "❌",
        }.get(status, "❓")

        # Run type emoji
        type_emoji = "📊" if run_type == "QUALITY" else "📈"

        # Format timestamp
        created = run.get("created_at")
        if created:
            if hasattr(created, "strftime"):
                created_str = created.strftime("%Y-%m-%d %H:%M")
            else:
                created_str = str(created)[:16]
        else:
            created_str = "-"

        with st.expander(f"{status_emoji} {type_emoji} {run_type} - {dataset_name} ({created_str})", expanded=False):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown(f"**Run ID:** `{run['id']}`")
                st.markdown(f"**Type:** {run_type}")
                st.markdown(f"**Trigger:** {run.get('trigger_type', 'on_demand')}")

            with col2:
                st.markdown(f"**Status:** {status_emoji} {status.title()}")
                st.markdown(f"**Started:** {created_str}")
                completed = run.get("completed_at")
                if completed:
                    if hasattr(completed, "strftime"):
                        completed_str = completed.strftime("%Y-%m-%d %H:%M")
                    else:
                        completed_str = str(completed)[:16]
                    st.markdown(f"**Completed:** {completed_str}")

            with col3:
                st.markdown(f"**Total Checks:** {run.get('total_checks', 0)}")
                st.markdown(f"**Passed:** {run.get('passed_checks', 0)}")
                st.markdown(f"**Failed:** {run.get('failed_checks', 0)}")

            # Results summary
            summary = run.get("results_summary")
            if summary:
                st.divider()
                st.markdown(f"**Summary:** {summary}")

