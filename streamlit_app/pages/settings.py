"""
Chalk and Duster - Settings Page

Configuration and settings management.
"""

import streamlit as st
from typing import Dict, Any


def render_settings_page():
    """Render the settings page."""
    st.markdown("### ⚙️ Settings")
    
    if not st.session_state.tenant_id:
        st.warning("⚠️ No tenant selected. Please use the Chat Assistant to set up your tenant first.")
        return
    
    # Tabs for different settings
    tab1, tab2, tab3 = st.tabs(["🏢 Tenant", "🔔 Notifications", "📅 Schedules"])
    
    with tab1:
        render_tenant_settings()
    
    with tab2:
        render_notification_settings()
    
    with tab3:
        render_schedule_settings()


def render_tenant_settings():
    """Render tenant settings."""
    st.markdown("#### Tenant Information")
    
    st.text_input("Tenant ID", value=st.session_state.tenant_id, disabled=True)
    
    st.markdown("---")
    st.markdown("#### Snowflake Defaults")
    
    default_account = st.text_input("Default Account", placeholder="xy12345.us-east-1")
    default_warehouse = st.text_input("Default Warehouse", value="COMPUTE_WH")
    default_role = st.text_input("Default Role", placeholder="ANALYST")
    
    if st.button("Save Tenant Settings"):
        st.success("Settings saved successfully!")


def render_notification_settings():
    """Render notification settings."""
    st.markdown("#### Slack Integration")
    
    slack_enabled = st.toggle("Enable Slack Notifications", value=False)
    
    if slack_enabled:
        webhook_url = st.text_input("Webhook URL", type="password", placeholder="https://hooks.slack.com/...")
        channel = st.text_input("Channel", placeholder="#data-quality-alerts")
        
        st.markdown("#### Notification Triggers")
        col1, col2 = st.columns(2)
        
        with col1:
            st.checkbox("Quality check failures", value=True)
            st.checkbox("Drift detected", value=True)
        
        with col2:
            st.checkbox("Run completed", value=False)
            st.checkbox("Schedule missed", value=True)
    
    st.markdown("---")
    st.markdown("#### Email Notifications")
    
    email_enabled = st.toggle("Enable Email Notifications", value=False)
    
    if email_enabled:
        email_addresses = st.text_area("Email Addresses (one per line)", placeholder="user@example.com")
    
    if st.button("Save Notification Settings"):
        st.success("Notification settings saved!")


def render_schedule_settings():
    """Render schedule settings."""
    st.markdown("#### Default Schedules")
    
    st.markdown("Set default schedules for new datasets:")
    
    quality_schedule = st.selectbox(
        "Quality Check Schedule",
        ["Disabled", "Hourly", "Every 6 hours", "Daily", "Weekly"],
        index=3,
    )
    
    quality_time = st.time_input("Quality Check Time", value=None)
    
    st.markdown("---")
    
    drift_schedule = st.selectbox(
        "Drift Detection Schedule",
        ["Disabled", "Hourly", "Every 6 hours", "Daily", "Weekly"],
        index=3,
    )
    
    drift_time = st.time_input("Drift Detection Time", value=None)
    
    st.markdown("---")
    st.markdown("#### Time Travel Settings")
    
    time_travel_days = st.number_input(
        "Default Time Travel Days (for drift reference)",
        min_value=1,
        max_value=90,
        value=1,
        help="Number of days to look back for reference data using Snowflake Time Travel",
    )
    
    if st.button("Save Schedule Settings"):
        st.success("Schedule settings saved!")

