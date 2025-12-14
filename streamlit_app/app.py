"""
Chalk and Duster - Enterprise Data Quality & Drift Monitoring Platform

Professional Streamlit UI with AI-powered chatbot for:
- Tenant onboarding and management
- Snowflake connection setup
- DDL-based quality and drift rule generation
- Manual and scheduled lambda execution
"""

import streamlit as st
import asyncio
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Chalk and Duster",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for professional look
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E3A5F;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #6B7280;
        margin-bottom: 2rem;
    }
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .user-message {
        background-color: #E5E7EB;
    }
    .assistant-message {
        background-color: #DBEAFE;
    }
    .stButton > button {
        width: 100%;
    }
    .success-box {
        padding: 1rem;
        background-color: #D1FAE5;
        border-radius: 0.5rem;
        border-left: 4px solid #10B981;
    }
    .info-box {
        padding: 1rem;
        background-color: #DBEAFE;
        border-radius: 0.5rem;
        border-left: 4px solid #3B82F6;
    }
    .warning-box {
        padding: 1rem;
        background-color: #FEF3C7;
        border-radius: 0.5rem;
        border-left: 4px solid #F59E0B;
    }
</style>
""", unsafe_allow_html=True)


def init_session_state():
    """Initialize session state variables."""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "conversation_state" not in st.session_state:
        st.session_state.conversation_state = "welcome"
    if "tenant_id" not in st.session_state:
        st.session_state.tenant_id = None
    if "connection_id" not in st.session_state:
        st.session_state.connection_id = None
    if "dataset_id" not in st.session_state:
        st.session_state.dataset_id = None
    if "pending_data" not in st.session_state:
        st.session_state.pending_data = {}
    if "awaiting_confirmation" not in st.session_state:
        st.session_state.awaiting_confirmation = False


def main():
    """Main application entry point."""
    init_session_state()
    
    # Header
    st.markdown('<p class="main-header">🎓 Chalk and Duster</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">AI-Powered Data Quality & Drift Monitoring Platform</p>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 📊 Navigation")
        page = st.radio(
            "Select Page",
            ["💬 Chat Assistant", "📈 Dashboard", "⚙️ Settings"],
            label_visibility="collapsed"
        )
        
        st.divider()
        
        # Session info
        if st.session_state.tenant_id:
            st.markdown("### 🏢 Current Session")
            st.markdown(f"**Tenant ID:** `{st.session_state.tenant_id[:8]}...`")
            if st.session_state.connection_id:
                st.markdown(f"**Connection:** `{st.session_state.connection_id[:8]}...`")
            if st.session_state.dataset_id:
                st.markdown(f"**Dataset:** `{st.session_state.dataset_id[:8]}...`")
        
        st.divider()
        
        # Reset button
        if st.button("🔄 Reset Conversation", use_container_width=True):
            st.session_state.messages = []
            st.session_state.conversation_state = "welcome"
            st.session_state.pending_data = {}
            st.session_state.awaiting_confirmation = False
            st.rerun()
    
    # Main content based on page
    if page == "💬 Chat Assistant":
        from streamlit_app.pages.chat import render_chat_page
        render_chat_page()
    elif page == "📈 Dashboard":
        from streamlit_app.pages.dashboard import render_dashboard_page
        render_dashboard_page()
    elif page == "⚙️ Settings":
        from streamlit_app.pages.settings import render_settings_page
        render_settings_page()


if __name__ == "__main__":
    main()

