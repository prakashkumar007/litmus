"""
Chalk and Duster - Chat Assistant Page

AI-powered chatbot for tenant onboarding, connection setup, and rule generation.

This module provides the main chat interface using modular handlers for different
conversation states. The conversation flow is managed through a state machine.
"""

import streamlit as st
from typing import Dict, Any

from streamlit_app.handlers import (
    # Tenant handlers
    handle_welcome_state,
    handle_tenant_id_input,
    handle_tenant_creation,
    # Connection handlers
    handle_tenant_ready_state,
    handle_connection_creation,
    # Dataset handlers
    handle_connection_ready_state,
    handle_ddl_input,
    handle_yaml_review,
    # Confirmation handler
    handle_confirmation,
    # Utilities
    add_message,
    get_welcome_message,
    get_llm_response,
)


# State handler mapping for cleaner dispatch
STATE_HANDLERS: Dict[str, Any] = {
    "welcome": handle_welcome_state,
    "awaiting_tenant_id": handle_tenant_id_input,
    "creating_tenant": handle_tenant_creation,
    "tenant_ready": handle_tenant_ready_state,
    "creating_connection": handle_connection_creation,
    "connection_ready": handle_connection_ready_state,
    "awaiting_ddl": handle_ddl_input,
    "reviewing_yaml": handle_yaml_review,
}


def process_user_input(user_input: str) -> str:
    """
    Process user input and generate appropriate response.

    Uses a state machine to route input to the appropriate handler.

    Args:
        user_input: The user's message

    Returns:
        Response message from the appropriate handler
    """
    # Handle confirmation responses first
    if st.session_state.awaiting_confirmation:
        return _handle_confirmation_response(user_input)

    # Get current state and dispatch to handler
    state = st.session_state.conversation_state
    handler = STATE_HANDLERS.get(state)

    if handler:
        return handler(user_input)

    # Fallback to LLM for unknown states
    return get_llm_response(user_input)


def _handle_confirmation_response(user_input: str) -> str:
    """
    Handle yes/no confirmation responses.

    Args:
        user_input: User's confirmation response

    Returns:
        Response message
    """
    lower_input = user_input.lower()

    if lower_input in ["yes", "y", "confirm", "ok", "sure", "proceed"]:
        return handle_confirmation()
    elif lower_input in ["no", "n", "cancel", "abort"]:
        st.session_state.awaiting_confirmation = False
        st.session_state.pending_data = {}
        return "No problem! Let me know what you'd like to change or if you want to start over."

    # If not a clear yes/no, still process through confirmation
    return handle_confirmation()


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

