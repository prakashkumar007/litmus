"""
Chalk and Duster - Handler Utilities

Shared utility functions for chat handlers.
"""

from datetime import datetime
from typing import Dict, Any, List

import streamlit as st

from streamlit_app.utils.llm_chat import chat_with_ollama, SYSTEM_PROMPT


def add_message(role: str, content: str) -> None:
    """
    Add a message to the conversation history.
    
    Args:
        role: Message role ('user' or 'assistant')
        content: Message content
    """
    st.session_state.messages.append({
        "role": role,
        "content": content,
        "timestamp": datetime.now().isoformat(),
    })


def get_welcome_message() -> str:
    """
    Get the initial welcome message.
    
    Returns:
        Formatted welcome message
    """
    return """👋 Welcome to **Chalk and Duster**!

I'm your AI assistant for setting up data quality monitoring and drift detection.

Let me help you get started. **Do you have an existing tenant ID?**

- If **yes**, please share your tenant ID
- If **no**, I'll help you create a new tenant"""


def get_llm_response(user_input: str) -> str:
    """
    Get a response from the LLM for general conversation.
    
    Args:
        user_input: The user's message
        
    Returns:
        LLM-generated response
    """
    # Build conversation history
    messages: List[Dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT}]
    
    # Add recent conversation history (last 10 messages)
    for msg in st.session_state.messages[-10:]:
        messages.append({
            "role": msg["role"],
            "content": msg["content"],
        })
    
    # Add current user input
    messages.append({"role": "user", "content": user_input})
    
    return chat_with_ollama(messages)


def is_uuid_format(text: str) -> bool:
    """
    Check if text looks like a UUID.
    
    Args:
        text: Text to check
        
    Returns:
        True if text appears to be a UUID
    """
    cleaned = text.replace("-", "")
    return len(cleaned) == 32 and cleaned.isalnum()

