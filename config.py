"""
Configuration Management
Handles DhanHQ API credentials and settings
Supports both .env files (local) and Streamlit secrets (cloud)
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Try to import streamlit for secrets support
try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False


def _get_secret(key: str) -> str:
    """
    Get secret from Streamlit secrets or environment variables
    
    Args:
        key: Secret key name
        
    Returns:
        Secret value or None
    """
    # Try Streamlit secrets first (for cloud deployment)
    if HAS_STREAMLIT:
        try:
            return st.secrets.get(key)
        except (FileNotFoundError, KeyError):
            pass
    
    # Fall back to environment variables (for local deployment)
    return os.getenv(key)


def get_dhan_credentials():
    """
    Get DhanHQ API credentials from Streamlit secrets or environment variables
    
    Returns:
        tuple: (client_id, access_token)
    """
    client_id = _get_secret('DHAN_CLIENT_ID')
    access_token = _get_secret('DHAN_ACCESS_TOKEN')
    
    if not client_id or not access_token:
        raise ValueError(
            "DhanHQ credentials not found. "
            "Please set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in:\n"
            "- .env file (for local development)\n"
            "- .streamlit/secrets.toml (for Streamlit Cloud)"
        )
    
    return client_id, access_token


def get_telegram_credentials():
    """
    Get Telegram Bot credentials from Streamlit secrets or environment variables
    
    Returns:
        tuple: (bot_token, chat_id)
    """
    bot_token = _get_secret('TELEGRAM_BOT_TOKEN')
    chat_id = _get_secret('TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        raise ValueError(
            "Telegram credentials not found. "
            "Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in:\n"
            "- .env file (for local development)\n"
            "- .streamlit/secrets.toml (for Streamlit Cloud)"
        )
    
    return bot_token, chat_id
