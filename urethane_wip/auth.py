from __future__ import annotations

import streamlit as st

from urethane_wip.config import CONFIG


def init_auth_state() -> None:
    """Prepare Streamlit session keys used by the lightweight admin auth."""
    st.session_state.setdefault(CONFIG.admin_session_key, False)
    st.session_state.setdefault(CONFIG.admin_name_key, "")


def is_authenticated() -> bool:
    return bool(st.session_state.get(CONFIG.admin_session_key, False))


def current_admin_name() -> str:
    return str(st.session_state.get(CONFIG.admin_name_key, "")).strip()


def login(password: str, admin_name: str) -> bool:
    """Validate password from configuration and persist session state."""
    if CONFIG.admin_password and password == CONFIG.admin_password:
        st.session_state[CONFIG.admin_session_key] = True
        st.session_state[CONFIG.admin_name_key] = admin_name.strip() or "관리자"
        return True
    return False


def logout() -> None:
    st.session_state[CONFIG.admin_session_key] = False
    st.session_state[CONFIG.admin_name_key] = ""
