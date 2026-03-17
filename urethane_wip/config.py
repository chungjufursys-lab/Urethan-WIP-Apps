from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _read_setting(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value:
        return value
    try:
        import streamlit as st

        secret_value = st.secrets.get(name)
        if secret_value:
            return str(secret_value)
    except Exception:
        pass
    return default


@dataclass(frozen=True)
class AppConfig:
    """Application settings loaded from environment variables or Streamlit secrets."""

    base_dir: Path = BASE_DIR
    db_path: Path = BASE_DIR / _read_setting("URETHANE_DB_PATH", "data/urethane_wip.db")
    admin_password: str = _read_setting("URETHANE_ADMIN_PASSWORD", "")
    default_plan_path: Path = BASE_DIR / "가공실적등록.xls"
    supabase_url: str = _read_setting("SUPABASE_URL", "")
    supabase_service_role_key: str = _read_setting("SUPABASE_SERVICE_ROLE_KEY", "")
    admin_session_key: str = "urethane_admin_authenticated"
    admin_name_key: str = "urethane_admin_name"


CONFIG = AppConfig()
