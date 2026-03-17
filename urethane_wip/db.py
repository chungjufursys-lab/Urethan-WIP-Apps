from __future__ import annotations

import time
from datetime import datetime
from functools import lru_cache
from typing import Any

import httpx
import pandas as pd
from supabase import Client, create_client

from urethane_wip.config import CONFIG


def utcnow_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@lru_cache(maxsize=1)
def get_client() -> Client:
    if not CONFIG.supabase_url or not CONFIG.supabase_service_role_key:
        raise RuntimeError("SUPABASE_URL 또는 SUPABASE_SERVICE_ROLE_KEY가 설정되지 않았습니다.")
    return create_client(CONFIG.supabase_url, CONFIG.supabase_service_role_key)


def init_database() -> None:
    get_client()


def _execute_with_retry(action, *, retries: int = 3, delay_seconds: float = 0.5):
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            return action()
        except (httpx.HTTPError, OSError) as exc:
            last_error = exc
            get_client.cache_clear()
            if attempt == retries - 1:
                raise
            time.sleep(delay_seconds * (attempt + 1))
    if last_error:
        raise last_error


def _apply_filters(query, filters: dict[str, Any] | None = None):
    if not filters:
        return query
    for column, value in filters.items():
        query = query.eq(column, value)
    return query


def fetch_table(
    table_name: str,
    *,
    columns: str = "*",
    filters: dict[str, Any] | None = None,
    order_by: str | None = None,
    ascending: bool = True,
    limit: int | None = None,
) -> pd.DataFrame:
    client = get_client()
    page_size = min(limit or 1000, 1000)
    offset = 0
    rows: list[dict[str, Any]] = []

    while True:
        def _run():
            query = client.table(table_name).select(columns)
            query = _apply_filters(query, filters)
            if order_by:
                query = query.order(order_by, desc=not ascending)
            query = query.range(offset, offset + page_size - 1)
            return query.execute()

        response = _execute_with_retry(_run)
        data = response.data or []
        rows.extend(data)

        if limit is not None and len(rows) >= limit:
            rows = rows[:limit]
            break
        if len(data) < page_size:
            break
        offset += page_size

    return pd.DataFrame(rows)


def insert_rows(table_name: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    for start in range(0, len(rows), 200):
        batch = rows[start:start + 200]
        _execute_with_retry(lambda: get_client().table(table_name).insert(batch).execute())


def upsert_rows(table_name: str, rows: list[dict[str, Any]], *, on_conflict: str) -> None:
    if not rows:
        return
    for start in range(0, len(rows), 200):
        batch = rows[start:start + 200]
        _execute_with_retry(lambda: get_client().table(table_name).upsert(batch, on_conflict=on_conflict).execute())


def update_rows(table_name: str, match_filters: dict[str, Any], values: dict[str, Any]) -> None:
    def _run():
        query = get_client().table(table_name).update(values)
        query = _apply_filters(query, match_filters)
        return query.execute()

    _execute_with_retry(_run)


def delete_rows(table_name: str, match_filters: dict[str, Any]) -> None:
    query = get_client().table(table_name).delete()
    query = _apply_filters(query, match_filters)
    _execute_with_retry(lambda: query.execute())


def delete_rows_by_ids(table_name: str, ids: list[int | str]) -> None:
    if not ids:
        return
    for start in range(0, len(ids), 200):
        batch = ids[start:start + 200]
        _execute_with_retry(lambda: get_client().table(table_name).delete().in_("id", batch).execute())


def table_has_rows(table_name: str) -> bool:
    return not fetch_table(table_name, limit=1).empty
