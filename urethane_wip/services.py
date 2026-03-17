from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache
from math import ceil

import pandas as pd

from urethane_wip import db
from urethane_wip.data_loader import make_business_key


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def clear_caches() -> None:
    _table_cached.cache_clear()


@lru_cache(maxsize=32)
def _table_cached(table_name: str, order_by: str | None, ascending: bool) -> pd.DataFrame:
    frame = db.fetch_table(table_name, order_by=order_by, ascending=ascending)
    return frame if not frame.empty else pd.DataFrame()


def _table(table_name: str, *, order_by: str | None = None, ascending: bool = True) -> pd.DataFrame:
    return _table_cached(table_name, order_by, ascending).copy()


def get_items() -> pd.DataFrame:
    return _table("items", order_by="item_code")


def get_item_variants() -> pd.DataFrame:
    return _table("item_variants", order_by="item_code")


def get_locations() -> pd.DataFrame:
    return _table("locations", order_by="location_code")


def get_inventory_detail() -> pd.DataFrame:
    inventory = _table("inventory", order_by="updated_at", ascending=False)
    if inventory.empty:
        return pd.DataFrame(columns=["id", "base_date", "item_code", "item_name", "color", "qty", "location_code", "location_name", "status", "remark", "updated_by", "updated_at"])

    items_all = get_items()
    locations_all = get_locations()
    items = items_all[["item_code", "item_name"]] if not items_all.empty else pd.DataFrame(columns=["item_code", "item_name"])
    locations = locations_all[["location_code", "location_name"]] if not locations_all.empty else pd.DataFrame(columns=["location_code", "location_name"])

    merged = inventory.merge(items, how="left", on="item_code").merge(locations, how="left", on="location_code")
    merged["item_name"] = merged["item_name"].fillna(merged["item_code"])
    merged["location_name"] = merged["location_name"].fillna(merged["location_code"])
    return merged[["id", "base_date", "item_code", "item_name", "color", "qty", "location_code", "location_name", "status", "remark", "updated_by", "updated_at"]]


def get_inventory_summary() -> pd.DataFrame:
    detail = get_inventory_input_sheet()
    if detail.empty:
        return pd.DataFrame(columns=["item_code", "item_name", "color", "current_qty", "pallet_count", "last_updated_at"])

    summary = detail[["item_code", "item_name", "color", "current_qty", "updated_at"]].copy()
    summary["pallet_count"] = summary["current_qty"].apply(calculate_pallets)
    summary = summary.rename(columns={"updated_at": "last_updated_at"})
    return summary.sort_values(["current_qty", "item_code"], ascending=[False, True])


def calculate_pallets(qty: float) -> int:
    qty_value = float(qty or 0)
    if qty_value <= 0:
        return 0
    return int(ceil(qty_value / 40))


def get_inventory_input_sheet() -> pd.DataFrame:
    detail = get_inventory_detail()
    latest_records = pd.DataFrame(columns=["record_id", "base_date", "item_code", "color", "current_qty", "remark", "updated_at", "updated_by"])
    if not detail.empty:
        latest_by_location = (
            detail.sort_values(["updated_at", "id"], ascending=[False, False])
            .drop_duplicates(subset=["item_code", "color", "location_code"], keep="first")
            .copy()
        )
        latest_by_location["qty"] = latest_by_location["qty"].fillna(0.0)
        total_rows = latest_by_location[latest_by_location["location_code"] == "TOTAL"].copy()
        total_records = (
            total_rows.rename(columns={"id": "record_id", "qty": "current_qty"})[
                ["record_id", "base_date", "item_code", "color", "current_qty", "remark", "updated_at", "updated_by"]
            ]
            if not total_rows.empty
            else pd.DataFrame(columns=["record_id", "base_date", "item_code", "color", "current_qty", "remark", "updated_at", "updated_by"])
        )

        aggregated_records = (
            latest_by_location[latest_by_location["location_code"] != "TOTAL"]
            .groupby(["item_code", "color"], dropna=False)
            .agg(
                record_id=("id", "max"),
                base_date=("base_date", "max"),
                current_qty=("qty", "sum"),
                remark=("remark", "last"),
                updated_at=("updated_at", "max"),
                updated_by=("updated_by", "last"),
            )
            .reset_index()
        )
        if not total_records.empty:
            total_keys = set(zip(total_records["item_code"], total_records["color"]))
            aggregated_records = aggregated_records[
                ~aggregated_records.apply(lambda row: (row["item_code"], row["color"]) in total_keys, axis=1)
            ]
        frames = [frame for frame in (aggregated_records, total_records) if not frame.empty]
        latest_records = pd.concat(frames, ignore_index=True) if frames else latest_records

    variants = get_item_variants()
    item_variants = variants[["item_code", "color"]] if not variants.empty else pd.DataFrame(columns=["item_code", "color"])
    inventory_keys = latest_records[["item_code", "color"]] if not latest_records.empty else pd.DataFrame(columns=["item_code", "color"])
    base_keys = pd.concat([item_variants, inventory_keys], ignore_index=True).drop_duplicates()

    if base_keys.empty:
        items_all = get_items()
        if items_all.empty:
            return pd.DataFrame(columns=["record_id", "base_date", "item_code", "item_name", "color", "current_qty", "current_pallet", "remark", "updated_at", "updated_by"])
        base_keys = items_all[["item_code"]].copy()
        base_keys["color"] = ""

    items_all = get_items()
    items = items_all[["item_code", "item_name"]] if not items_all.empty else pd.DataFrame(columns=["item_code", "item_name"])
    merged = base_keys.merge(items, how="left", on="item_code").merge(latest_records, how="left", on=["item_code", "color"])
    merged["item_name"] = merged["item_name"].fillna(merged["item_code"])
    merged["base_date"] = merged["base_date"].fillna("")
    merged["current_qty"] = pd.to_numeric(merged["current_qty"], errors="coerce").fillna(0.0)
    merged["remark"] = merged["remark"].fillna("")
    merged["updated_at"] = merged["updated_at"].fillna("")
    merged["updated_by"] = merged["updated_by"].fillna("")
    merged["current_pallet"] = merged["current_qty"].apply(calculate_pallets)

    return merged[["record_id", "base_date", "item_code", "item_name", "color", "current_qty", "current_pallet", "remark", "updated_at", "updated_by"]].sort_values(["item_code", "color"], ascending=[True, True])


def get_item_vendor_map() -> pd.DataFrame:
    frame = _table("item_vendor_map", order_by="vendor_name")
    if frame.empty:
        return pd.DataFrame(columns=["id", "item_code", "color", "vendor_name", "updated_at"])
    return frame[["id", "item_code", "color", "vendor_name", "updated_at"]].sort_values(["vendor_name", "item_code", "color"], ascending=[True, True, True])


def get_plan_detail() -> pd.DataFrame:
    plans = _table("production_plan", order_by="updated_at", ascending=False)
    if plans.empty:
        return pd.DataFrame(columns=["id", "plan_date", "product_code", "product_name", "item_code", "item_name", "color", "plan_qty", "required_qty", "due_date", "priority", "plan_status", "source_type", "completed_at", "share_yn", "source_note", "updated_by", "updated_at", "vendor_name"])

    items_all = get_items()
    vendor_map_all = get_item_vendor_map()
    items = items_all[["item_code", "item_name"]] if not items_all.empty else pd.DataFrame(columns=["item_code", "item_name"])
    vendor_map = vendor_map_all[["item_code", "color", "vendor_name"]] if not vendor_map_all.empty else pd.DataFrame(columns=["item_code", "color", "vendor_name"])

    detail = plans.rename(columns={"urethane_item_code": "item_code"}).merge(items, how="left", on="item_code").merge(vendor_map, how="left", on=["item_code", "color"])
    detail["item_name"] = detail["item_name"].fillna(detail["item_code"])
    status_order = detail["plan_status"].map(lambda value: 0 if value == "진행중" else 1).fillna(1)
    detail = detail.assign(_status_order=status_order).sort_values(["_status_order", "due_date", "id"], ascending=[True, True, False]).drop(columns=["_status_order"])
    return detail[["id", "plan_date", "product_code", "product_name", "item_code", "item_name", "color", "plan_qty", "required_qty", "due_date", "priority", "plan_status", "source_type", "completed_at", "share_yn", "source_note", "updated_by", "updated_at", "vendor_name"]]


def build_shortage_report() -> pd.DataFrame:
    inventory = get_inventory_summary().rename(columns={"current_qty": "current_wip"})[["item_code", "item_name", "color", "current_wip"]]
    plans = get_plan_detail()
    active_plans = plans[plans["plan_status"] != "생산완료"].copy()
    if active_plans.empty:
        return pd.DataFrame(columns=["item_code", "item_name", "color", "current_wip", "required_qty", "shortage_qty", "due_date", "shortage_date", "priority", "risk_level", "vendor_name", "remaining_after_due"])

    plan_summary = (
        active_plans.groupby(["item_code", "item_name", "color", "plan_date", "due_date", "vendor_name"], dropna=False)
        .agg(plan_qty=("plan_qty", "sum"), required_qty=("required_qty", "sum"), priority=("priority", lambda s: ",".join(sorted(set(map(str, s))))))
        .reset_index()
    )
    merged = plan_summary.merge(inventory, how="left", on=["item_code", "item_name", "color"])
    merged["current_wip"] = merged["current_wip"].fillna(0)
    merged["plan_date"] = pd.to_datetime(merged["plan_date"], errors="coerce")
    merged["due_date"] = pd.to_datetime(merged["due_date"], errors="coerce")
    merged["shortage_date"] = merged["plan_date"] - pd.Timedelta(days=1)
    merged = merged.sort_values(["item_code", "color", "shortage_date", "due_date"], ascending=[True, True, True, True]).reset_index(drop=True)
    merged["cumulative_required_qty"] = merged.groupby(["item_code", "color"], dropna=False)["required_qty"].cumsum()
    merged["today_required_qty"] = merged.apply(lambda row: row["required_qty"] if pd.notna(row["shortage_date"]) and row["shortage_date"].date() <= date.today() else 0, axis=1)
    merged["d2_required_qty"] = merged.apply(lambda row: row["required_qty"] if pd.notna(row["shortage_date"]) and row["shortage_date"].date() <= date.today() + timedelta(days=2) else 0, axis=1)
    merged["required_today_cumulative"] = merged.groupby(["item_code", "color"], dropna=False)["today_required_qty"].transform("sum")
    merged["required_d2_cumulative"] = merged.groupby(["item_code", "color"], dropna=False)["d2_required_qty"].transform("sum")
    merged["required_total_qty"] = merged.groupby(["item_code", "color"], dropna=False)["required_qty"].transform("sum")
    merged["shortage_qty"] = (merged["cumulative_required_qty"] - merged["current_wip"]).clip(lower=0)
    merged["remaining_after_due"] = (merged["current_wip"] - merged["cumulative_required_qty"]).clip(lower=0)
    merged["risk_level"] = merged.apply(_risk_level_from_row, axis=1)
    merged["plan_date"] = merged["plan_date"].dt.strftime("%Y-%m-%d")
    merged["due_date"] = merged["due_date"].dt.strftime("%Y-%m-%d")
    merged["shortage_date"] = merged["shortage_date"].dt.strftime("%Y-%m-%d")
    return merged.sort_values(["risk_level", "shortage_date", "due_date", "item_code"], ascending=[True, True, True, True])


def get_vendor_share_view() -> pd.DataFrame:
    vendor_map = get_item_vendor_map()
    if vendor_map.empty:
        return vendor_map

    inventory = get_inventory_summary()[["item_code", "item_name", "color", "current_qty"]].rename(columns={"current_qty": "current_wip"})
    shortage = build_shortage_report()
    if shortage.empty:
        base = vendor_map.merge(inventory, how="left", on=["item_code", "color"])
        base["item_name"] = base["item_name"].fillna(base["item_code"])
        base["current_wip"] = base["current_wip"].fillna(0)
        base["due_date"] = ""
        base["required_qty"] = 0.0
        base["remaining_after_due"] = base["current_wip"]
        base["shortage_qty"] = 0.0
        base["risk_level"] = "정상"
        base["priority"] = "보통"
    else:
        base = vendor_map.merge(shortage, how="left", on=["item_code", "color", "vendor_name"])
        base = base.merge(inventory, how="left", on=["item_code", "color"], suffixes=("", "_inventory"))
        base["item_name"] = base["item_name"].fillna(base["item_name_inventory"]).fillna(base["item_code"])
        base["current_wip"] = base["current_wip"].fillna(0)
        base["required_qty"] = base["required_qty"].fillna(0)
        base["remaining_after_due"] = base["remaining_after_due"].fillna(base["current_wip"])
        base["shortage_qty"] = base["shortage_qty"].fillna(0)
        base["risk_level"] = base["risk_level"].fillna("정상")
        base["priority"] = base["priority"].fillna("보통")
        base["due_date"] = base["due_date"].fillna("")

    base["current_pallet"] = base["current_wip"].apply(calculate_pallets)
    base["required_pallet"] = base["required_qty"].apply(calculate_pallets)
    base["shortage_pallet"] = base["shortage_qty"].apply(calculate_pallets)
    return base[["vendor_name", "item_code", "item_name", "color", "due_date", "current_wip", "current_pallet", "required_qty", "required_pallet", "remaining_after_due", "shortage_qty", "shortage_pallet", "risk_level", "priority"]].sort_values(["vendor_name", "due_date", "item_code"], ascending=[True, True, True])


def get_dashboard_metrics() -> dict[str, float | int]:
    inventory = get_inventory_input_sheet()
    inventory_summary = get_inventory_summary()
    shortage = build_shortage_report()
    vendor = get_vendor_share_view()
    return {
        "total_wip_qty": float(inventory["current_qty"].sum()) if not inventory.empty else 0.0,
        "total_pallet_count": int(inventory_summary["pallet_count"].sum()) if not inventory_summary.empty else 0,
        "total_required_qty": float(shortage["required_qty"].sum()) if not shortage.empty else 0.0,
        "shortage_item_count": int((shortage["shortage_qty"] > 0).sum()) if not shortage.empty else 0,
        "urgent_count": int((shortage["risk_level"] == "미출").sum()) if not shortage.empty else 0,
        "vendor_share_count": int(len(vendor)),
    }


def get_due_soon_dashboard_report() -> pd.DataFrame:
    report = build_shortage_report().copy()
    if report.empty:
        return report
    due_limit = date.today() + timedelta(days=2)
    due_dates = pd.to_datetime(report["shortage_date"], errors="coerce")
    filtered = report[(due_dates.dt.date <= due_limit) & (report["required_qty"] > 0)].copy()
    return filtered.sort_values(["risk_level", "shortage_date", "due_date", "item_code"], ascending=[True, True, True, True])


def get_plan_calendar_entries(item_code: str, color: str) -> pd.DataFrame:
    plans = get_plan_detail()
    if plans.empty:
        return pd.DataFrame(columns=["plan_date", "due_date", "plan_qty", "required_qty", "product_count"])

    filtered = plans[(plans["item_code"] == item_code) & (plans["color"].fillna("") == (color or "")) & (plans["plan_status"] != "생산완료")].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["plan_date", "due_date", "plan_qty", "required_qty", "product_count"])

    filtered["plan_date"] = pd.to_datetime(filtered["plan_date"], errors="coerce")
    filtered["due_date"] = pd.to_datetime(filtered["due_date"], errors="coerce")
    grouped = (
        filtered.groupby(["plan_date"], dropna=False)
        .agg(due_date=("due_date", "min"), plan_qty=("plan_qty", "sum"), required_qty=("required_qty", "sum"), product_count=("product_code", "nunique"))
        .reset_index()
        .sort_values(["plan_date", "due_date"], ascending=[True, True])
    )
    grouped["plan_date"] = grouped["plan_date"].dt.strftime("%Y-%m-%d")
    grouped["due_date"] = grouped["due_date"].dt.strftime("%Y-%m-%d")
    return grouped


def _risk_level_from_row(row: pd.Series) -> str:
    current_wip = float(row.get("current_wip", 0) or 0)
    required_today = float(row.get("required_today_cumulative", 0) or 0)
    required_d2 = float(row.get("required_d2_cumulative", 0) or 0)
    required_total = float(row.get("required_total_qty", 0) or 0)

    if current_wip > required_total and required_total > 0:
        return "과입고"
    if current_wip < required_today:
        return "미출"
    if current_wip < required_d2:
        return "주의"
    if current_wip < required_total:
        return "일정확인요망"
    return "정상"


def save_item(item_code: str, item_name: str, category: str, spec: str, unit: str, active_yn: str) -> None:
    now = db.utcnow_text()
    db.upsert_rows("items", [{"item_code": item_code, "item_name": item_name, "category": category, "spec": spec, "unit": unit, "active_yn": active_yn, "updated_at": now}], on_conflict="item_code")
    clear_caches()


def save_item_variant(item_code: str, color: str, active_yn: str) -> None:
    now = db.utcnow_text()
    db.upsert_rows("item_variants", [{"item_code": item_code, "color": color, "display_name": f"{item_code} / {color}", "active_yn": active_yn, "updated_at": now}], on_conflict="item_code,color")
    clear_caches()


def save_location(location_code: str, location_name: str, area_type: str, capacity: float, use_yn: str) -> None:
    now = db.utcnow_text()
    db.upsert_rows("locations", [{"location_code": location_code, "location_name": location_name, "area_type": area_type, "capacity": capacity, "use_yn": use_yn, "updated_at": now}], on_conflict="location_code")
    clear_caches()


def save_inventory(record_id: int | None, base_date: str, item_code: str, color: str, qty: float, location_code: str, status: str, remark: str, updated_by: str) -> None:
    now = db.utcnow_text()
    payload = {"base_date": base_date, "item_code": item_code, "color": color, "qty": qty, "location_code": location_code, "status": status, "remark": remark, "updated_by": updated_by, "updated_at": now}
    if record_id:
        db.update_rows("inventory", {"id": record_id}, payload)
    else:
        db.insert_rows("inventory", [payload])
    clear_caches()


def save_inventory_snapshot(base_date: str, item_code: str, color: str, qty: float, remark: str, updated_by: str) -> None:
    existing = get_inventory_detail()
    existing = existing[(existing["item_code"] == item_code) & (existing["color"].fillna("") == (color or ""))].sort_values(["updated_at", "id"], ascending=[False, False])
    record_id = int(existing.iloc[0]["id"]) if not existing.empty else None
    save_inventory(record_id, base_date, item_code, color, qty, "TOTAL", "정상", remark, updated_by)


def save_item_vendor_map(record_id: int | None, item_code: str, color: str, vendor_name: str) -> None:
    now = db.utcnow_text()
    payload = {"item_code": item_code, "color": color, "vendor_name": vendor_name, "updated_at": now}
    if record_id:
        db.update_rows("item_vendor_map", {"id": record_id}, payload)
    else:
        db.upsert_rows("item_vendor_map", [payload], on_conflict="item_code,color")
    clear_caches()


def save_plan(record_id: int | None, plan_date: str, product_code: str, product_name: str, item_code: str, color: str, plan_qty: float, required_qty: float, due_date: str, priority: str, plan_status: str, share_yn: str, source_note: str, updated_by: str) -> None:
    now = db.utcnow_text()
    payload = {
        "plan_date": plan_date,
        "product_code": product_code,
        "product_name": product_name,
        "urethane_item_code": item_code,
        "color": color,
        "plan_qty": plan_qty,
        "required_qty": 0 if plan_status == "생산완료" else required_qty,
        "due_date": due_date,
        "priority": priority,
        "business_key": make_business_key(item_code, color, due_date),
        "plan_status": plan_status,
        "source_type": "MANUAL",
        "completed_at": now if plan_status == "생산완료" else None,
        "share_yn": share_yn,
        "source_note": source_note,
        "updated_by": updated_by,
        "updated_at": now,
    }
    if record_id:
        db.update_rows("production_plan", {"id": record_id}, payload)
    else:
        db.insert_rows("production_plan", [payload])
    clear_caches()


def save_vendor_share(record_id: int | None, vendor_name: str, item_code: str, color: str, request_qty: float, due_date: str, status: str, remark: str) -> None:
    now = db.utcnow_text()
    payload = {"vendor_name": vendor_name, "item_code": item_code, "color": color, "request_qty": request_qty, "due_date": due_date, "status": status, "remark": remark, "updated_at": now}
    if record_id:
        db.update_rows("vendor_share", {"id": record_id}, payload)
    else:
        db.insert_rows("vendor_share", [payload])
    clear_caches()


def delete_by_id(table_name: str, record_id: int) -> None:
    db.delete_rows(table_name, {"id": record_id})
    clear_caches()


def delete_by_code(table_name: str, code_column: str, code: str) -> None:
    db.delete_rows(table_name, {code_column: code})
    clear_caches()
