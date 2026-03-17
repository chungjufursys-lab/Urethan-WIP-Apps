from __future__ import annotations

import io
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from urethane_wip import db
from urethane_wip.config import CONFIG


ITEM_COLUMNS = ["item_code", "item_name", "category", "spec", "unit", "active_yn", "updated_at"]
VARIANT_COLUMNS = ["item_code", "color", "display_name", "active_yn", "updated_at"]
PLAN_COLUMNS = [
    "plan_date",
    "product_code",
    "product_name",
    "urethane_item_code",
    "color",
    "plan_qty",
    "required_qty",
    "due_date",
    "priority",
    "business_key",
    "plan_status",
    "source_type",
    "completed_at",
    "share_yn",
    "source_note",
    "updated_by",
    "updated_at",
]


def _normalize_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _pick_first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def _coerce_date_text(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def make_business_key(item_code: str, color: str, due_date: str) -> str:
    return f"{item_code.strip()}::{color.strip()}::{due_date.strip()}"


def _priority_from_shortage_date(shortage_date: object) -> str:
    if pd.isna(shortage_date):
        return "보통"
    shortage_day = pd.to_datetime(shortage_date).date()
    if shortage_day <= date.today():
        return "긴급"
    if shortage_day <= date.today() + timedelta(days=2):
        return "높음"
    return "보통"


def parse_plan_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    card_no_col = _pick_first_existing(df, ["부품이동카드번호"])
    first_input_date_col = _pick_first_existing(df, ["최초투입일자"])
    package_date_col = _pick_first_existing(df, ["포장일자"])
    product_code_col = _pick_first_existing(df, ["제품코드"])
    product_color_col = _pick_first_existing(df, ["제품색상"])
    product_name_col = _pick_first_existing(df, ["제품명"])
    part_code_col = _pick_first_existing(df, ["부품코드"])
    part_color_col = _pick_first_existing(df, ["부품색상"])
    part_name_col = _pick_first_existing(df, ["부품명"])
    plan_qty_col = _pick_first_existing(df, ["계획량"])
    produced_qty_col = _pick_first_existing(df, ["생산수량"])
    remark_col = _pick_first_existing(df, ["건명"])

    if not all([card_no_col, first_input_date_col, package_date_col, product_code_col, product_color_col, plan_qty_col, produced_qty_col, remark_col]):
        return pd.DataFrame(columns=PLAN_COLUMNS)

    filtered = df[df[remark_col].astype(str).str.contains("외주입고", na=False)].copy()
    if filtered.empty:
        return pd.DataFrame(columns=PLAN_COLUMNS)

    filtered["business_key"] = filtered[card_no_col].map(_normalize_text)
    filtered["product_code_norm"] = filtered[product_code_col].map(_normalize_text)
    filtered["color_norm"] = filtered[product_color_col].map(_normalize_text)
    filtered["product_name_norm"] = filtered[product_name_col].map(_normalize_text) if product_name_col else filtered["product_code_norm"]
    filtered["part_code_norm"] = filtered[part_code_col].map(_normalize_text) if part_code_col else filtered["product_code_norm"]
    filtered["part_color_norm"] = filtered[part_color_col].map(_normalize_text) if part_color_col else filtered["color_norm"]
    filtered["part_name_norm"] = filtered[part_name_col].map(_normalize_text) if part_name_col else filtered["product_name_norm"]
    filtered["plan_date_norm"] = _coerce_date_text(filtered[first_input_date_col])
    filtered["due_date_norm"] = _coerce_date_text(filtered[package_date_col])
    filtered["plan_qty_norm"] = pd.to_numeric(filtered[plan_qty_col], errors="coerce")
    filtered["produced_qty_norm"] = pd.to_numeric(filtered[produced_qty_col], errors="coerce")

    filtered = filtered[
        (filtered["business_key"] != "")
        & (filtered["product_code_norm"] != "")
        & (filtered["part_code_norm"] != "")
        & (filtered["part_color_norm"] != "")
        & filtered["plan_date_norm"].notna()
        & filtered["due_date_norm"].notna()
        & filtered["plan_qty_norm"].notna()
        & filtered["produced_qty_norm"].notna()
    ].copy()
    if filtered.empty:
        return pd.DataFrame(columns=PLAN_COLUMNS)

    filtered["plan_qty_norm"] = filtered["plan_qty_norm"].astype(int)
    filtered["produced_qty_norm"] = filtered["produced_qty_norm"].astype(int)
    filtered["required_qty"] = (filtered["plan_qty_norm"] - filtered["produced_qty_norm"]).clip(lower=0).astype(int)
    filtered["shortage_date"] = pd.to_datetime(filtered["plan_date_norm"], errors="coerce") - pd.Timedelta(days=1)
    filtered["priority_text"] = filtered["shortage_date"].map(_priority_from_shortage_date)

    normalized = pd.DataFrame(
        {
            "plan_date": filtered["plan_date_norm"],
            "product_code": filtered["product_code_norm"],
            "product_name": filtered["product_name_norm"],
            "urethane_item_code": filtered["part_code_norm"],
            "color": filtered["part_color_norm"],
            "part_name": filtered["part_name_norm"],
            "plan_qty": filtered["plan_qty_norm"],
            "required_qty": filtered["required_qty"],
            "due_date": filtered["due_date_norm"],
            "priority": filtered["priority_text"],
            "business_key": filtered["business_key"],
            "plan_status": "진행중",
            "source_type": "UPLOAD",
            "completed_at": None,
            "share_yn": "Y",
            "source_note": filtered[remark_col].map(_normalize_text),
            "updated_by": "system",
            "updated_at": db.utcnow_text(),
        }
    )

    aggregated = (
        normalized.groupby(["business_key"], dropna=False)
        .agg(
            plan_date=("plan_date", "max"),
            product_code=("product_code", "first"),
            product_name=("product_name", "first"),
            urethane_item_code=("urethane_item_code", "first"),
            color=("color", "first"),
            part_name=("part_name", "first"),
            plan_qty=("plan_qty", "sum"),
            required_qty=("required_qty", "sum"),
            due_date=("due_date", "max"),
            priority=("priority", _merge_priority),
            plan_status=("plan_status", "first"),
            source_type=("source_type", "first"),
            completed_at=("completed_at", "first"),
            share_yn=("share_yn", "first"),
            source_note=("source_note", lambda s: " / ".join(sorted({value for value in s if value}))),
            updated_by=("updated_by", "first"),
            updated_at=("updated_at", "max"),
        )
        .reset_index()
    )
    return aggregated[PLAN_COLUMNS]


def _merge_priority(values: pd.Series) -> str:
    order = {"긴급": 0, "높음": 1, "보통": 2}
    unique_values = sorted({str(value) for value in values if str(value)}, key=lambda value: order.get(value, 99))
    return unique_values[0] if unique_values else "보통"


def load_plan_from_path(path: Path) -> pd.DataFrame:
    return parse_plan_dataframe(pd.read_excel(path))


def load_plan_from_bytes(uploaded_bytes: bytes, file_name: str) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()
    buffer = io.BytesIO(uploaded_bytes)
    source = pd.read_csv(buffer) if suffix == ".csv" else pd.read_excel(buffer)
    return parse_plan_dataframe(source)


def sync_item_vendor_map_from_csv() -> int:
    csv_path = CONFIG.base_dir / "품목별 업체현황.csv"
    if not csv_path.exists():
        return 0

    vendor_df = pd.read_csv(csv_path, encoding="utf-8-sig")
    vendor_df = vendor_df.rename(columns={"업체명": "vendor_name"})
    required_columns = {"item_code", "color", "vendor_name"}
    if not required_columns.issubset(vendor_df.columns):
        return 0

    vendor_df = vendor_df[list(required_columns)].fillna("")
    vendor_df["item_code"] = vendor_df["item_code"].astype(str).str.strip()
    vendor_df["color"] = vendor_df["color"].astype(str).str.strip()
    vendor_df["vendor_name"] = vendor_df["vendor_name"].astype(str).str.strip()
    vendor_df = vendor_df[(vendor_df["item_code"] != "") & (vendor_df["vendor_name"] != "")]
    vendor_df = vendor_df.drop_duplicates(subset=["item_code", "color"], keep="first")
    vendor_df["updated_at"] = db.utcnow_text()

    item_rows = (
        vendor_df[["item_code"]]
        .drop_duplicates()
        .assign(item_name=lambda frame: frame["item_code"], category="우레탄", spec="업체맵", unit="EA", active_yn="Y", updated_at=db.utcnow_text())
    )
    db.upsert_rows("items", item_rows[ITEM_COLUMNS].to_dict("records"), on_conflict="item_code")

    existing_vendor_map = db.fetch_table("item_vendor_map")
    if not existing_vendor_map.empty:
        db.delete_rows_by_ids("item_vendor_map", existing_vendor_map["id"].astype(int).tolist())
    db.insert_rows("item_vendor_map", vendor_df[["item_code", "color", "vendor_name", "updated_at"]].to_dict("records"))
    from urethane_wip.services import clear_caches

    clear_caches()
    return int(len(vendor_df))


def seed_database_if_needed() -> None:
    now = db.utcnow_text()
    vendor_map = db.fetch_table("item_vendor_map", limit=1)
    if vendor_map.empty:
        sync_item_vendor_map_from_csv()

    locations = db.fetch_table("locations", limit=1)
    if not locations.empty:
        return

    db.insert_rows(
        "locations",
        [
            {"location_code": "LOC-A", "location_name": "우레탄 1구역", "area_type": "일반", "capacity": 500, "use_yn": "Y", "updated_at": now},
            {"location_code": "LOC-B", "location_name": "우레탄 2구역", "area_type": "일반", "capacity": 800, "use_yn": "Y", "updated_at": now},
            {"location_code": "LOC-C", "location_name": "외주 대기구역", "area_type": "출하대기", "capacity": 300, "use_yn": "Y", "updated_at": now},
            {"location_code": "LOC-D", "location_name": "검사 대기구역", "area_type": "검사", "capacity": 200, "use_yn": "Y", "updated_at": now},
            {"location_code": "TOTAL", "location_name": "총재공", "area_type": "집계", "capacity": 999999, "use_yn": "Y", "updated_at": now},
        ],
    )
    from urethane_wip.services import clear_caches

    clear_caches()


def _sync_plan_reference_data(normalized: pd.DataFrame) -> None:
    now = db.utcnow_text()
    item_name_source = "part_name" if "part_name" in normalized.columns else "product_name"
    item_rows = (
        normalized[["urethane_item_code", item_name_source]]
        .drop_duplicates()
        .rename(columns={"urethane_item_code": "item_code", item_name_source: "item_name"})
        .assign(category="우레탄", spec="업로드", unit="EA", active_yn="Y", updated_at=now)
    )
    variant_rows = (
        normalized[["urethane_item_code", "color"]]
        .drop_duplicates()
        .rename(columns={"urethane_item_code": "item_code"})
        .assign(display_name=lambda frame: frame["item_code"] + " / " + frame["color"], active_yn="Y", updated_at=now)
    )

    db.upsert_rows("items", item_rows[ITEM_COLUMNS].to_dict("records"), on_conflict="item_code")
    db.upsert_rows("item_variants", variant_rows[VARIANT_COLUMNS].to_dict("records"), on_conflict="item_code,color")


def replace_production_plan(plan_df: pd.DataFrame, updated_by: str) -> dict[str, int]:
    normalized = plan_df.copy()
    if normalized.empty:
        return {"active_count": 0, "deleted_count": 0, "inserted_count": 0, "updated_count": 0}

    normalized["updated_by"] = updated_by
    normalized["updated_at"] = db.utcnow_text()
    normalized["plan_status"] = "진행중"
    normalized["source_type"] = "UPLOAD"
    normalized["completed_at"] = None

    _sync_plan_reference_data(normalized)

    existing_upload = db.fetch_table("production_plan")
    existing_upload = existing_upload[existing_upload["source_type"] == "UPLOAD"].copy() if not existing_upload.empty else pd.DataFrame(columns=["id", "business_key"])
    latest_by_key = existing_upload.sort_values(["id"], ascending=[False]).drop_duplicates(subset=["business_key"], keep="first") if not existing_upload.empty else pd.DataFrame(columns=["id", "business_key"])
    existing_map = dict(zip(latest_by_key["business_key"], latest_by_key["id"]))
    uploaded_keys = set(normalized["business_key"].tolist())

    inserted_count = 0
    updated_count = 0
    for _, row in normalized.iterrows():
        payload = {column: row[column] for column in PLAN_COLUMNS}
        existing_id = existing_map.get(row["business_key"])
        if existing_id:
            updated_count += 1
            db.update_rows("production_plan", {"id": int(existing_id)}, payload)
        else:
            inserted_count += 1
            db.insert_rows("production_plan", [payload])

    delete_ids: list[int] = []
    if not existing_upload.empty:
        for _, row in existing_upload.iterrows():
            if not row.get("business_key") or row["business_key"] not in uploaded_keys:
                delete_ids.append(int(row["id"]))

    if delete_ids:
        db.delete_rows_by_ids("production_plan", delete_ids)

    from urethane_wip.services import clear_caches

    clear_caches()
    return {
        "active_count": int(len(normalized)),
        "deleted_count": int(len(delete_ids)),
        "inserted_count": inserted_count,
        "updated_count": updated_count,
    }
