from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from urethane_wip.config import CONFIG
from urethane_wip import db
from urethane_wip.data_loader import load_plan_from_path, replace_production_plan, sync_item_vendor_map_from_csv


def cleanup_derived_masters() -> dict[str, int]:
    plans = db.fetch_table("production_plan")
    if plans.empty:
        return {"items_rebuilt": 0, "variants_rebuilt": 0}

    item_rows = (
        plans[["urethane_item_code"]]
        .drop_duplicates()
        .rename(columns={"urethane_item_code": "item_code"})
        .assign(item_name=lambda frame: frame["item_code"], category="우레탄", spec="업로드", unit="EA", active_yn="Y", updated_at=db.utcnow_text())
    )
    variant_rows = (
        plans[["urethane_item_code", "color"]]
        .drop_duplicates()
        .rename(columns={"urethane_item_code": "item_code"})
        .assign(display_name=lambda frame: frame["item_code"] + " / " + frame["color"], active_yn="Y", updated_at=db.utcnow_text())
    )

    db.upsert_rows("items", item_rows.to_dict("records"), on_conflict="item_code")
    db.upsert_rows("item_variants", variant_rows.to_dict("records"), on_conflict="item_code,color")
    return {"items_rebuilt": len(item_rows), "variants_rebuilt": len(variant_rows)}


def main() -> None:
    plan_path = CONFIG.default_plan_path
    if not plan_path.exists():
        raise SystemExit(f"기본 계획 파일이 없습니다: {plan_path}")

    normalized = load_plan_from_path(plan_path)
    if normalized.empty:
        raise SystemExit("가공실적등록.xls 에서 외주입고 계획을 찾지 못했습니다.")

    summary = replace_production_plan(normalized, "codex-migration")
    vendor_count = sync_item_vendor_map_from_csv()
    cleanup_summary = cleanup_derived_masters()

    print("production_plan rebuild:", summary)
    print("item_vendor_map rebuild:", vendor_count)
    print("cleanup:", cleanup_summary)


if __name__ == "__main__":
    main()
