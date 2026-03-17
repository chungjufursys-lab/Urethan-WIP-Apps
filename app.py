from __future__ import annotations

import calendar
from datetime import date

import pandas as pd
import streamlit as st

from urethane_wip import auth, db, services
from urethane_wip.config import CONFIG
from urethane_wip.data_loader import load_plan_from_bytes, load_plan_from_path, replace_production_plan, seed_database_if_needed


st.set_page_config(page_title="우레탄 재공 관리 툴", page_icon="📦", layout="wide")


def init_app() -> None:
    """Initialize database and auth state exactly once per session."""
    db.init_database()
    if not st.session_state.get("urethane_reference_seeded"):
        seed_database_if_needed()
        st.session_state["urethane_reference_seeded"] = True
    auth.init_auth_state()
    st.session_state["urethane_initialized"] = True


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .badge {
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 999px;
            font-weight: 700;
            font-size: 0.82rem;
            margin-right: 0.25rem;
        }
        .badge-normal { background: #d8f5d0; color: #1e6b2d; }
        .badge-short { background: #ffe1b5; color: #8a4b00; }
        .badge-warning { background: #ffd7b8; color: #a14b00; }
        .badge-urgent { background: #ffd3d3; color: #9f1d1d; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def status_badge(value: str) -> str:
    mapping = {
        "정상": "badge badge-normal",
        "일정확인요망": "badge badge-normal",
        "주의": "badge badge-warning",
        "미출": "badge badge-urgent",
        "과입고": "badge badge-urgent",
    }
    return f"<span class='{mapping.get(value, 'badge badge-short')}'>{value}</span>"


def _build_month_calendar(entries: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    entry_map: dict[int, dict[str, str]] = {}
    for _, row in entries.iterrows():
        plan_day = pd.to_datetime(row["plan_date"], errors="coerce")
        if pd.isna(plan_day) or plan_day.year != year or plan_day.month != month:
            continue
        entry_map[plan_day.day] = {
            "required_qty": f"{float(row['required_qty'] or 0):.0f}",
        }

    weeks = calendar.monthcalendar(year, month)
    rows: list[list[str]] = []
    for week in weeks:
        week_cells: list[str] = []
        for day in week:
            if day == 0:
                week_cells.append("<div class='calendar-cell empty'></div>")
                continue
            day_entry = entry_map.get(day)
            if day_entry:
                week_cells.append(
                    "<div class='calendar-cell active'>"
                    f"<div class='calendar-day'>{day}</div>"
                    f"<div class='calendar-qty'>{day_entry['required_qty']}</div>"
                    "</div>"
                )
            else:
                week_cells.append(
                    "<div class='calendar-cell'>"
                    f"<div class='calendar-day'>{day}</div>"
                    "<div class='calendar-qty empty'>-</div>"
                    "</div>"
                )
        rows.append(week_cells)
    return pd.DataFrame(rows, columns=["월", "화", "수", "목", "금", "토", "일"])


@st.dialog("계획 캘린더", width="large")
def render_plan_calendar_dialog(item_code: str, color: str) -> None:
    entries = services.get_plan_calendar_entries(item_code, color)
    st.markdown(
        """
        <style>
        .calendar-grid {
            display: grid;
            grid-template-columns: repeat(7, minmax(0, 1fr));
            gap: 8px;
            margin-bottom: 10px;
        }
        .calendar-head {
            font-size: 0.78rem;
            font-weight: 700;
            color: #666;
            text-align: center;
        }
        .calendar-cell {
            min-height: 88px;
            border: 1px solid #e5e2d8;
            border-radius: 10px;
            padding: 8px;
            background: #fbf8f1;
        }
        .calendar-cell.active {
            background: #fff1c7;
            border-color: #d7b64a;
        }
        .calendar-cell.empty {
            background: transparent;
            border: none;
        }
        .calendar-day {
            font-size: 0.72rem;
            color: #6b7280;
            margin-bottom: 8px;
        }
        .calendar-qty {
            font-size: 1.4rem;
            font-weight: 700;
            line-height: 1.1;
            color: #1f2937;
        }
        .calendar-qty.empty {
            color: #c4c4c4;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.subheader(f"{item_code} / {color or '-'}")
    if entries.empty:
        st.info("선택한 품목/색상의 진행중 계획이 없습니다.")
        return

    months = (
        pd.to_datetime(entries["plan_date"], errors="coerce")
        .dropna()
        .dt.to_period("M")
        .astype(str)
        .drop_duplicates()
        .tolist()
    )

    for month_text in months:
        year, month = map(int, month_text.split("-"))
        st.caption(f"{year}-{month:02d}")
        month_df = _build_month_calendar(entries, year, month)
        header_html = "".join(f"<div class='calendar-head'>{day}</div>" for day in month_df.columns)
        body_html = ""
        for _, row in month_df.iterrows():
            body_html += "".join(row.tolist())
        st.markdown(f"<div class='calendar-grid'>{header_html}{body_html}</div>", unsafe_allow_html=True)

    with st.expander("일자별 상세 보기"):
        detail_view = entries.rename(
            columns={
                "plan_date": "계획일",
                "due_date": "포장일",
                "plan_qty": "계획량",
                "required_qty": "부족기준수량",
                "product_count": "제품코드수",
            }
        )
        st.dataframe(detail_view, use_container_width=True, hide_index=True)


@st.dialog("관리자 인증")
def admin_login_dialog(target_page: str) -> None:
    st.caption(f"`{target_page}` 화면은 관리자만 수정할 수 있습니다.")
    if not CONFIG.admin_password:
        st.error(".env에 URETHANE_ADMIN_PASSWORD가 설정되지 않았습니다.")
        return

    with st.form("admin_popup_login_form"):
        admin_name = st.text_input("수정자명", value=auth.current_admin_name() or "관리자")
        password = st.text_input("관리자 비밀번호", type="password")
        submitted = st.form_submit_button("로그인", use_container_width=True)
        if submitted:
            if auth.login(password, admin_name):
                st.success("관리자 인증이 완료되었습니다.")
                st.rerun()
            st.error("비밀번호가 일치하지 않습니다.")


def render_admin_intro(title: str, description: str) -> None:
    st.title(title)
    col_left, col_right = st.columns([4, 1])
    with col_left:
        st.caption(description)
    with col_right:
        if auth.is_authenticated():
            st.success(f"관리자: {auth.current_admin_name()}")
        else:
            st.info("읽기 전용")


def make_record_options(df: pd.DataFrame, id_column: str, label_columns: list[str]) -> list[tuple[str, str]]:
    options = [("신규", "신규 등록")]
    if df.empty:
        return options
    for _, row in df.iterrows():
        parts = [str(row[column]) for column in label_columns if column in df.columns and pd.notna(row[column]) and str(row[column]).strip()]
        options.append((str(row[id_column]), " | ".join(parts)))
    return options


def render_sidebar() -> str:
    with st.sidebar:
        st.title("우레탄 재공 관리")
        st.caption("품목별 재공 실사와 생산계획 조회")
        if auth.is_authenticated():
            st.success(f"관리자 인증됨: {auth.current_admin_name()}")
            if st.button("로그아웃", use_container_width=True):
                auth.logout()
                st.rerun()
        else:
            st.info("현재 공개 조회 모드")

        menu = st.radio(
            "메뉴",
            [
                "대시보드",
                "재공 현황 조회",
                "생산계획 / 부족분 조회",
                "외주 공유 화면",
                "관리자 전용: 오늘 재공 실사",
                "관리자 전용: 품목 마스터",
                "관리자 전용: 업체등록",
                "관리자 전용: 생산계획 관리",
            ],
        )
        st.caption("Backend: Supabase")
    return menu


def render_dashboard() -> None:
    metrics = services.get_dashboard_metrics()
    shortage_df = services.build_shortage_report()
    due_soon_df = services.get_due_soon_dashboard_report()
    inventory_summary = services.get_inventory_summary()

    st.title("대시보드")
    cols = st.columns(5)
    cols[0].metric("현재 재공", f"{metrics['total_wip_qty']:.0f}")
    cols[1].metric("사용 파레트 수", f"{metrics['total_pallet_count']}")
    cols[2].metric("계획 필요량", f"{metrics['total_required_qty']:.0f}")
    cols[3].metric("부족 품목 수", f"{metrics['shortage_item_count']}")
    cols[4].metric("미출 건수", f"{metrics['urgent_count']}")

    left, right = st.columns((3, 2))
    with left:
        st.subheader("D+2 부족/위험 품목")
        top_shortage = due_soon_df.head(15).copy()
        if top_shortage.empty:
            st.info("D+2 기준 부족/위험 품목이 없습니다.")
        else:
            top_shortage_view = top_shortage[["item_code", "color", "required_qty", "current_wip", "shortage_date", "due_date", "risk_level"]].copy()
            event = st.dataframe(
                top_shortage_view,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key="dashboard_due_soon_table",
            )
            selection = event.selection.rows if event and event.selection else []
            if selection:
                selected_row = top_shortage_view.iloc[selection[0]]
                render_plan_calendar_dialog(str(selected_row["item_code"]), str(selected_row["color"]))
    with right:
        st.subheader("품목별 현재 재공")
        inventory_summary_view = inventory_summary[inventory_summary["current_qty"] > 0].head(15).copy()
        if inventory_summary_view.empty:
            st.info("재공 데이터가 없습니다.")
        else:
            st.dataframe(
                inventory_summary_view[["item_code", "color", "current_qty", "pallet_count", "last_updated_at"]],
                use_container_width=True,
                hide_index=True,
            )

    st.subheader("품목-색상별 파레트 현황")
    if inventory_summary.empty:
        st.info("재공 데이터가 없습니다.")
    else:
        inventory_summary_view = inventory_summary[inventory_summary["current_qty"] > 0].copy()
        if inventory_summary_view.empty:
            st.info("현재 재공이 있는 품목이 없습니다.")
            return
        st.dataframe(
            inventory_summary_view[["item_code", "color", "current_qty", "pallet_count", "last_updated_at"]],
            use_container_width=True,
            hide_index=True,
        )

    risk_only = shortage_df[shortage_df["risk_level"].isin(["미출", "주의", "과입고"])].copy()
    st.subheader("오늘 기준 위험 품목")
    if risk_only.empty:
        st.success("주의/미출/과입고 품목이 없습니다.")
    else:
        st.dataframe(
            risk_only[["item_code", "color", "current_wip", "required_qty", "shortage_qty", "due_date", "risk_level"]],
            use_container_width=True,
            hide_index=True,
        )
        badges = " ".join(risk_only["risk_level"].dropna().astype(str).head(5).map(status_badge))
        st.markdown(badges, unsafe_allow_html=True)


def render_inventory_page() -> None:
    st.title("재공 현황 조회")
    detail = services.get_inventory_input_sheet().rename(columns={"record_id": "id"})
    if detail.empty:
        st.info("재공 데이터가 없습니다.")
        return

    cols = st.columns(3)
    selected_item = cols[0].selectbox("품목", ["전체"] + sorted(detail["item_code"].dropna().unique().tolist()))
    selected_color = cols[1].selectbox("색상", ["전체"] + sorted(detail["color"].dropna().astype(str).unique().tolist()))
    search_text = cols[2].text_input("검색", placeholder="품목, 색상, 품명, 비고")

    filtered = detail.copy()
    if selected_item != "전체":
        filtered = filtered[filtered["item_code"] == selected_item]
    if selected_color != "전체":
        filtered = filtered[filtered["color"] == selected_color]
    if search_text:
        search_mask = filtered.astype(str).apply(lambda col: col.str.contains(search_text, case=False, na=False))
        filtered = filtered[search_mask.any(axis=1)]

    summary_view = filtered.copy()
    st.subheader("품목별 현재 재공")
    st.dataframe(summary_view[["item_code", "item_name", "color", "current_qty", "current_pallet", "updated_at"]], use_container_width=True, hide_index=True)
    st.download_button("CSV 다운로드", services.to_csv_bytes(summary_view), "inventory_summary.csv", "text/csv")

    st.subheader("상세 재공 현황")
    detail_view = filtered.copy()
    st.dataframe(detail_view[["item_code", "item_name", "color", "current_qty", "current_pallet", "remark", "updated_at", "updated_by"]], use_container_width=True, hide_index=True)


def render_shortage_page() -> None:
    st.title("생산계획 / 부족분 조회")
    report = services.build_shortage_report()
    if report.empty:
        st.info("생산계획 데이터가 없습니다.")
        return

    left, right = st.columns(2)
    item_filter = left.selectbox("품목 필터", ["전체"] + sorted(report["item_code"].dropna().unique().tolist()))
    risk_filter = right.selectbox("위험도 필터", ["전체", "미출", "주의", "일정확인요망", "과입고", "정상"])

    filtered = report.copy()
    if item_filter != "전체":
        filtered = filtered[filtered["item_code"] == item_filter]
    if risk_filter != "전체":
        filtered = filtered[filtered["risk_level"] == risk_filter]

    st.caption("기본 계산식: 부족분 = 계획 필요량 - 현재 재공")
    st.dataframe(
        filtered[["item_code", "color", "current_wip", "required_qty", "shortage_qty", "due_date", "priority", "risk_level"]],
        use_container_width=True,
        hide_index=True,
    )
    st.download_button("부족분 CSV 다운로드", services.to_csv_bytes(filtered), "shortage_report.csv", "text/csv")


def _build_upload_result_message(summary: dict[str, int]) -> str:
    return (
        f"진행중 {summary['active_count']}건 반영, "
        f"삭제 {summary['deleted_count']}건 처리, "
        f"신규 {summary['inserted_count']}건, "
        f"업데이트 {summary['updated_count']}건"
    )


def render_vendor_share_page() -> None:
    st.title("외주 공유 화면")
    view = services.get_vendor_share_view()
    if view.empty:
        st.info("업체별로 매핑된 품목 현황이 없습니다.")
        return

    left, right = st.columns(2)
    vendor_filter = left.selectbox("업체 필터", ["전체"] + sorted(view["vendor_name"].dropna().unique().tolist()))
    risk_filter = right.selectbox("위험도 필터", ["전체"] + sorted(view["risk_level"].dropna().unique().tolist()))

    filtered = view.copy()
    if vendor_filter != "전체":
        filtered = filtered[filtered["vendor_name"] == vendor_filter]
    if risk_filter != "전체":
        filtered = filtered[filtered["risk_level"] == risk_filter]

    st.dataframe(
        filtered[["vendor_name", "item_code", "color", "due_date", "current_wip", "current_pallet", "required_qty", "required_pallet", "remaining_after_due", "shortage_qty", "shortage_pallet", "risk_level", "priority"]],
        use_container_width=True,
        hide_index=True,
    )
    st.download_button("외주 공유 CSV 다운로드", services.to_csv_bytes(filtered), "vendor_share.csv", "text/csv")


def require_admin(page_name: str) -> bool:
    if auth.is_authenticated():
        return True
    st.warning("관리자 전용 페이지입니다. 로그인 팝업에서 비밀번호를 입력해 주세요.")
    admin_login_dialog(page_name)
    return False


def render_admin_inventory() -> None:
    render_admin_intro("관리자 전용: 오늘 재공 실사", "품목별 실제 수량만 빠르게 입력하고 저장하는 현장용 화면입니다.")
    if not require_admin("오늘 재공 실사"):
        return

    inventory_df = services.get_inventory_input_sheet()
    items = services.get_items()
    if items.empty:
        st.error("품목 마스터가 비어 있습니다. 먼저 품목을 등록해 주세요.")
        return

    quick_add_buffer = st.session_state.get("inventory_quick_add_buffer")
    if quick_add_buffer:
        inventory_df = pd.DataFrame(quick_add_buffer)

    variant_df = services.get_item_variants()
    base_date = st.session_state.get("inventory_sheet_base_date", date.today())

    with st.container(border=True):
        st.subheader("신규 품목 빠른 추가")
        with st.form("inventory_quick_add_form"):
            item_codes = items["item_code"].tolist()
            c1, c2, c3 = st.columns(3)
            quick_item_code = c1.selectbox("품목코드", item_codes)
            color_options = sorted(variant_df[variant_df["item_code"] == quick_item_code]["color"].dropna().astype(str).unique().tolist()) if not variant_df.empty else []
            quick_color = c2.selectbox("색상", color_options) if color_options else c2.text_input("색상")
            quick_qty = c3.number_input("실사수량", min_value=0.0, value=0.0, step=1.0)
            quick_remark = st.text_input("비고")
            if st.form_submit_button("목록에 추가", use_container_width=True):
                item_name = items.loc[items["item_code"] == quick_item_code, "item_name"].iloc[0]
                candidate = pd.DataFrame(
                    [
                        {
                            "record_id": None,
                            "base_date": base_date.isoformat(),
                            "item_code": quick_item_code,
                            "item_name": item_name,
                            "color": quick_color,
                            "current_qty": 0.0,
                            "current_pallet": 0,
                            "remark": quick_remark,
                            "updated_at": "",
                            "updated_by": "",
                        }
                    ]
                )
                inventory_df = pd.concat([inventory_df, candidate], ignore_index=True).drop_duplicates(subset=["item_code", "color"], keep="last")
                inventory_df.loc[(inventory_df["item_code"] == quick_item_code) & (inventory_df["color"] == quick_color), "current_qty"] = quick_qty
                inventory_df.loc[(inventory_df["item_code"] == quick_item_code) & (inventory_df["color"] == quick_color), "current_pallet"] = services.calculate_pallets(quick_qty)
                st.session_state["inventory_quick_add_buffer"] = inventory_df.to_dict("records")
                st.success("신규 품목을 입력 목록에 추가했습니다.")
                st.rerun()

    with st.container(border=True):
        st.subheader("필터")
        f1, f2 = st.columns(2)
        item_filter = f1.selectbox("품목", ["전체"] + sorted(inventory_df["item_code"].dropna().astype(str).unique().tolist()))
        search_text = f2.text_input("빠른 검색", placeholder="품목코드, 색상")

    filtered_df = inventory_df.copy()
    if item_filter != "전체":
        filtered_df = filtered_df[filtered_df["item_code"] == item_filter]
    if search_text:
        search_mask = filtered_df.astype(str).apply(lambda col: col.str.contains(search_text, case=False, na=False))
        filtered_df = filtered_df[search_mask.any(axis=1)]
    if filtered_df.empty:
        st.info("조건에 맞는 입력 대상이 없습니다.")
        return

    st.caption("품목별 실제 수량만 입력하세요. 0도 정상 입력값입니다.")

    editable_df = filtered_df[
        [
            "record_id",
            "item_code",
            "color",
            "current_qty",
        ]
    ].copy()
    editable_df["실사수량"] = editable_df["current_qty"]

    edited_df = st.data_editor(
        editable_df,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=["record_id", "item_code", "color", "current_qty"],
        column_config={
            "record_id": None,
            "item_code": st.column_config.TextColumn("품목코드", width="small"),
            "color": st.column_config.TextColumn("색상", width="small"),
            "current_qty": st.column_config.NumberColumn("현재고", format="%.0f"),
            "실사수량": st.column_config.NumberColumn("실사수량", min_value=0, step=1, format="%.0f"),
        },
        key="inventory_sheet_editor",
    )

    with st.container(border=True):
        st.subheader("저장")
        s1, s2 = st.columns(2)
        base_date = s1.date_input("기준일", value=date.today(), key="inventory_sheet_base_date")
        updated_by = s2.text_input("수정자명", value=auth.current_admin_name(), key="inventory_sheet_user")

    if st.button("오늘 재공 실사 저장", use_container_width=True, type="primary"):
        if updated_by.strip() == "":
            st.error("수정자명을 입력해 주세요.")
        else:
            for _, row in edited_df.iterrows():
                services.save_inventory_snapshot(
                    base_date.isoformat(),
                    str(row["item_code"]),
                    str(row["color"]),
                    float(row["실사수량"]),
                    "",
                    updated_by,
                )
            st.session_state["inventory_quick_add_buffer"] = edited_df.assign(
                item_name="",
                current_qty=edited_df["실사수량"],
                current_pallet=edited_df["실사수량"].apply(services.calculate_pallets),
                remark="",
                updated_at="",
                updated_by=updated_by,
            )[["record_id", "item_code", "item_name", "color", "current_qty", "current_pallet", "remark", "updated_at", "updated_by"]].to_dict("records")
            st.success(f"{len(edited_df)}건의 재공 실사값을 저장했습니다.")
            st.rerun()


def render_admin_items() -> None:
    render_admin_intro("관리자 전용: 품목 마스터", "품목과 색상 기준정보를 분리해 관리할 수 있도록 구성했습니다.")
    if not require_admin("품목 마스터"):
        return

    items = services.get_items()
    variants = services.get_item_variants()
    tab_item, tab_variant, tab_list = st.tabs(["품목 등록", "색상 등록", "목록 보기"])

    with tab_item:
        with st.form("item_form"):
            c1, c2, c3 = st.columns(3)
            item_code = c1.text_input("품목코드")
            item_name = c2.text_input("품목명")
            category = c3.text_input("카테고리", value="우레탄")
            c4, c5, c6 = st.columns(3)
            spec = c4.text_input("규격", value="기본")
            unit = c5.text_input("단위", value="EA")
            active_yn = c6.selectbox("사용여부", ["Y", "N"])
            if st.form_submit_button("품목 저장", use_container_width=True):
                services.save_item(item_code, item_name or item_code, category, spec, unit, active_yn)
                st.success("품목 마스터가 저장되었습니다.")
                st.rerun()

    with tab_variant:
        with st.form("variant_form"):
            item_codes = items["item_code"].tolist() if not items.empty else []
            c1, c2, c3 = st.columns(3)
            variant_item_code = c1.selectbox("품목코드(색상 등록)", item_codes) if item_codes else c1.text_input("품목코드(색상 등록)")
            color = c2.text_input("색상")
            variant_active = c3.selectbox("활성여부", ["Y", "N"])
            if st.form_submit_button("색상 등록", use_container_width=True):
                services.save_item_variant(variant_item_code, color, variant_active)
                st.success("품목 색상이 저장되었습니다.")
                st.rerun()

    with tab_list:
        if not items.empty:
            delete_code = st.selectbox("삭제할 품목코드", ["선택안함"] + items["item_code"].tolist())
            if delete_code != "선택안함" and st.button("품목 삭제", type="secondary"):
                services.delete_by_code("items", "item_code", delete_code)
                st.success("품목이 삭제되었습니다.")
                st.rerun()
        search_text = st.text_input("품목 빠른 검색", placeholder="품목코드, 규격")
        filtered_items = items.copy()
        if search_text and not filtered_items.empty:
            search_mask = filtered_items.astype(str).apply(lambda col: col.str.contains(search_text, case=False, na=False))
            filtered_items = filtered_items[search_mask.any(axis=1)]
        st.subheader("품목 목록")
        st.dataframe(filtered_items[["item_code", "item_name", "category", "spec", "unit", "active_yn", "updated_at"]], use_container_width=True, hide_index=True)
        st.subheader("품목 색상 목록")
        st.dataframe(variants, use_container_width=True, hide_index=True)


def render_admin_vendors() -> None:
    render_admin_intro("관리자 전용: 업체등록", "품목-색상별 외주 업체 매핑을 등록하고 관리합니다.")
    if not require_admin("업체등록"):
        return

    items = services.get_items()
    variants = services.get_item_variants()
    vendor_map = services.get_item_vendor_map()
    if items.empty:
        st.error("품목 마스터가 비어 있습니다. 먼저 품목을 등록해 주세요.")
        return

    with st.form("vendor_form"):
        options = make_record_options(vendor_map, "id", ["vendor_name", "item_code", "color"])
        selected_label = st.selectbox("수정할 업체 매핑", options, format_func=lambda option: option[1])
        selected_id = selected_label[0]
        selected_row = vendor_map[vendor_map["id"].astype(str) == selected_id].iloc[0] if selected_id != "신규" else None
        item_codes = items["item_code"].tolist()
        c1, c2, c3 = st.columns(3)
        vendor_item_code = c1.selectbox(
            "품목코드",
            item_codes,
            index=item_codes.index(selected_row["item_code"]) if selected_row is not None and selected_row["item_code"] in item_codes else 0,
        )
        color_options = sorted(variants[variants["item_code"] == vendor_item_code]["color"].dropna().astype(str).unique().tolist()) if not variants.empty else []
        default_color = str(selected_row["color"]) if selected_row is not None and pd.notna(selected_row["color"]) else ""
        if default_color and default_color not in color_options:
            color_options = color_options + [default_color]
        vendor_color = c2.selectbox("색상", color_options, index=color_options.index(default_color) if default_color in color_options else 0) if color_options else c2.text_input("색상", value=default_color)
        vendor_name = c3.text_input("업체명", value=str(selected_row["vendor_name"]) if selected_row is not None else "")
        if st.form_submit_button("업체 매핑 저장", use_container_width=True):
            services.save_item_vendor_map(None if selected_id == "신규" else int(selected_id), vendor_item_code, vendor_color, vendor_name)
            st.success("업체 매핑을 저장했습니다.")
            st.rerun()

    st.subheader("업체 매핑 목록")
    if vendor_map.empty:
        st.info("등록된 업체 매핑이 없습니다.")
        return

    f1, f2 = st.columns(2)
    item_filter = f1.selectbox("품목별 필터", ["전체"] + sorted(vendor_map["item_code"].dropna().astype(str).unique().tolist()))
    vendor_filter = f2.selectbox("업체별 필터", ["전체"] + sorted(vendor_map["vendor_name"].dropna().astype(str).unique().tolist()))

    filtered_vendor_map = vendor_map.copy()
    if item_filter != "전체":
        filtered_vendor_map = filtered_vendor_map[filtered_vendor_map["item_code"] == item_filter]
    if vendor_filter != "전체":
        filtered_vendor_map = filtered_vendor_map[filtered_vendor_map["vendor_name"] == vendor_filter]

    if filtered_vendor_map.empty:
        st.info("필터 조건에 맞는 업체 매핑이 없습니다.")
        return

    delete_view = filtered_vendor_map.copy()
    delete_view["삭제선택"] = False
    selected_vendor_map = st.data_editor(
        delete_view[["삭제선택", "item_code", "color", "vendor_name", "updated_at", "id"]],
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "삭제선택": st.column_config.CheckboxColumn("선택"),
            "item_code": st.column_config.TextColumn("품목코드", width="small"),
            "color": st.column_config.TextColumn("색상", width="small"),
            "vendor_name": st.column_config.TextColumn("업체명", width="medium"),
            "updated_at": st.column_config.TextColumn("수정일시", width="medium"),
            "id": None,
        },
        disabled=["item_code", "color", "vendor_name", "updated_at", "id"],
        key="vendor_delete_editor",
    )

    delete_rows = selected_vendor_map[selected_vendor_map["삭제선택"]].copy()
    if st.button("선택한 업체 매핑 삭제", type="secondary", use_container_width=True):
        if delete_rows.empty:
            st.info("삭제할 업체 매핑을 선택해 주세요.")
        else:
            for record_id in delete_rows["id"].tolist():
                services.delete_by_id("item_vendor_map", int(record_id))
            st.success(f"{len(delete_rows)}건의 업체 매핑을 삭제했습니다.")
            st.rerun()


def render_admin_plans() -> None:
    render_admin_intro("관리자 전용: 생산계획 관리", "업로드, 수기수정, 계획 목록을 날짜 기준으로 관리합니다.")
    if not require_admin("생산계획 관리"):
        return

    plan_df = services.get_plan_detail()
    item_codes = services.get_items()["item_code"].tolist()
    tab_upload, tab_manual, tab_list = st.tabs(["엑셀 업로드", "수기 수정", "계획 목록"])

    with tab_upload:
        st.caption("업로드 규칙: 맨 앞 시트에서 `건명`에 `외주입고`가 포함된 행만 읽고, `부품이동카드번호` 기준으로 전체 동기화합니다.")
        uploaded = st.file_uploader("생산계획 파일 업로드", type=["xls", "xlsx", "csv"])
        updated_by = st.text_input("업로드 수정자명", value=auth.current_admin_name(), key="plan_upload_user")
        c1, c2 = st.columns(2)
        if c1.button("기본 가공실적등록.xls 다시 불러오기", use_container_width=True):
            if CONFIG.default_plan_path.exists():
                normalized = load_plan_from_path(CONFIG.default_plan_path)
                if normalized.empty:
                    st.error("기본 엑셀에서 조건에 맞는 외주입고 계획을 찾지 못했습니다.")
                    return
                summary = replace_production_plan(normalized, updated_by or auth.current_admin_name())
                st.success(_build_upload_result_message(summary))
                st.rerun()
            st.error("기본 엑셀 파일을 찾을 수 없습니다.")
        if c2.button("업로드 파일 반영", use_container_width=True, disabled=uploaded is None):
            normalized = load_plan_from_bytes(uploaded.getvalue(), uploaded.name)
            if normalized.empty:
                st.error("업로드 파일에서 조건에 맞는 외주입고 계획을 찾지 못했습니다.")
                return
            summary = replace_production_plan(normalized, updated_by or auth.current_admin_name())
            st.success(_build_upload_result_message(summary))
            st.rerun()

    with tab_manual:
        if not item_codes:
            st.info("업로드 후 품목 마스터가 자동 생성됩니다. 지금은 수기 수정 대상을 표시할 데이터가 없습니다.")
        else:
            options = make_record_options(plan_df, "id", ["item_code", "color", "plan_status", "due_date"])
            selected_label = st.selectbox("수정할 생산계획", options, format_func=lambda option: option[1])
            selected_id = selected_label[0]
            selected_row = plan_df[plan_df["id"].astype(str) == selected_id].iloc[0] if selected_id != "신규" else None
            with st.form("plan_form"):
                c1, c2, c3, c4 = st.columns(4)
                plan_date = c1.date_input("계획일", value=pd.to_datetime(selected_row["plan_date"]).date() if selected_row is not None else date.today())
                product_code = c2.text_input("제품코드", value=str(selected_row["product_code"]) if selected_row is not None else "")
                product_name = c3.text_input("제품명", value=str(selected_row["product_name"]) if selected_row is not None else "")
                item_code = c4.selectbox("우레탄 품목코드", item_codes, index=item_codes.index(selected_row["item_code"]) if selected_row is not None and selected_row["item_code"] in item_codes else 0)
                c5, c6, c7, c8 = st.columns(4)
                color = c5.text_input("색상", value=str(selected_row["color"]) if selected_row is not None else "")
                plan_qty = c6.number_input("계획량", min_value=0.0, value=float(selected_row["plan_qty"]) if selected_row is not None else 0.0, step=1.0)
                required_qty = c7.number_input("필요량", min_value=0.0, value=float(selected_row["required_qty"]) if selected_row is not None else 0.0, step=1.0)
                due_date = c8.date_input("최초포장일", value=pd.to_datetime(selected_row["due_date"]).date() if selected_row is not None else date.today())
                c9, c10, c11 = st.columns(3)
                priority_list = ["긴급", "높음", "보통"]
                priority = c9.selectbox("우선순위", priority_list, index=priority_list.index(selected_row["priority"]) if selected_row is not None and selected_row["priority"] in priority_list else 2)
                plan_status_list = ["진행중", "생산완료"]
                plan_status = c10.selectbox("생산상태", plan_status_list, index=plan_status_list.index(selected_row["plan_status"]) if selected_row is not None and selected_row["plan_status"] in plan_status_list else 0)
                share_yn = "Y"
                c11.markdown(f"**업체명**\n\n{selected_row['vendor_name'] if selected_row is not None and pd.notna(selected_row['vendor_name']) else '미매핑'}")
                updated_by = st.text_input("수정자명", value=auth.current_admin_name(), key="manual_plan_user")
                source_note = st.text_area("비고/원천 정보", value=str(selected_row["source_note"]) if selected_row is not None else "", height=80)
                if st.form_submit_button("생산계획 저장", use_container_width=True):
                    services.save_plan(None if selected_id == "신규" else int(selected_id), plan_date.isoformat(), product_code, product_name, item_code, color, plan_qty, required_qty, due_date.isoformat(), priority, plan_status, share_yn, source_note, updated_by)
                    st.success("생산계획이 저장되었습니다.")
                    st.rerun()
            if selected_id != "신규" and st.button("선택 계획 삭제", type="secondary"):
                services.delete_by_id("production_plan", int(selected_id))
                st.success("생산계획이 삭제되었습니다.")
                st.rerun()

    with tab_list:
        show_completed = st.checkbox("생산완료 포함", value=True)
        search_text = st.text_input("계획 빠른 검색", placeholder="품목코드, 색상, 상태, 제품코드, 최초포장일")
        filtered_plan = plan_df.copy()
        if not show_completed:
            filtered_plan = filtered_plan[filtered_plan["plan_status"] != "생산완료"]
        if search_text:
            search_mask = filtered_plan.astype(str).apply(lambda col: col.str.contains(search_text, case=False, na=False))
            filtered_plan = filtered_plan[search_mask.any(axis=1)]
        st.dataframe(filtered_plan, use_container_width=True, hide_index=True)


def main() -> None:
    init_app()
    inject_styles()
    selected_menu = render_sidebar()
    if selected_menu == "대시보드":
        render_dashboard()
    elif selected_menu == "재공 현황 조회":
        render_inventory_page()
    elif selected_menu == "생산계획 / 부족분 조회":
        render_shortage_page()
    elif selected_menu == "외주 공유 화면":
        render_vendor_share_page()
    elif selected_menu == "관리자 전용: 오늘 재공 실사":
        render_admin_inventory()
    elif selected_menu == "관리자 전용: 품목 마스터":
        render_admin_items()
    elif selected_menu == "관리자 전용: 업체등록":
        render_admin_vendors()
    elif selected_menu == "관리자 전용: 생산계획 관리":
        render_admin_plans()


if __name__ == "__main__":
    main()
