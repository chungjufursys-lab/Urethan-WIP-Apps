# Supabase Migration Runbook

## Goal
- 운영 기준을 `정규화 부품명 + 색상`으로 맞춘다.
- 제품코드와 정규화 부품명의 관계를 `code_mapping` 테이블로 보존한다.
- 기존 `production_plan`, `items`, `item_variants`, `item_vendor_map`, `inventory`는 `item_code = 정규화 부품명` 의미로 재적재한다.

## Files
- SQL migration: [`migrations/20260317_create_code_mapping.sql`](/C:/Users/FURSYS/Desktop/Urethane_WIP_Manager/migrations/20260317_create_code_mapping.sql)
- Data rebuild script: [`scripts/rebuild_remote_state.py`](/C:/Users/FURSYS/Desktop/Urethane_WIP_Manager/scripts/rebuild_remote_state.py)

## Execution Order
1. Supabase SQL Editor에서 `migrations/20260317_create_code_mapping.sql`을 실행한다.
2. 로컬에서 `python scripts/rebuild_remote_state.py`를 실행한다.
3. 앱에서 기본 계획 다시 불러오기와 업체 매핑 화면을 점검한다.

## Notes
- 현재 환경에서는 Supabase DDL 자동 실행 권한을 확인하지 못해 SQL 파일을 직접 생성했다.
- 데이터 재적재 스크립트는 현재 `.env`의 `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`를 사용한다.
