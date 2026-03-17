# 04. DB 테이블 구조

## 핵심 테이블

### items

- `item_code` TEXT PK
- `item_name` TEXT
- `category` TEXT
- `spec` TEXT
- `unit` TEXT
- `active_yn` TEXT
- `updated_at` TEXT

### item_variants

- `id` INTEGER PK
- `item_code` TEXT FK
- `color` TEXT
- `display_name` TEXT
- `active_yn` TEXT
- `updated_at` TEXT

### locations

- `location_code` TEXT PK
- `location_name` TEXT
- `area_type` TEXT
- `capacity` REAL
- `use_yn` TEXT
- `updated_at` TEXT

### inventory

- `id` INTEGER PK
- `base_date` TEXT
- `item_code` TEXT
- `color` TEXT
- `qty` REAL
- `location_code` TEXT
- `status` TEXT
- `remark` TEXT
- `updated_by` TEXT
- `updated_at` TEXT

### production_plan

- `id` INTEGER PK
- `plan_date` TEXT
- `product_code` TEXT
- `product_name` TEXT
- `urethane_item_code` TEXT
- `color` TEXT
- `plan_qty` REAL
- `required_qty` REAL
- `due_date` TEXT
- `priority` TEXT
- `share_yn` TEXT
- `source_note` TEXT
- `updated_by` TEXT
- `updated_at` TEXT

### vendor_share

- `id` INTEGER PK
- `vendor_name` TEXT
- `item_code` TEXT
- `color` TEXT
- `request_qty` REAL
- `due_date` TEXT
- `status` TEXT
- `remark` TEXT
- `updated_at` TEXT

### audit_log

- `id` INTEGER PK
- `table_name` TEXT
- `record_id` TEXT
- `action` TEXT
- `changed_by` TEXT
- `changed_at` TEXT
- `change_summary` TEXT

## 계산용 뷰 개념

- 재공 집계: `inventory`를 품목/색상 기준으로 합산
- 계획 집계: `production_plan`을 품목/색상 기준으로 합산
- 부족분: `required_qty - current_qty`
- 위험도: 부족분 > 0 이고 납기일까지 남은 일수 기준으로 `긴급`, `주의`, `정상`
