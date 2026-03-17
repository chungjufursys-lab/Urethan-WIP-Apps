# 05. 폴더 구조 제안

```text
Urethane_WIP_Manager/
├─ app.py
├─ requirements.txt
├─ .env
├─ data/
│  └─ urethane_wip.db
├─ docs/
│  └─ sdd/
├─ urethane_wip/
│  ├─ __init__.py
│  ├─ auth.py
│  ├─ config.py
│  ├─ data_loader.py
│  ├─ db.py
│  └─ services.py
└─ 공정진행정보.xls
```

## 분리 기준

- `app.py`: Streamlit UI 및 페이지 라우팅
- `config.py`: 설정값 및 환경변수 로딩
- `db.py`: SQLite 스키마/CRUD
- `data_loader.py`: 엑셀 파싱 및 샘플 데이터 적재
- `services.py`: 부족분/위험도/대시보드 계산
- `auth.py`: 세션 기반 관리자 인증
