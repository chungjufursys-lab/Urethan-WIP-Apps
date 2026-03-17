# 우레탄 재공 관리 툴 MVP

Streamlit + SQLite 기반의 우레탄 재공 운영보드입니다. 공개 조회 화면과 관리자 수정 화면을 분리했고, `공정진행정보.xls`를 읽어 우레탄 생산계획/외주입고 기준 데이터를 초기 적재합니다.

## 실행 방법

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## 기본 설정

- 관리자 비밀번호: `.env`의 `URETHANE_ADMIN_PASSWORD`
- SQLite 파일: `.env`의 `URETHANE_DB_PATH`
- 기본 생산계획 파일: 프로젝트 루트의 `공정진행정보.xls`

`.env`의 기본 비밀번호는 샘플값이므로 실제 운영 전 반드시 변경해야 합니다.

## 주요 기능

- 공개 대시보드
- 재공 현황 조회 / 필터 / CSV 다운로드
- 생산계획 대비 부족분 조회 / 위험도 표시
- 외주 공유 화면
- 관리자 비밀번호 인증 / 로그아웃
- 재공 입력관리
- 품목 / 색상 / 위치 마스터 관리
- 생산계획 수기 입력 및 엑셀 업로드

## 설계 문서

MVP 요구사항과 구조는 `docs/sdd/` 아래 문서에 정리했습니다.
