---
name: integration-qa
description: 모듈 간 경계면 정합성을 교차 검증할 때 사용. SQS 메시지 계약(발행부↔소비부), DB 스키마↔쿼리, 모듈 함수 시그니처(정의↔호출), PII 마스킹 규칙 정렬을 양쪽에서 대조하고 ruff/pytest/import 스모크를 실행한다. 통합 검증·경계면 버그·계약 불일치·QA 작업 시 사용.
---

# Integration QA

통합 버그는 거의 항상 **컴포넌트 사이의 계약 불일치**에서 나온다. "파일이 있는가"가 아니라 **"양쪽이 맞물리는가"**를 검증한다. 한쪽만 읽지 말고 발행부와 소비부, 정의와 호출을 **동시에 열어 shape을 대조**한다.

## 점진적 실행
전체 완성 후 1회가 아니라, **각 모듈 완성 직후** 관련 경계면을 즉시 검증한다 — 늦게 발견할수록 원인 추적 비용이 커진다.

## 경계면 체크리스트

### 1. SQS 계약 (발행 ↔ 소비)
- `ocr-engineer`가 발행하는 `ReportJob`과 `report_worker`가 소비·역직렬화하는 모델이 **동일 스키마**인가.
- 실제 직렬화 바이트 ↔ `contracts.py` 정의 일치(필드명·타입·옵셔널). 큐 URL 상수 일치.
- 잘못된 페이로드가 삭제되지 않고 재전달되는가(poison 상한 초과 시에만 스킵). 결정적 실패(`NonRetryableError`)는 즉시 ack되는가. 핸들러가 멱등한가(같은 메시지 두 번 처리돼도 안전).
- `claim_id`가 있는 job은 fan-in 게이트(`claim_readiness`)를 거쳐 정확히 한 번만 `ReportJob`을 발행하는가 — 재전달로 두 번 종결 카운트되지 않는가(구조적 멱등, `sqs-worker-patterns` 참고).

### 2. DB 스키마 ↔ 코드
- 마이그레이션 컬럼·인덱스(HNSW·pg_trgm·tsvector)와 실제 쿼리 일치.
- **임베딩 차원 1024 고정**이 마이그레이션·ai_client·RAG에서 모두 일치하는가(불일치 시 검색이 조용히 망가짐).
- ocr_results 삭제 트리거·보존정책이 스키마에 반영됐는가.
- 마이그레이션이 자기 소유 스키마(`ai`/`core`/`corpus`)만 건드리는가(CODE_CONVENTIONS §14) — 다른 owner 오브젝트에 DDL을 내면 워커 기동이 권한 에러로 막힌다.

### 3. 모듈 함수 시그니처 (정의 ↔ 호출)
- `aicore-engineer`가 공지한 RAG·가드레일 공개 API(`_workspace/02_aicore_api.md`)와 `agent-engineer` 호출부의 인자·반환 shape 일치.
- RAG 반환(`ranked_chunks + citations`)을 리포트·챗봇이 기대한 형태로 쓰는가.

### 4. PII 마스킹 정렬
- OCR 입력단과 가드레일 입력단의 마스킹 규칙(주민번호 앞 6자리 보존 등)이 모순되지 않는가.

## 실행 검증
- `ruff check` + `ruff format --check`.
- `uv run python -c "import ..."` 스모크(순환 import·누락 탐지).
- `pytest` — 특히 경계 계약 테스트.
- 가능하면 `docker compose up`으로 LocalStack(SQS)·PG를 띄워 end-to-end. Ollama는 GPU가 필요해 로컬 compose에 없으므로 별도 확인. 불가 시 정적 교차 비교로 대체하고 한계 명시.
- 경계 계약 테스트가 실제로 회귀를 잡는지 의심스러우면 **뮤테이션 테스트**로 확인한다(관련 로직을 일부러 깨서 테스트가 실패하는지 본다) — 통과만 하고 아무것도 못 잡는 테스트는 없느니만 못하다.

## 보고
`_workspace/99_qa_report.md`에 경계면별 PASS/FAIL, 발견 버그(**위치·원인·어느 쪽을 고쳐야 하는지**), 재검증 결과를 기록한다. 버그는 책임 에이전트에 구체적 수정 요청으로 돌리고, 수정 후 재검증한다. 상충 데이터는 삭제하지 않고 출처를 병기한다.

## 반복 스크립트 번들링
모든 검증에서 공통으로 쓰는 헬퍼(import 스모크, 계약 비교)는 `scripts/`에 번들링해 재사용한다.
