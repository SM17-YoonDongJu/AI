---
name: python-worker-scaffolding
description: 보험·법률 AI 엔진 Python 프로젝트의 토대를 세울 때 사용. uv 초기화, src 레이아웃, pyproject.toml(의존성 그룹 분리), ruff 설정, core/(config·kafka·db·ai_client·contracts·logging), docker-compose.dev(Kafka·PG+pgvector·Ollama·Redis), 마이그레이션 골격을 만든다. 프로젝트 초기 구성·스캐폴딩·"환경 구축"·core 모듈 작업 시 반드시 사용.
---

# Python Worker Scaffolding

보험·법률 AI 엔진의 **공통 토대**를 세운다. 모든 워커·API가 이 위에 올라가므로, 여기서 정한 컨벤션과 계약이 전체 기준이 된다. 작업 전 반드시 `.claude/CODE_CONVENTIONS.md`를 읽고 따른다.

## 구축 순서 (의존 역순)
1. **프로젝트 골격** → 2. **config** → 3. **contracts** → 4. **db/kafka/ai_client 래퍼** → 5. **docker-compose + 마이그레이션**

먼저 동작하는 최소 골격(import 가능, 풀 생성/해제)을 완성하고, 세부는 호출자 요구에 맞춰 채운다. 과도한 선구현을 피한다 — 컨텍스트는 공공재다.

## 1. 프로젝트 골격
- `uv init` 기반, `pyproject.toml`만 사용(`setup.py` 금지), `uv.lock` 커밋.
- `src/` 레이아웃: `src/core`, `src/rag`, `src/guardrail`, `src/ocr`, `src/report`, `src/chatbot`, `src/workers`, `src/api`, `tests/`.
- 의존성 **그룹 분리** — 워커별 슬림 배포를 위해:
  - 기본: pydantic, pydantic-settings, aiokafka, asyncpg, redis, ollama, structlog
  - `ocr`(optional): paddleocr, paddlepaddle-gpu, 한국어 NER
  - `report`(optional): langgraph, langchain-core
  - `chatbot`(optional): fastapi, uvicorn, websockets, python-jose(JWT)
  - `dev`: ruff, pytest, pytest-asyncio
- `ruff.toml`: line-length 100, target py312, isort 활성.

## 2. config — `src/core/config.py`
- `pydantic-settings`의 `BaseSettings`. 모든 환경값을 한 곳에. `os.getenv` 산재 금지.
- 그룹: Kafka(bootstrap·토픽명), DB(dsn·pool size), Ollama(base_url·임베딩/LLM 모델명), Redis, JWT(공개키), S3.

## 3. contracts — `src/core/contracts.py` (가장 중요)
Spring과의 인터페이스 계약. 전부 pydantic v2 `BaseModel`. 확정 즉시 팀에 공유한다.
- `OcrJob` (consume `ocr-job-queue`): job_id, s3_key, doc_type_hint, uploaded_at …
- `ReportJob` (produce/consume `report-job`): report_id, ocr_result_id, user_ref …
- WebSocket 메시지: `ChatClientMessage`(session_id, text), `ChatServerMessage`(type="message", text, citations) — **비스트리밍**(stream/done 없음).
- 토픽명 상수도 여기서 export.

## 4. 인프라 래퍼 — `src/core/`
- `kafka/`: aiokafka consumer 래퍼(자동 오프셋 커밋 전략·역직렬화→pydantic·핸들러 콜백·에러 시 DLQ), producer 래퍼.
- `db.py`: asyncpg 풀 생성/해제, `async with` 컨텍스트. pgvector 등록.
- `ai_client.py`: Ollama HTTP(qwen3:embedding 1024d, EXAONE). 임베딩 실패 시 BGE-M3(sentence-transformers) 폴백. 차원 1024 고정.
- `logging.py`: structlog 구조적 로깅, job_id/session_id/correlation_id 바인딩, **PII 로깅 금지**.

## 5. 로컬 환경 + 마이그레이션
- `docker-compose.dev.yml`: redpanda(경량 Kafka)·postgres(pgvector 이미지)·ollama·redis. Spring 없이 워커 단독 테스트용.
- `migrations/`: `ocr_results`, `search_terms`(pg_trgm 인덱스), 청크 테이블(tsvector 인덱스 + `embedding vector(1024)` HNSW), 대화 이력.

## 검증
- `uv run python -c "import src.core.contracts"` 스모크 통과.
- `ruff check` + `ruff format --check` 통과.
- docker-compose up 후 db/kafka/ollama 헬스 확인.

## 산출물
`src/core/*`, `pyproject.toml`, `ruff.toml`, `docker-compose.dev.yml`, `migrations/*`. contracts 스키마 목록을 `_workspace/00_platform_contracts.md`에 기록하고 팀에 공유한다.
