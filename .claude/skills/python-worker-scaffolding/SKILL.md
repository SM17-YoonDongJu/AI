---
name: python-worker-scaffolding
description: 보험·법률 AI 엔진 Python 프로젝트의 토대를 세울 때 사용. uv 초기화, src 레이아웃, pyproject.toml(의존성 그룹 분리), ruff 설정, core/(config·sqs·db·ai_client·contracts·logging), docker-compose(LocalStack SQS·PG+pgvector·Redis), 마이그레이션 골격을 만든다. 프로젝트 초기 구성·스캐폴딩·"환경 구축"·core 모듈 작업 시 반드시 사용.
---

# Python Worker Scaffolding

보험·법률 AI 엔진의 **공통 토대**를 세운다. 모든 워커·API가 이 위에 올라가므로, 여기서 정한 컨벤션과 계약이 전체 기준이 된다. 작업 전 반드시 `.claude/CODE_CONVENTIONS.md`를 읽고 따른다.

## 구축 순서 (의존 역순)
1. **프로젝트 골격** → 2. **config** → 3. **contracts** → 4. **db/sqs/ai_client 래퍼** → 5. **docker-compose + 마이그레이션**

먼저 동작하는 최소 골격(import 가능, 풀 생성/해제)을 완성하고, 세부는 호출자 요구에 맞춰 채운다. 과도한 선구현을 피한다 — 컨텍스트는 공공재다.

## 1. 프로젝트 골격
- `uv init` 기반, `pyproject.toml`만 사용(`setup.py` 금지), `uv.lock` 커밋.
- `src/` 레이아웃(워커중심): `src/core`, `src/rag`, `src/guardrail`, `src/ocr_worker`, `src/report_worker`, `src/chatbot`, `tests/`. 워커 진입점은 각 패키지의 `__main__.py`(챗봇은 `app.py`).
- 의존성 **그룹 분리** — 워커별 슬림 배포를 위해:
  - 기본: pydantic, pydantic-settings, asyncpg, httpx(ai_client), structlog
  - `ocr`(optional): surya-ocr, transformers, pillow, boto3(S3·SQS)
  - `report`(optional): langgraph, langchain-core, boto3(SQS)
  - `chatbot`(optional): fastapi, uvicorn, websockets, python-jose(JWT), redis
  - `dev`: ruff, pytest, pytest-asyncio
- `ruff.toml`: line-length 100, target py312, isort 활성.

## 2. config — `src/core/config.py`
- `pydantic-settings`의 `BaseSettings`. 모든 환경값을 한 곳에. `os.getenv` 산재 금지.
- 그룹: SQS(큐 URL·`sqs_endpoint_url`·`sqs_max_receive_count`·`sqs_visibility_timeout` 등), DB(dsn·pool size), Ollama(base_url·임베딩/LLM 모델명·`num_ctx` — VLM 등 프롬프트가 커질 수 있는 호출은 명시적으로 지정), Redis, JWT(공개키), S3.

## 3. contracts — `src/core/contracts.py` (가장 중요)
Spring과의 인터페이스 계약. 전부 pydantic v2 `BaseModel`. 확정 즉시 팀에 공유한다.
- `OcrJob` (consume `ocr-job-queue`): job_id, s3_key, doc_type_hint, claim_id, uploaded_at …
- `ReportJob` (produce/consume `report-job`): report_id, ocr_result_id, claim_id, user_ref …
- WebSocket 메시지: `ChatClientMessage`(session_id, text), `ChatServerMessage`(type="message", text, citations) — **비스트리밍**(stream/done 없음).
- 큐 URL 관련 상수는 config에서, 메시지 스키마는 여기서 export.

## 4. 인프라 래퍼 — `src/core/`
- `sqs/`: boto3(동기 SDK, `asyncio.to_thread`로 격리) 기반 `SqsConsumer`(롱폴링·역직렬화→pydantic·`NonRetryableError` 즉시 ack·poison 가드·`on_poison` 훅), `SqsProducer`. DLQ는 아직 없다 — poison 가드가 대체(`sqs-worker-patterns` 참고).
- `db.py`: asyncpg 풀 생성/해제, `async with` 컨텍스트. pgvector 등록.
- `exceptions.py`: `AppError` 도메인 예외 계층 + `NonRetryableError` 마커(재전달해도 결과가 같은 결정적 실패).
- `ai_client.py`: Ollama HTTP(qwen3-embedding 1024d, Qwen3 MoE LLM). 임베딩 실패 시 BGE-M3(sentence-transformers) 폴백. 차원 1024 고정.
- `logging.py`: structlog 구조적 로깅, job_id/session_id/correlation_id 바인딩, **PII 로깅 금지**(예외 메시지 본문도 포함 — 타입명만 로깅).

## 5. 로컬 환경 + 마이그레이션
- `docker-compose.yml`: LocalStack(SQS)·postgres(pgvector 이미지)·redis. Ollama는 GPU가 필요해 로컬 compose에 없다(별도 기동 또는 원격 GPU 노드 접속). Spring 없이 워커 단독 테스트용.
- `migrations/{ai,core,corpus}/`: 스키마 소유자별 분리(CODE_CONVENTIONS §14) — 자기 소유 스키마만 DDL을 낸다. `ocr_results`, `claim_readiness`, `ocr_job_failures`, `search_terms`(pg_trgm 인덱스), 청크 테이블(tsvector 인덱스 + `embedding vector(1024)` HNSW), 대화 이력.

## 검증
- `uv run python -c "import core.contracts"` 스모크 통과 (PYTHONPATH=src).
- `ruff check` + `ruff format --check` 통과.
- `docker compose up` 후 db/localstack 헬스 확인(Ollama는 별도).

## 산출물
`src/core/*`, `pyproject.toml`, `ruff.toml`, `docker-compose.yml`, `migrations/*`. contracts 스키마 목록을 `_workspace/00_platform_contracts.md`에 기록하고 팀에 공유한다.
