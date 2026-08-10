# AI Engine — 보험·법률 AI 처리 엔진

보험·법률 문서를 처리하는 Python AI 엔진. **문서 OCR → Hybrid RAG 검색 → LangGraph 멀티에이전트 리포트 생성 → 가드레일 → 상담 챗봇**으로 구성된다. 파일 수신·JWT·S3 저장·SQS 발행을 담당하는 **Spring Boot 게이트웨이는 별도 레포(범위 밖)**다.

## 아키텍처

```
Spring Boot (게이트웨이, 별도)            Python AI 엔진 (이 레포)
──────────────────────────             ──────────────────────────────
업로드/JWT/S3/SQS 발행        ──SQS───▶  ocr_worker     (02, GPU 노드)
                                         report_worker  (05, 범용 노드)
Frontend ──WS 직결(/ws/chat)──────────▶  chatbot        (12, 범용 노드)
                                         └ 공용: core · rag(04) · guardrail(06)
                                              │
                              ┌───────────────┘ (HTTP, OpenAI 호환)
                              ▼
                  외부 GPU 노드: Ollama/vLLM [ LLM + 임베딩 ]  (모델 미정)
```

- **OCR(02)·리포트(05)** = SQS 워커(비동기)
- **챗봇(12)** = FastAPI WebSocket 직결 (**비스트리밍**, 완성 응답 1회)
- **RAG(04)·가드레일(06)** = 공용 Python 모듈 (워커가 import해서 조립)
- **LLM·임베딩** = 외부 GPU 노드의 **OpenAI 호환 서버(Ollama/vLLM)** HTTP 호출. **모델 미정** → `ai_client`가 config(`base_url`/`model`)로만 다룸
- **관리형**: DB = AWS RDS(PostgreSQL + pgvector), 메시지 큐 = AWS SQS, Redis = ElastiCache

## 패키지 구조 & 담당

| 패키지 | 역할 | 노드 | 담당 | 상세 |
|---|---|---|---|---|
| `src/core` | 공용 인프라(설정·**계약**·sqs·db·ai_client·로깅) | — | _TBD_ | [README](src/core/README.md) |
| `src/rag` | Hybrid RAG 검색 (04) | (호출자에 포함) | _TBD_ | [README](src/rag/README.md) |
| `src/guardrail` | 입력/생성/출력 가드레일 (06) | (호출자에 포함) | _TBD_ | [README](src/guardrail/README.md) |
| `src/ocr_worker` | OCR SQS 워커 (02) | **GPU** | _TBD_ | [README](src/ocr_worker/README.md) |
| `src/report_worker` | LangGraph 리포트 워커 (05) | 범용 | _TBD_ | [README](src/report_worker/README.md) |
| `src/chatbot` | 챗봇 FastAPI WS (12) | 범용 | _TBD_ | [README](src/chatbot/README.md) |

## 기술 스택

Python 3.12 · **uv** · ruff · mypy · pytest · boto3(SQS) · asyncpg + pgvector · FastAPI · structlog

## 개발 환경 설정

```bash
# 1. uv 설치 (https://docs.astral.sh/uv/)
# 2. 의존성 설치 (개발용: base + dev, 필요한 워커 extra 추가)
uv sync                       # base + dev
uv sync --extra report        # + report 워커 의존성
uv sync --all-extras          # 전부 (주의: ocr는 paddlepaddle-gpu → Linux+CUDA 전용)

# 3. 로컬 백킹 서비스 기동 (LocalStack SQS·PostgreSQL·Redis)
docker compose up -d

# 4. .env 작성 (DATABASE_URL, SQS_OCR_JOB_QUEUE_URL·SQS_ENDPOINT_URL, AI_BASE_URL 등)
```

> DB는 로컬에서도 RDS를 쓰거나 별도 PostgreSQL+pgvector를 띄운다(현재 compose엔 미포함).

## 실행 방법

> 엔트리포인트(`__main__.py`/`app.py`)는 코드 구현 단계에서 추가된다. 아래는 실행 흐름이다.

### A. 로컬 (소스로 직접 실행)

```bash
# 0) 사전 준비
uv sync                       # 의존성 설치 (base + dev). 워커별: --extra ocr|report|chatbot
cp .env.example .env          # 환경변수 작성 (DATABASE_URL, SQS_..._QUEUE_URL, AI_BASE_URL 등)
docker compose up -d          # 백킹 서비스(LocalStack SQS·PostgreSQL·Redis) 기동

# 1) 워커 실행 (각각 별도 터미널)
uv run python -m ocr_worker                          # OCR 워커 (GPU 필요)
uv run python -m report_worker                       # 리포트 워커
uv run uvicorn chatbot.app:app --reload --port 8000  # 챗봇 → http://localhost:8000
```

> `uv run`은 `.venv`(PYTHONPATH=src 포함)에서 실행한다. uv 없이 돌리려면 `PYTHONPATH=src python -m ocr_worker`.
> 챗봇 헬스체크: `curl localhost:8000/health`

### B. Docker (워커별 이미지, 배포 형태)

```bash
# 빌드 (build context = 레포 루트)
docker build -f src/ocr_worker/Dockerfile    -t ocr-worker .      # CUDA 베이스, GPU 노드
docker build -f src/report_worker/Dockerfile -t report-worker .   # slim, 범용 노드
docker build -f src/chatbot/Dockerfile       -t chatbot .         # slim, 범용 노드

# 실행 (env로 RDS·SQS·ElastiCache·외부 Ollama/vLLM 주입)
docker run --env-file .env report-worker
docker run --env-file .env -p 8000:8000 chatbot
docker run --gpus all --env-file .env ocr-worker                  # GPU 노드
```

> prod은 노드별 docker-compose로 위 이미지를 띄우고, 백킹 서비스는 AWS 관리형 + 외부 GPU 노드를 env로 연결한다.

## 품질

```bash
uv run ruff check . && uv run ruff format .   # 린트·포맷
uv run mypy src                                # 타입 체크
uv run pytest                                  # 테스트
```

코드 컨벤션: **[.claude/CODE_CONVENTIONS.md](.claude/CODE_CONVENTIONS.md)** (전원 준수)

## 병렬 개발 가이드 (팀원 간 분리 개발)

1. **통합 경계 = `src/core/contracts.py`** (SQS 큐 페이로드 + WebSocket 메시지). **먼저 합의·고정**한 뒤 각 패키지를 병렬 개발한다. 계약이 어긋나면 워커 간 메시지가 깨진다. → 상세: **[.claude/docs/contracts.md](.claude/docs/contracts.md)**, 챗봇 이벤트는 **[.claude/docs/chatbot-events.md](.claude/docs/chatbot-events.md)**
2. 각자 **패키지 README의 입력/출력(계약)에 맞춰** 구현하고, 공용 모듈(`rag`/`guardrail`)은 함수 시그니처를 합의한다.
3. `core`는 다른 모두가 의존하므로 **우선 안정화**한다.
4. 브랜치: `dev` 기반 작업 → PR. (PR 메시지는 `/pr-writer` 스킬로 생성 가능)

## 배포

- **워커별 Dockerfile**(`src/<worker>/Dockerfile`)로 이미지 빌드 → **노드별 compose**로 실행.
  - `ocr_worker` = CUDA 베이스, **GPU 노드** / `report_worker`·`chatbot` = slim, **범용 노드**
- 백킹 서비스는 AWS 관리형(RDS·SQS·ElastiCache) + 외부 GPU 노드(Ollama/vLLM)를 **env로 주입**.
- 빌드 예: `docker build -f src/report_worker/Dockerfile -t report-worker .`

## 문서

- **인터페이스 계약(워커 간)**: **[.claude/docs/contracts.md](.claude/docs/contracts.md)** ← 병렬 개발 기준
- **챗봇 이벤트 명세**: **[.claude/docs/chatbot-events.md](.claude/docs/chatbot-events.md)**
- 시스템 아키텍처(Notion 미러): **[.claude/docs](.claude/docs)** (02 OCR / 04 RAG / 05 리포트 / 06 가드레일 / 12 챗봇)
- 코드 컨벤션: **[.claude/CODE_CONVENTIONS.md](.claude/CODE_CONVENTIONS.md)**
