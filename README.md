# AI Engine — 보험·법률 AI 처리 엔진

> 보험 청구 서류를 받아 **OCR → PII 마스킹 → Hybrid RAG → LangGraph 멀티에이전트 → 가드레일**을 거쳐 보상 분석 리포트를 만드는 Python 엔진.

[![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?logo=githubactions&logoColor=white)](.github/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![uv](https://img.shields.io/badge/deps-uv-DE5FE9)
![ruff](https://img.shields.io/badge/lint-ruff-D7FF64)

파일 수신·JWT·S3 저장·SQS 발행은 **Spring Boot 게이트웨이(별도 레포)** 가 맡고, 이 레포는 그 뒤의 AI 처리만 담당한다.

---

## 목차

- [아키텍처](#아키텍처)
- [모듈 맵](#모듈-맵)
- [퀵스타트](#퀵스타트)
- [메시지 계약 & 상태 흐름](#메시지-계약--상태-흐름)
- [DB 스키마 & 마이그레이션](#db-스키마--마이그레이션)
- [테스트 & 품질](#테스트--품질)
- [배포](#배포)
- [문서](#문서)

---

## 아키텍처

```
Spring Boot gateway  — 별도 레포: 업로드 · JWT · S3 PutObject
   │
   │  SQS ocr-job-queue  — 문서 1건 = 1메시지
   ▼
ocr_worker (02, GPU)
   │  S3 원본 → surya-ocr (+VLM 표 보조) → 분류·추출
   │  → PII 마스킹 (텍스트+이미지) → ai.ocr_results
   │  → 청구 fan-in 판정 → 청구당 1건 발행
   │
   │  SQS report-job
   ▼
report_worker (05, 범용)
      LangGraph: 진단 → 약관·담보 → 판례 → 후유장해
      → 지급액 계산 (순수 함수) → 리포트 → 가드레일

corpus_worker (상시 데몬)
      Notion 약관 카탈로그 → PG 미러 동기화 + 원문 S3 스테이징

core · rag (04) · guardrail (06)  — 공용 모듈, 워커가 import
   │
   │  HTTP (OpenAI 호환)
   ▼
Ollama / vLLM  — 외부 GPU 노드: LLM · 임베딩 · VLM
```

| 구분 | 방식 | 비고 |
|---|---|---|
| OCR·리포트 | SQS 워커 (at-least-once) | `job_id`/`report_id` 멱등, poison 가드, 결정적 실패 즉시 종결 |
| RAG·가드레일 | 공용 Python 모듈 | 워커가 import해서 조립 |
| LLM·임베딩 | 외부 Ollama/vLLM HTTP 호출 | 모델은 config(`LLM_MODEL`·`EMBEDDING_MODEL`)로만 지정 |
| 인프라 | RDS(PostgreSQL+pgvector) · SQS · S3 | 로컬은 docker-compose(LocalStack·pgvector) |

> 돈에 직결되는 숫자(장해 지급률·예상 보상금)는 LLM이 아니라 **정해진 계산 규칙(순수 함수)** 이 만든다. LLM은 분류·해석만 맡는다.

## 모듈 맵

| 패키지 | 역할 | 실행 형태 | 상세 |
|---|---|---|---|
| [`src/core`](src/core) | 설정·**계약**(`contracts.py`)·SQS 래퍼·asyncpg 풀·`ai_client`·암호화·로깅 | 라이브러리 | [README](src/core/README.md) |
| [`src/rag`](src/rag) | Hybrid RAG: 쿼리 라우터 → trigram 오타보정 → tsvector + pgvector → RRF → 인용 역추적 | 라이브러리 | [README](src/rag/README.md) |
| [`src/guardrail`](src/guardrail) | 입력(PII·도메인 차단) / 생성(단정 표현 치환·인용 강제) / 출력(고지문·LLM Judge) | 라이브러리 | [README](src/guardrail/README.md) |
| [`src/ocr_worker`](src/ocr_worker) | `ocr-job-queue` 소비 → OCR·마스킹·저장 → 청구 fan-in → `report-job` 발행 | `python -m ocr_worker` (GPU) | [README](src/ocr_worker/README.md) |
| [`src/report_worker`](src/report_worker) | `report-job` 소비 → LangGraph 멀티에이전트 리포트 생성 | `python -m report_worker` | [README](src/report_worker/README.md) |
| [`src/corpus_worker`](src/corpus_worker) | Notion 약관 카탈로그 → PG 미러 동기화 + 원문 S3 스테이징 (상시 데몬) | `python -m corpus_worker` | [README](src/corpus_worker/README.md) |

<details>
<summary>리포트 그래프 노드 (<code>src/report_worker/graph.py</code>)</summary>

```
load_context → input_guardrail ─┬─▶ persist_blocked                         (1)
                                └─▶ diagnosis ─┬─▶ terms_parse ─┐           (2)
                                               └────────────────┴─▶ coverage_parse → coverage_analysis
                                    → case_search ─┬─▶ disability_rag → disability_verify → disability_calc ─┐
                                                   └─────────────────────────────────────────────────────────┤  (3)
                                    payment_calc ◀───────────────────────────────────────────────────────────┘
                                    → report_compose → output_guardrail → persist
```

1. 보험·법률 외 질문이거나 PII 복호화에 실패하면 `reports.status=BLOCKED`만 기록하고 종료
2. 약관이 `policy_chunks`에 없을 때만 런타임 파싱
3. 후유장해 검토가 필요 없으면 `payment_calc`로 바로 이동

상세: [docs/architecture/langgraph.md](docs/architecture/langgraph.md)
</details>

## 퀵스타트

**요구사항:** Python 3.12, [uv](https://docs.astral.sh/uv/), Docker. OCR 워커는 Linux와 CUDA GPU가 필요하다(surya-ocr / PyTorch).

```bash
# 1) 의존성 — base + dev, 필요한 워커 extra만 추가
uv sync                          # base + dev
uv sync --extra report           # 리포트 워커
uv sync --extra ocr              # OCR 워커 (GPU 노드)

# 2) 로컬 백킹 서비스 — LocalStack(SQS, 큐 자동 생성) · PostgreSQL+pgvector
docker compose up -d

# 3) 환경변수
cp .env.example .env             # DATABASE_URL · SQS_*_QUEUE_URL · SQS_ENDPOINT_URL · AI_BASE_URL · LLM_MODEL ...

# 4) 워커 실행 (각각 별도 터미널)
uv run python -m ocr_worker      # 기동 시 migrations/ai 자동 적용
uv run python -m report_worker
uv run python -m corpus_worker   # 기동 시 migrations/corpus 자동 적용 (NOTION_TOKEN 필요)
```

<details>
<summary>샘플 메시지 발행해 보기 (LocalStack)</summary>

```bash
aws --endpoint-url http://localhost:4566 sqs send-message \
  --queue-url http://localhost:4566/000000000000/ocr-job-queue \
  --message-body '{
    "job_id": "00000000-0000-0000-0000-000000000001",
    "s3_key": "uploads/sample.pdf",
    "content_type": "application/pdf",
    "user_ref": "u-1",
    "report_id": "00000000-0000-0000-0000-0000000000a1",
    "attachment_id": "00000000-0000-0000-0000-0000000000b1",
    "doc_index": 1, "doc_total": 1,
    "uploaded_at": "2026-01-01T00:00:00Z"
  }'
```

전체 흐름 데모: `scripts/demo_pipeline.py`
</details>

> `uv run`은 `pythonpath=src`를 잡아 준다. uv 없이 실행할 때는 `PYTHONPATH=src python -m <worker>`로 실행한다.

## 메시지 계약 & 상태 흐름

통합 경계는 **[`src/core/contracts.py`](src/core/contracts.py)** 다. 발행 측(Spring의 `OcrJob.java`)과 소비 측이 이 스키마를 함께 지켜야 한다.

| 큐 | 페이로드 | 발행 → 소비 | 멱등 키 |
|---|---|---|---|
| `ocr-job-queue` | `OcrJob` | Spring → `ocr_worker` (문서 1건씩) | `job_id` |
| `report-job` | `ReportJob` | `ocr_worker` → `report_worker` (**청구당 1건**) | `report_id` |

**청구 fan-in (`ai.claim_readiness`)**
한 청구는 문서 여러 건(`doc_index`/`doc_total`)으로 들어온다. 문서가 **종결**될 때마다 카운터가 오른다. 종결에는 성공, 확정 실패, poison 소진이 모두 포함된다. 마지막 문서를 처리한 워커가 필수 유형(증권과 진단서)이 갖춰졌는지 판정한다.

| 판정 (전 문서 종결 시) | `claim_readiness.status` | 발행 | 결과 |
|---|---|---|---|
| 필수 유형 충족 | `pending` → `published` | `ReportJob(ocr_quality=ok)` | 리포트 생성 |
| 필수 유형 누락·인식 불완전 | `pending` → `blocked` | `ReportJob(ocr_quality=needs_reupload)` | `reports.status = NEEDS_REUPLOAD` |

**실패 처리**: DLQ는 쓰지 않는다. 결정적 실패(`NonRetryableError`: 마스킹 잔류, 디코드 실패)는 첫 시도에서 바로 ack하고 종결한다. 일시 실패는 재전달되며, 수신 횟수 상한을 넘기면 poison으로 스킵한다. 모든 실패는 `ai.ocr_job_failures` 저널에 기록되므로 실패가 조용히 사라지지 않는다.

상세: [.claude/docs/contracts.md](.claude/docs/contracts.md) · [docs/architecture/lifecycle.md](docs/architecture/lifecycle.md)

## DB 스키마 & 마이그레이션

프레임워크 없이 번호 순서대로 적용하는 **멱등 SQL** 파일로 관리한다. 디렉터리마다 소유 스키마와 실행 워커가 정해져 있다.

| 디렉터리 | 스키마 소유 role | 적용 주체 |
|---|---|---|
| [`migrations/ai/`](migrations/ai) | `ai_owner` | `ocr_worker` 기동 시 |
| [`migrations/corpus/`](migrations/corpus) | `corpus_owner` | `corpus_worker` 기동 시 |
| `core.*` | — | backend Flyway (이 레포 범위 밖) |

> 새 마이그레이션은 **소유 워커의 디렉터리**에만 추가한다. 다른 role의 스키마를 건드리면 `permission denied`로 워커가 다운된다. 배경: [migrations/README.md](migrations/README.md)
> PII 컬럼은 AES-256-GCM으로 암호화한다. [deploy/DB_ENCRYPTION.md](deploy/DB_ENCRYPTION.md)

## 테스트 & 품질

```bash
uv run ruff check src tests && uv run ruff format .   # 린트·포맷
uv run mypy src                                        # 타입 (strict)
uv run pytest                                          # 단위 테스트
```

- 무거운 의존성(surya, pymupdf, torch)은 **lazy import**로 분리해 두어서, GPU가 없어도 OCR 오케스트레이션을 테스트할 수 있다.
- e2e 테스트(`tests/e2e`)는 LocalStack에 자체 유니크 큐를 만들어 실행한다.
- 코드 컨벤션: **[.claude/CODE_CONVENTIONS.md](.claude/CODE_CONVENTIONS.md)**

## 배포

`dev` 브랜치에 push하면 [`deploy.yml`](.github/workflows/deploy.yml)이 실행된다. `ci.yml`의 테스트 게이트를 통과해야 진행하며, **변경된 서비스만** 빌드해 ECR에 push한다.

| 서비스 | Dockerfile | 배포 경로 |
|---|---|---|
| `report` | `src/report_worker/Dockerfile` (slim) | K8s-manifests의 `image.tag`를 커밋하면 **Argo CD**가 롤링 업데이트 |
| `ocr` | `src/ocr_worker/Dockerfile` (CUDA) | GPU EC2에 **SSM**으로 호스트 compose 재시작 |
| `corpus` | `src/corpus_worker/Dockerfile` | ETL EC2에 **SSM**으로 호스트 compose 재시작 |

공용 코드(`core`, `pyproject.toml`, `uv.lock`)가 바뀌면 전 서비스를 다시 빌드한다. `migrations/ai/**`가 바뀌면 ocr을, `migrations/corpus/**`가 바뀌면 corpus를 다시 배포한다.
상세: [deploy/README.md](deploy/README.md)

## 문서

| 문서 | 내용 |
|---|---|
| [docs/architecture/](docs/architecture/README.md) | LangGraph·DB·RAG·가드레일·라이프사이클 통합 아키텍처 |
| [docs/report-worker/](docs/report-worker/README.md) | 후유장해 파이프라인·일정 데이터·알려진 이슈 |
| [.claude/docs/contracts.md](.claude/docs/contracts.md) | 워커 간 인터페이스 계약 (병렬 개발 기준) |
| [.claude/docs/](.claude/docs) | 기능별 설계 (02 OCR · 04 RAG · 05 리포트 · 06 가드레일, Notion 미러) |

## 기여

`dev`에서 브랜치를 따서 작업하고 PR을 올린다. `contracts.py`를 바꿀 때는 Spring 쪽 `OcrJob.java`도 함께 맞춰야 한다. 커밋 메시지는 한국어 Conventional Commits 형식(`feat(ocr): ...`)을 따른다.
