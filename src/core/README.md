# core — 공용 인프라

모든 워커(`ocr_worker`·`report_worker`·`corpus_worker`)와 공용 모듈(`rag`·`guardrail`)이 의존하는 **토대**. 여기를 바꾸면 전 서비스가 재빌드된다(CI paths-filter). 그래서 변경은 작게, 하위 호환을 유지한다.

## 모듈

| 모듈 | 역할 | 주요 API |
|---|---|---|
| `config.py` | `pydantic-settings` 환경설정. 모든 env값의 단일 출처. AI 모델은 **이름·URL만** 받는다(하드코딩 금지) | `Settings`, `get_settings()` |
| `contracts.py` | **워커 간 메시지·결과 계약(단일 출처)**. SQS 페이로드와 RAG·가드레일 결과 모델 | `OcrJob`, `ReportJob`, `DocType`, `Chunk`, `Citation`, `RagResult`, `InputGuardResult`, `OutputGuardResult` |
| `sqs/` | boto3 SQS 래퍼. 롱폴링·역직렬화·ack(DeleteMessage)·poison 스킵·우아한 종료 | `SqsConsumer`, `SqsProducer`, `get_sqs_client()` |
| `db.py` | asyncpg 풀 lifecycle(pgvector 타입 등록 포함)과 멱등 SQL 마이그레이션 적용 | `db_pool()`, `init_pool()`/`get_pool()`/`close_pool()`, `run_migrations()` |
| `ai_client.py` | **OpenAI 호환** 추론 클라이언트(Ollama/vLLM 무관). 임베딩 차원 검증 포함 | `chat()`, `chat_json()`, `embed()`, `close_client()` |
| `crypto.py` | AES-256-GCM PII 컬럼 복호화(Spring 암호화 봉투 포맷). DEK 조회 | `decrypt_pii()`, `maybe_decrypt()`, `get_pii_dek()` |
| `exceptions.py` | 공용 예외 계층. `NonRetryableError`는 SQS 컨슈머가 **첫 시도에 즉시 ack**하는 결정적 실패 표식 | `AppError`, `NonRetryableError`, `UnreadableFileError`, `PiiCryptoError` 등 |
| `logging.py` | structlog JSON 로깅. 출력 직전 **PII 자동 마스킹**(주민·카드·전화·이메일), 요청 컨텍스트 바인딩 | `configure_logging()`, `get_logger()`, `bind_context()`, `log_event()` |

## SQS 컨슈머 동작 (`sqs/consumer.py`)

| 핸들러 결과 | 처리 |
|---|---|
| 정상 반환 | DeleteMessage(ack) |
| `NonRetryableError` | **즉시 ack**하고 종결. 재전달 낭비 없음 |
| 그 외 예외 | 삭제하지 않음. visibility timeout 뒤 재전달 |
| 수신 횟수가 `SQS_MAX_RECEIVE_COUNT` 초과 | poison으로 스킵. 스킵 직전 훅으로 저널 기록 가능 |

DLQ는 쓰지 않는다. 핸들러는 반드시 **멱등**해야 한다(at-least-once).

## 주요 환경변수

| 변수 | 용도 |
|---|---|
| `DATABASE_URL`, `RDS_CA_PATH` | PostgreSQL 접속(RDS면 TLS CA 경로) |
| `SQS_OCR_JOB_QUEUE_URL`, `SQS_REPORT_JOB_QUEUE_URL` | 큐 전체 URL |
| `SQS_ENDPOINT_URL` | 로컬 LocalStack(`http://localhost:4566`). prod은 비움 |
| `AI_BASE_URL`, `LLM_MODEL` | 챗 추론 엔드포인트·모델 |
| `EMBEDDING_BASE_URL`, `EMBEDDING_MODEL`, `EMBEDDING_DIM` | 임베딩(기본 1024d) |
| `VLM_BASE_URL`, `VLM_MODEL`, `VLM_GROUNDING_MODEL` | OCR 보조 VLM |
| `PII_ENC_KEY` / `PII_KMS_KEY_ARN` | PII 복호화 키(local·dev는 env, 그 외 KMS) |
| `SERVICE_NAME`, `ENV`, `LOG_LEVEL` | 로깅 메타 |

전체 목록과 기본값은 `config.py`를 참고한다. AWS 자격증명은 정적 키 없이 IAM Role로 받는다.

## ⚠️ contracts.py 변경 시

`contracts.py`는 **팀 전체의 통합 경계**다.

- 발행자와 소비자 양쪽 영향을 함께 확인한다. 예: `ocr_worker`가 발행하는 `ReportJob` ↔ `report_worker`가 소비.
- `OcrJob`의 정본은 Spring의 `OcrJob.java`다. 필드를 바꾸면 게이트웨이 쪽도 함께 맞춘다.
- 필드 추가는 기본값을 둬서 **하위 호환**으로 만든다. 이미 큐에 들어 있는 메시지가 검증에서 깨지면 안 된다.

## 참고

- [계약 명세](../../.claude/docs/contracts.md) · [암호화](../../deploy/DB_ENCRYPTION.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
