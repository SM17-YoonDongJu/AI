# core — 공용 인프라

모든 워커(`ocr_worker`·`report_worker`·`chatbot`)와 공용 모듈(`rag`·`guardrail`)이 의존하는 **토대**. 다른 모두가 여기에 의존하므로 **가장 먼저 안정화**한다.

## 책임 (구현 예정 모듈)

| 모듈 | 역할 |
|---|---|
| `config.py` | `pydantic-settings` 기반 환경설정. 모든 env값을 한 곳에서. **AI 모델은 미정** → `base_url`/`model`만 다룸(하드코딩 금지) |
| `contracts.py` | **Spring↔Python·워커 간 메시지 계약(단일 출처)**. Kafka 토픽 페이로드 + WebSocket 메시지 pydantic 모델 |
| `kafka/` | aiokafka consumer/producer 래퍼 (역직렬화·오프셋 커밋·재시도·DLQ) |
| `db.py` | asyncpg 풀 lifecycle. AWS RDS(pgvector) 접속, 앱 시작 시 1회 생성 |
| `ai_client.py` | **OpenAI 호환** 추론 클라이언트(Ollama/vLLM/TEI 무관). chat/embed |
| `logging.py` | 구조적 로깅. **PII 로깅 금지**, 상관관계 식별자 바인딩 |

## ⚠️ contracts.py 주의

`contracts.py`는 **팀 전체의 통합 경계**다. 변경 시 **발행자·소비자 양쪽**(예: `ocr_worker`가 발행하는 `ReportJob` ↔ `report_worker`가 소비) 영향을 반드시 확인하고, 관련 담당자와 합의한다.

## 개발 시작점

`config.py` → `contracts.py`(계약 합의) → `kafka`/`db`/`ai_client` 순으로 골격을 채운다. 컨벤션은 [.claude/CODE_CONVENTIONS.md](../../.claude/CODE_CONVENTIONS.md).
