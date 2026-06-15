---
name: kafka-worker-patterns
description: aiokafka 기반 비동기 Kafka 워커(consumer/producer)를 구현할 때 사용. OCR Worker·Report Worker처럼 토픽을 소비해 처리하고 결과를 발행/저장하는 워커, 메시지 역직렬화·검증·오프셋 커밋·재시도·DLQ·우아한 종료 패턴을 다룬다. Kafka 컨슈머/프로듀서·워커 진입점·메시지 처리 루프 작업 시 사용.
---

# Kafka Worker Patterns

OCR(02)·리포트(05) 워커가 공유하는 비동기 Kafka 패턴. 이 프로젝트의 노드 간 통신은 OCR/리포트가 Kafka(브로커 경유, 인스턴스 분리)다. 항상 `src/core/kafka/` 래퍼를 거치고 직접 클라이언트를 만들지 않는다 — 재시도·역직렬화·로깅이 한 곳에 모여야 하기 때문이다.

## 핵심 구조
워커 진입점(`src/workers/*.py`)은 얇게:
1. config 로드 → 풀(db)·ai_client·producer 초기화
2. consumer 래퍼에 **pydantic 스키마 + 핸들러 콜백** 등록
3. 처리 루프 시작, 시그널(SIGTERM) 시 우아한 종료

비즈니스 로직은 `src/ocr/` · `src/report/`에 두고, 워커는 배선만 한다.

## 메시지 처리 원칙
- **역직렬화 즉시 검증**: raw → `contracts.py`의 pydantic 모델. 검증 실패 메시지는 DLQ로(파이프라인을 막지 않음).
- **오프셋 커밋 전략**: 처리 성공 후 커밋(at-least-once). 핸들러는 **멱등**하게 설계한다 — 재처리되어도 안전하도록(예: upsert, job_id 기준 중복 차단).
- **재시도/DLQ**: 일시적 실패는 제한 횟수 재시도, 초과 시 DLQ로 보내고 상태를 DB에 기록. 원본 메시지를 절대 유실하지 않는다.
- **장시간 작업**: OCR·LangGraph는 수 초~분. `max.poll.interval`을 충분히 두거나 처리를 백그라운드 태스크로 분리해 컨슈머 하트비트를 유지한다.

## 비동기 규칙
- 핸들러 전체 async. 블로킹 라이브러리(PaddleOCR 등)는 `asyncio.to_thread`로 격리해 이벤트 루프를 막지 않는다.
- 독립 I/O(S3 읽기·임베딩·DB)는 `asyncio.gather`로 병렬화.

## 우아한 종료
- SIGTERM 수신 시 새 메시지 소비 중단 → 진행 중 작업 완료 대기 → 오프셋 커밋 → 풀·연결 정리. K8s 롤링 배포에서 메시지 유실을 막는다.

## 발행 (request 측이 아닌 결과 이벤트)
- OCR 완료 → `ReportJob` 발행. 페이로드는 contracts 스키마로 직렬화. 발행 실패도 재시도/로깅.

## 검증
- 로컬 redpanda에 테스트 메시지를 주입해 consume→처리→발행/저장 end-to-end 확인.
- 잘못된 페이로드가 DLQ로 가는지, 중복 메시지가 멱등 처리되는지 테스트.
