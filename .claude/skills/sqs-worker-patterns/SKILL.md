---
name: sqs-worker-patterns
description: boto3 기반 비동기 SQS 워커(consumer/producer)를 구현할 때 사용. OCR Worker·Report Worker처럼 큐를 롱폴링해 처리하고 결과를 발행/저장하는 워커, 메시지 검증·at-least-once 재전달·poison 가드·결정적 실패 즉시 종결·클레임 fan-in·우아한 종료 패턴을 다룬다. SQS 컨슈머/프로듀서·워커 진입점·메시지 처리 루프 작업 시 사용.
---

# SQS Worker Patterns

OCR(02)·리포트(05) 워커가 공유하는 비동기 SQS 패턴. 이 프로젝트의 노드 간 통신은 OCR/리포트가 SQS(`ocr-job-queue`·`report-job`, 로컬은 LocalStack)다 — Kafka가 아니다(PR #53에서 마이그레이션 완료). 항상 `src/core/sqs/`(`consumer.py`/`producer.py`/`client.py`) 래퍼를 거치고 직접 boto3 클라이언트를 만들지 않는다 — poison 가드·역직렬화·로깅이 한 곳에 모여야 하기 때문이다.

## 핵심 구조
워커 진입점(`src/<worker>/__main__.py`)은 얇게:
1. config 로드 → DB 풀·마이그레이션 적용 → `SqsProducer` 준비 → 파이프라인 배선
2. `SqsConsumer[T]`에 **pydantic 스키마 + 핸들러 콜백(+ 선택적 `on_poison` 훅)** 등록
3. `consumer.run()` — 롱폴링 루프 시작, 시그널(SIGTERM/SIGINT) 시 우아한 종료

비즈니스 로직은 `src/ocr_worker/` · `src/report_worker/`의 모듈에 두고, `__main__.py`는 배선만 한다(`ocr_worker/__main__.py`가 표준 예시).

## 메시지 처리 원칙 — at-least-once, 오프셋 없음
Kafka의 오프셋 커밋과 달리 SQS는 **메시지 단위 삭제(DeleteMessage)**가 ack다. 인프로세스 재시도는 두지 않는다 — 재시도 책임은 브로커(visibility timeout)에 맡긴다.

- **역직렬화 즉시 검증**: raw Body → `contracts.py`의 pydantic 모델(`model_validate_json`). 검증 실패는 삭제하지 않는다 — 재전달되고, poison 가드가 끝내 걷어낸다.
- **처리 성공 후에만 DeleteMessage**: 실패 시 삭제하지 않으면 `VisibilityTimeout`이 지난 뒤 SQS가 자동 재전달한다. 핸들러는 **반드시 멱등**해야 한다(예: `job_id` 기준 upsert로 중복 처리 차단).
- **결정적 실패는 첫 시도에서 종결**: 핸들러가 `NonRetryableError`(`core/exceptions.py`)를 던지면 재전달해도 결과가 같으므로 **즉시 삭제(ack)**하고 `error` 로그를 남긴다(`HandleOutcome.TERMINAL`). 일시적 실패는 삭제하지 않아 재전달된다(`HandleOutcome.RETRY`).
- **poison 가드(DLQ 대체)**: `ApproximateReceiveCount`가 `sqs_max_receive_count`를 넘으면 더는 못 살릴 메시지로 보고 명시적 삭제(스킵) 후 `error` 로그를 남긴다. **DLQ는 아직 붙이지 않았다** — 이 자체 방어가 없으면 poison 메시지가 큐 보존기간(기본 4일) 내내 재전달 루프를 돈다. 향후 redrive policy(DLQ)만 붙이면 이 코드는 그대로 호환된다.
- **`on_poison` 훅**: poison 메시지를 걷어내기 **직전**에 호출해 추적 근거(실패 저널 등)를 남길 기회를 준다. **순서가 핵심** — 훅이 성공한 뒤에만 삭제한다. 반대로 하면 훅 실패 시 메시지가 이미 사라져 어디에도 흔적이 남지 않는 무음 실패가 된다. 훅이 예외를 던지면 삭제를 보류해 다음 재전달에서 재시도한다(유계 — 큐 보존기간이 상한).

## 비동기 규칙
- boto3는 **동기 SDK**다. `receive_message`/`delete_message`/`send_message` 전부 `asyncio.to_thread`로 이벤트 루프에서 뗀다(CODE_CONVENTIONS §7).
- 무거운 블로킹 라이브러리(surya-ocr·PyTorch 등)도 동일하게 `asyncio.to_thread`로 격리한다.
- 독립 I/O(S3 읽기·임베딩·DB)는 `asyncio.gather`로 병렬화.

## 우아한 종료
- SIGTERM/SIGINT 수신 시 새 메시지 소비 중단 → 진행 중 메시지 완료 대기(짧게) → 풀·연결 정리. 종료 신호 중 강제 종료(SIGKILL)돼도 삭제 전 메시지는 재전달·멱등 재처리되므로 정합성은 깨지지 않는다(효율만 손해).

## 발행 (request 측이 아닌 결과 이벤트)
- 처리 완료 → `SqsProducer.send(queue_url, message)`로 결과 이벤트(`ReportJob` 등) 발행. 페이로드는 contracts 스키마 그대로 UTF-8 JSON 직렬화(래퍼·MessageAttributes 없음).
- Standard 큐라 순서 보장이 없다 — 멱등은 본문 식별자(예: `report_id`)로 다운스트림이 책임진다.

## 클레임 단위 fan-in 게이트 (OCR Worker 전용 패턴)
여러 문서가 한 클레임(`claim_id`)에 묶여 각자 별도 SQS 메시지로 도착하는 경우, 문서 하나 처리 완료 = 발행 시점이 아니다. `ocr_worker/pipeline.py`의 `advance_claim_progress`/`_judge_claim`이 이 문제를 푼다:

- **구조적 멱등 카운팅**: `claim_readiness.terminal_job_ids`(text[])에 `job_id`를 조건부 추가(`ON CONFLICT ... WHERE NOT terminal_job_ids @> ARRAY[$1]`)하고, `docs_terminal`은 `GENERATED ALWAYS AS (cardinality(terminal_job_ids)) STORED`로 파생시킨다. 같은 job이 재전달돼 두 번 세어져도(at-least-once) 배열 멤버십 체크가 중복 카운트를 막는다 — 별도 dedup 로직이 필요 없다.
- **판정은 두 조건의 합집합**(둘 다 걸리면 정상 리포트를 안 낸다): `missing`(`_REQUIRED_DOC_TYPES`(보험증권·진단서)가 성공한 문서들 중에 없음) OR `incomplete`(`len(docs) < doc_total` — 업로드 수보다 성공 수가 적음, 필수·비필수 안 가림). 어느 쪽이든 `claim_readiness.status='blocked'`로 남기되(운영 조회용), **report_worker로도 반드시 넘긴다** — `ocr_quality='needs_reupload'`로 `ReportJob`을 발행해 report_worker의 기존 스킵·통지 로직(`reports.status='NEEDS_REUPLOAD'`)을 그대로 태운다.
- **워커 경계를 넘는 쓰기는 메시지로, 직접 쓰지 않는다**: 이 상태를 Backend에 알리는 실제 지점(`core.reports` UPDATE)은 report_worker에만 있다. ocr_worker는 별도 컨테이너라 그 코드를 가져다 쓸 수 없다 — 설령 DB role이 같아 권한이 있어도, 이미 그 책임을 가진 워커가 있으면 그 워커의 메시지 인터페이스(SQS)로 넘기지 새 직접 쓰기 경로를 만들지 않는다.
- **poison과의 연결**: `on_poison` 훅에서도 `advance_claim_progress`를 호출한다 — poison으로 걷힌 문서(=영구히 처리 안 됨)도 "종결"로 카운트해야, 그 문서 하나 때문에 클레임 fan-in이 영원히 멈추지 않는다. `incomplete`로 잡히는 문서는 전부 이 경로(결정적 실패든 poison이든)를 거치므로, `job`이 파싱된 상태에서만 카운트가 오른다 → `ai.ocr_job_failures`에 `attachment_id`가 항상 남아 문서 특정이 가능하다(역직렬화 자체가 실패한 메시지는 카운트가 안 올라 이 체크에 걸리지 않는다 — 그 클레임은 대신 영구 `pending`으로 남는, 별개의 미해결 문제).

## 결정적 실패 저널 (OCR Worker 전용 패턴)
사용자에게 "판정 불가/재업로드 필요"를 빠르게 알리기 위해, 결정적 실패(`NonRetryableError`)는 poison 가드의 느린 큐 보존기간 타임아웃을 기다리지 않고 **즉시** `ai.ocr_job_failures`에 기록된다(`record_job_failure`/`mark_failure_terminal`). 원문·예외 메시지는 저장하지 않는다 — `failure_class`(분류값)와 `error_type`(예외 클래스명)만 남긴다(CODE_CONVENTIONS §9).

## 검증
- 로컬 LocalStack(`docker-compose.yml`)에 테스트 메시지를 주입해 consume→처리→발행/저장 end-to-end 확인.
- 잘못된 페이로드가 재전달되는지(삭제되지 않는지), poison 상한 초과 시 스킵되는지, 중복 메시지가 멱등 처리되는지 테스트.
- `NonRetryableError`가 즉시 ack(`HandleOutcome.TERMINAL`)로 처리되고 일반 예외는 `RETRY`로 처리되는지 컨슈머 단위 테스트로 고정(`test_sqs_consumer.py` 패턴 참고).
