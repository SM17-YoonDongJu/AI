"""리포트 작업 실패 저널(``ai.report_job_failures``, 마이그레이션 012) — ``report_id`` 멱등 업서트.

실패한 리포트는 저장하지 않으므로, 재전달 한도를 다 써 poison으로 걷힌 리포트는 기록이
없으면 Spring에서 영원히 "처리 중"으로 보인다. 이 모듈이 그 실패를 한 행으로 남기고,
Spring은 ``terminal`` 행을 읽어 FAILED를 표시한다. OCR 실패 저널
(``ocr_worker.repository``의 ``ai.ocr_job_failures`` 함수들)과 같은 모양이다.

세 갈래:
- **기록**(``record_report_failure``): 하드 실패로 재전달될 때마다 비종결 행을 업서트한다.
  같은 리포트의 반복 실패는 ``attempts``로 쌓인다.
- **회복**(``clear_report_failure``): 리포트가 종결 상태(저장·BLOCKED·NEEDS_REUPLOAD)에
  이르면 행을 지운다. 남겨 두면 끝난 리포트가 실패로 조회된다.
- **확정**(``mark_report_failure_terminal``): poison 훅이 걷어내기 직전 ``terminal=true``로
  도장 찍는다. 기존 행의 ``failure_class``는 보존한다.

``terminal``은 단방향(false→true)이다. 비종결 업서트는 ``terminal``을 SET 절에 아예 두지
않아 구조적으로 되돌릴 수 없다. 예외 **메시지는 저장하지 않는다**(클래스명만, §9).

이 모듈은 OCR 쪽 코드를 import하지 않는다 — 두 워커는 다른 컨테이너이고, report 이미지에
OCR 의존이 없다(§14). 대신 같은 규약을 각자 갖는다.
"""

import uuid
from enum import StrEnum

import asyncpg

from core.contracts import ReportJob
from core.logging import get_logger

logger = get_logger(__name__)

# 풀·커넥션 어느 쪽으로도 실행할 수 있는 실행기(§10, ``ocr_worker.repository``와 같은 별칭).
# 저널 쓰기는 단일 문이라 보통 풀을 그대로 넘긴다.
type Executor = asyncpg.Pool | asyncpg.Connection


class FailureClass(StrEnum):
    """``failure_class`` 값(마이그레이션 012의 CHECK 목록과 1:1 — 테스트가 고정한다).

    값을 더하면 CHECK도 같은 마이그레이션 흐름으로 넓혀야 한다. 어긋나면 저널 쓰기가
    CHECK 위반으로 실패하고, 저널은 실패를 기록하는 마지막 방어선이라 그 실패가 곧 무음
    실패다.
    """

    CONTEXT_UNAVAILABLE = "context_unavailable"  # load_context 예외 또는 청구·대표 문서 0건
    LLM_UNAVAILABLE = "llm_unavailable"  # 노드 안의 AiClientError(거절 제외)
    PERSIST_FAILED = "persist_failed"  # 초안·상태 저장 실패(persist·BLOCKED·NEEDS_REUPLOAD 기록)
    SCHEMA_INVALID = "schema_invalid"  # 역직렬화 실패·report_id가 UUID가 아님(poison 훅 전용)
    UNKNOWN = "unknown"  # 위에 안 걸린 예외, 또는 기록 없이 poison까지 간 경우


# ── SQL ─────────────────────────────────────────────────────────
# 스키마를 명시(ai.)한다 — search_path에 기대지 않는다(#48~#52 소유권 사건 이후 관례).
# SET 절의 기존 행 참조는 008과 같이 스키마 없는 테이블명(`report_job_failures.…`)이다.
#
# 비종결 업서트. terminal은 VALUES에서만 false이고 SET 절에는 **없다** — 이미 확정(true)된
# 행에 또 실패가 와도 확정 상태가 유지된다(단방향). failure_class·error_type은 최신 실패로
# 갱신한다(무엇이 마지막으로 막았는지가 운영에 필요한 정보다). first_failed_at은 불변이다
# (체류 시간 산출 근거). 시각은 전부 SQL now() 기준(앱-DB 시계 스큐 회피).
_UPSERT_FAILURE_SQL = """
INSERT INTO ai.report_job_failures (
    report_id, claim_id, job_id, failure_class, error_type, attempts, terminal
) VALUES ($1, $2, $3, $4, $5, 1, false)
ON CONFLICT (report_id) DO UPDATE SET
    attempts = report_job_failures.attempts + 1,
    last_failed_at = now(),
    failure_class = EXCLUDED.failure_class,
    error_type = EXCLUDED.error_type,
    claim_id = EXCLUDED.claim_id,
    job_id = EXCLUDED.job_id
"""

# 회복한 리포트의 저널 정리(terminal 여부 무관 — 실제로 끝났으면 실패가 아니다).
_CLEAR_FAILURE_SQL = "DELETE FROM ai.report_job_failures WHERE report_id = $1"

# poison 훅 전용 — 확정 도장. failure_class를 SET 절에 **넣지 않아** 기존 행의 분류
# (예: llm_unavailable)를 보존한다. 행이 없을 때만(기록 없이 poison까지 간 경우 — 처리 중
# 프로세스가 죽는 등) 'unknown'으로 삽입된다. attempts는 GREATEST로 뒤로 가지 않게 한다 —
# receive_count(SQS 수신 횟수)와 저널 attempts(핸들러 실패 횟수)는 세는 대상이 다르다.
_MARK_TERMINAL_SQL = """
INSERT INTO ai.report_job_failures (
    report_id, message_id, claim_id, job_id, failure_class, attempts, terminal
) VALUES ($1, $2, $3, $4, 'unknown', $5, true)
ON CONFLICT (report_id) DO UPDATE SET
    terminal = true,
    message_id = EXCLUDED.message_id,
    attempts = GREATEST(report_job_failures.attempts, EXCLUDED.attempts),
    last_failed_at = now()
"""

# report_id를 모르는 poison(역직렬화 실패, 또는 report_id가 UUID가 아님) — message_id로만
# 추적한다. report_id가 NULL이면 UNIQUE가 걸리지 않아 업서트가 불가능하다 → 순수 INSERT다.
# 훅이 성공한 뒤에만 메시지가 지워지므로 보통 1건이지만, 삭제 자체가 실패해 재전달되면 같은
# message_id로 한 행이 더 생길 수 있다. 추적 목적상 중복은 무해하고, 이를 막자고 유니크
# 인덱스를 더하면 "기록조차 못 남기는" 실패 모드가 새로 생긴다(008과 같은 판단).
_INSERT_UNKEYED_TERMINAL_SQL = """
INSERT INTO ai.report_job_failures (
    message_id, claim_id, job_id, failure_class, attempts, terminal
) VALUES ($1, $2, $3, 'schema_invalid', $4, true)
"""


def _as_uuid(value: str | None, *, field_name: str) -> uuid.UUID | None:
    """UUID 문자열을 관대하게 변환한다(빈 값·형식 오류면 ``None``).

    저널은 다른 실패를 기록하는 마지막 방어선이라 식별자 하나 때문에 예외를 던지면 원래
    실패까지 기록 없이 사라진다. 형식이 깨진 컬럼만 비운다. 값은 로그에 남기지 않고 어떤
    필드였는지만 남긴다(``ReportJob``의 식별자는 형식 검증 없는 ``str``이다).
    """
    if not value:
        return None
    try:
        return uuid.UUID(value)
    except ValueError:
        logger.warning("report_job_failure_invalid_uuid", field=field_name)
        return None


async def record_report_failure(
    executor: Executor,
    job: ReportJob,
    *,
    failure_class: FailureClass,
    error_type: str | None,
) -> None:
    """하드 실패를 비종결 행으로 업서트한다(``report_id`` 멱등, 재전달마다 ``attempts`` 증가).

    ``report_id``가 UUID가 아니면 아무것도 쓰지 않는다. 그런 행은 Spring이 조인할 수 없고,
    회복 시 지울 수도 없어 재전달마다 쌓이기만 한다. 그 메시지는 poison 훅이
    ``message_id``로 ``schema_invalid`` 행을 남긴다(``mark_report_failure_terminal``).

    Args:
        executor: asyncpg 연결 풀 또는 커넥션(core.db).
        job: 실패한 리포트 작업(식별자만 남긴다).
        failure_class: 실패 분류(마이그레이션 012의 CHECK 목록 중 하나).
        error_type: 예외 **클래스명만**(없으면 ``None`` — 예: 문서 0건). 메시지 문자열은
            넘기지 않는다(§9).
    """
    report_uuid = _as_uuid(job.report_id, field_name="report_id")
    if report_uuid is None:
        return
    await executor.execute(
        _UPSERT_FAILURE_SQL,
        report_uuid,
        job.claim_id,
        _as_uuid(job.job_id, field_name="job_id"),
        str(failure_class),
        error_type,
    )
    logger.info(
        "report_job_failure_recorded",
        report_id=job.report_id,
        failure_class=str(failure_class),
        error_type=error_type,
    )


async def clear_report_failure(executor: Executor, report_id: str) -> None:
    """종결 상태에 이른 리포트의 저널 행을 지운다(없으면 아무 일도 하지 않는다).

    재전달 중이던(``terminal=false``) 행뿐 아니라, 같은 ``report_id``의 다른 메시지가 먼저
    poison으로 확정(``terminal=true``)했던 행도 지운다 — 리포트가 실제로 끝났으면 더는
    실패가 아니다(OCR ``clear_job_failure``와 같다).

    Args:
        executor: asyncpg 연결 풀 또는 커넥션(core.db).
        report_id: 리포트 식별자(UUID 문자열).
    """
    report_uuid = _as_uuid(report_id, field_name="report_id")
    if report_uuid is None:
        return  # 애초에 기록될 수 없는 키 — 지울 행도 없다
    await executor.execute(_CLEAR_FAILURE_SQL, report_uuid)


async def mark_report_failure_terminal(
    executor: Executor, *, job: ReportJob | None, message_id: str, receive_count: int
) -> None:
    """poison 메시지를 확정 실패로 도장 찍는다(``SqsConsumer.on_poison`` 훅용).

    컨슈머가 수신 횟수 상한을 넘긴 메시지를 큐에서 걷어내기 **직전**에 부른다. 이게 마지막
    기록 기회다. 여기서 실패(예외)하면 컨슈머가 삭제를 보류하므로, 이 함수가 성공했다는
    것은 곧 "Spring이 볼 근거를 남겼다"는 뜻이다.

    두 갈래:
    - ``report_id``를 알면(본문은 유효했으나 처리가 계속 실패) 업서트로 ``terminal=true``를
      확정한다. 기존 행의 ``failure_class``는 **보존**한다 — 재전달 중 남긴 구체적 분류가
      ``unknown``으로 덮이면 왜 실패했는지 알 수 없게 된다.
    - 모르면(``job``이 ``None``이거나 ``report_id``가 UUID가 아님) ``message_id``로만
      ``schema_invalid`` 행을 남긴다. Spring은 이 행을 조인할 수 없다(알려진 한계).

    Args:
        executor: asyncpg 연결 풀 또는 커넥션(core.db).
        job: 파싱된 작업. 스키마 위반이면 ``None``.
        message_id: SQS MessageId(``report_id``가 없을 때의 유일한 추적 키).
        receive_count: ``ApproximateReceiveCount``(재전달 횟수).
    """
    report_uuid = None if job is None else _as_uuid(job.report_id, field_name="report_id")
    if job is None or report_uuid is None:
        await executor.execute(
            _INSERT_UNKEYED_TERMINAL_SQL,
            message_id,
            None if job is None else job.claim_id,
            None if job is None else _as_uuid(job.job_id, field_name="job_id"),
            receive_count,
        )
        logger.error(
            "report_job_failure_terminal",
            message_id=message_id,
            failure_class=str(FailureClass.SCHEMA_INVALID),
            receive_count=receive_count,
        )
        return
    await executor.execute(
        _MARK_TERMINAL_SQL,
        report_uuid,
        message_id,
        job.claim_id,
        _as_uuid(job.job_id, field_name="job_id"),
        receive_count,
    )
    logger.error(
        "report_job_failure_terminal",
        report_id=job.report_id,
        message_id=message_id,
        receive_count=receive_count,
    )
