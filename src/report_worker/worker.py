"""report-job 핸들러 — ReportJob를 LangGraph 그래프에 태워 리포트 초안을 생성한다.

SqsConsumer가 소비·검증·ack(DeleteMessage)·재전달을 담당하므로 여기서는 (1) 그래프 실행,
(2) 실행 결과를 메시지 처분으로 옮기는 일, (3) 실패 저널 기록만 한다. 정상·부분 결과는
반환(ack), 재전달로 회복될 수 있는 실패는 `ReportWorkerError`(재전달), 재전달해도 같은 거절은
`ReportRefusedError`(즉시 ack)다. 처리는 report_id로 멱등하다(persist가 report_drafts를
ON CONFLICT(report_id) 업서트).

실패한 리포트는 저장하지 않는다. 컨텍스트 하드 실패와 LLM 시스템 실패·거절은 그래프가 persist
전에 멈춘다(graph.py). 여기서는 그 결과를 읽어 재전달할지 종결할지만 정한다.

실패 저널(`ai.report_job_failures`, 마이그레이션 012): 컨슈머에는 `handle_job`이 아니라
`journaled_handler(pool)`가 붙는다(`__main__`이 배선). 하드 실패로 재전달될 때마다 비종결 행을
남기고, 리포트가 종결 상태에 이르면 지운다. 재전달 한도를 다 쓴 poison의 확정(terminal)은
`__main__._poison_journal` 훅이 한다. Spring은 terminal 행을 보고 FAILED를 표시한다.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from time import perf_counter
from typing import Any

from core.contracts import ReportJob
from core.exceptions import NonRetryableError
from core.logging import bind_context, clear_context, elapsed_ms, get_logger, log_event
from report_worker.failure_journal import (
    Executor,
    FailureClass,
    clear_report_failure,
    record_report_failure,
)
from report_worker.graph import build_graph
from report_worker.metrics import critical_path_ms
from report_worker.nodes.agents import (
    CONTEXT_FAILURE_PREFIXES,
    LLM_REFUSED,
    LLM_SYSTEM_FAILURE,
    mark_needs_reupload,
)
from report_worker.state import ReportState

logger = get_logger(__name__)

type JobHandler = Callable[[ReportJob], Awaitable[None]]

# 하드 실패(리포트 미저장)로 보아 재전달(재처리)을 유도할 노드 실패 마커 prefix와, 그 실패를 실패
# 저널에 남길 분류. 위에서부터 본다.
# 컨텍스트 조립 실패(load_context_failed·ocr_result_missing — 그래프가 load_context 직후 멈춘다)나
# 저장 실패는 메시지를 잃지 않도록 raise로 승격한다. persist_refused_failed는 LLM 거절 뒤 BLOCKED
# 기록이 실패한 경우다 — 기록 없이 종결(ack)하면 리포트가 영원히 처리 중이라 재전달로 다시 기록한다.
# 그 외(rag_empty·input_blocked·*_llm_failed·pii_mask_failed 등)는 부분결과로 커밋한다(이슈 #11).
# persist_blocked 실패("persist_blocked_failed")는 여기에 넣지 않는다 — 차단 기록은 초안 없이
# status만 갱신하는 부수효과라, 실패해도 메시지 유실이 아니라 status 미기록일 뿐이므로 하드
# 실패로 승격하지 않는다(재처리해도 동일 차단 → 재전달 반복 방지).
_HARD_FAILURE_CLASSES: tuple[tuple[tuple[str, ...], FailureClass], ...] = (
    (CONTEXT_FAILURE_PREFIXES, FailureClass.CONTEXT_UNAVAILABLE),
    (("persist_failed", "persist_refused_failed"), FailureClass.PERSIST_FAILED),
)
_HARD_FAILURE_PREFIXES = tuple(
    prefix for prefixes, _failure_class in _HARD_FAILURE_CLASSES for prefix in prefixes
)

# 그래프는 DB에 접촉하지 않고 조립되므로 import 시 1회 컴파일해 재사용한다.
_graph = build_graph()


class ReportWorkerError(RuntimeError):
    """리포트 생성 하드 실패 — SqsConsumer가 재전달·poison 스킵하도록 전파한다.

    `failure_class`·`error_type`은 실패 저널(`ai.report_job_failures`)에 남길 분류다
    (`journaled_handler`가 읽는다). 예외 메시지는 저널에 넣지 않는다(§9).
    """

    def __init__(
        self,
        message: str,
        *,
        failure_class: FailureClass = FailureClass.UNKNOWN,
        error_type: str | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_class = failure_class
        self.error_type = error_type


class ReportRefusedError(ReportWorkerError, NonRetryableError):
    """LLM이 리포트 생성을 거절했다(서버 측 폴백 모델까지). 초안 없이 BLOCKED로 종결했다.

    같은 입력을 다시 보내도 거절될 가능성이 높아 재전달은 비용만 쓴다. 그래서 SqsConsumer가
    즉시 ack하도록 `NonRetryableError`를 함께 상속한다. BLOCKED 기록이 실패하면 이 예외를
    던지지 않는다(`persist_refused_failed` → `ReportWorkerError`로 재전달). 실패 저널에는
    남기지 않는다(BLOCKED가 이미 사용자에게 보이는 종료 상태다).
    """


def _hard_failure_class(marker: str) -> FailureClass:
    """하드 실패 표식을 저널 분류로 바꾼다(`_HARD_FAILURE_PREFIXES`로 거른 표식만 들어온다)."""
    for prefixes, failure_class in _HARD_FAILURE_CLASSES:
        if marker.startswith(prefixes):
            return failure_class
    return FailureClass.UNKNOWN


def _marker_error_type(marker: str | None) -> str | None:
    """`{노드}_failed:{예외 타입}` 표식에서 예외 타입만 꺼낸다. 타입이 없는 표식이면 None.

    표식은 safe_node가 노드명·예외 클래스명만으로 만든다(PII 없음) — 저널 error_type에 그대로
    쓸 수 있다. `ocr_result_missing`처럼 예외 없이 남은 표식은 타입이 없다.
    """
    if marker is None:
        return None
    _, separator, error_type = marker.rpartition(":")
    return error_type if separator and error_type else None


def _last_node_failure(errors: list[str]) -> str | None:
    """가장 최근 노드 예외 표식. LLM 실패 직후 다음 간선에서 그래프가 멈추므로 그 실패의 것이다."""
    return next((error for error in reversed(errors) if "_failed:" in error), None)


def _log_graph_finished(final: dict[str, Any], *, wall_ms: int | None = None) -> None:
    """그래프 1회 실행의 노드별 소요 시간(`node_trace`)을 이벤트 1건으로 남긴다(성능 SLI).

    `node_ms`는 `{노드명: ms}`라 CloudWatch Logs Insights에서 `node_ms.diagnosis`처럼 바로
    집계된다. 같은 노드가 두 번 돌면 합친다. 노드명·수치·건수만 싣는다(§9).

    `wall_ms`는 그래프 전체 벽시계다. 조건 분기 함수(`policy_in_db`의 약관 존재 조회 등)는
    노드가 아니라 `node_trace`에 안 잡히므로, 그 몫을 `outside_nodes_ms`로 따로 남긴다
    (dev 실측: 리포트 221초 중 43초가 여기였다).
    """
    trace = [entry for entry in final.get("node_trace") or [] if isinstance(entry, dict)]
    node_ms: dict[str, int] = {}
    for entry in trace:
        name = str(entry.get("node") or "")
        node_ms[name] = node_ms.get(name, 0) + int(entry.get("ms") or 0)
    # 동시에 돈 노드(R6 묶음)는 가장 긴 줄기만 센다 — 단순 합은 벽시계보다 커진다
    total_ms = critical_path_ms(trace)
    log_event(
        logger,
        "report.graph.finished",
        total_ms=total_ms,
        wall_ms=wall_ms,
        outside_nodes_ms=None if wall_ms is None else max(0, wall_ms - total_ms),
        node_ms=node_ms,
        node_count=len(trace),
        failed_nodes=[str(e.get("node")) for e in trace if e.get("status") == "failed"],
        error_count=len(final.get("errors") or []),
        llm_failure=final.get("llm_failure"),
    )


def _raise_if_failed(final: dict[str, Any]) -> None:
    """그래프 최종 상태를 메시지 처분으로 옮긴다. 저장된 결과(정상·부분)면 그냥 돌아간다.

    판정 순서가 계약이다. 하드 표식을 거절보다 먼저 본다 — 거절 뒤 BLOCKED 기록이 실패했으면
    종결하지 않고 재전달해야 하기 때문이다. 로그·예외 메시지엔 errors 표식만 싣는다(노드명·예외
    클래스명·고정 문자열, PII 없음). 컨슈머 로그엔 report_id와 사유가 남지 않으므로 여기서 남긴다.

    Args:
        final: `_graph.ainvoke()`가 돌려준 최종 상태.

    Raises:
        ReportWorkerError: 컨텍스트·저장 하드 실패, 또는 LLM 시스템 실패(재전달). 실패 저널
            분류(`failure_class`·`error_type`)를 싣는다.
        ReportRefusedError: LLM 거절 — BLOCKED 기록 후 즉시 종결.
    """
    errors = [str(e) for e in final.get("errors", [])]
    hard = [e for e in errors if e.startswith(_HARD_FAILURE_PREFIXES)]
    if hard:
        logger.warning("report not saved → redeliver", failure="hard_failure", errors=errors)
        raise ReportWorkerError(
            f"리포트 생성 하드 실패: {hard}",
            failure_class=_hard_failure_class(hard[0]),
            error_type=_marker_error_type(hard[0]),
        )
    llm_failure = final.get("llm_failure")
    if llm_failure == LLM_SYSTEM_FAILURE:
        logger.warning("report not saved → redeliver", failure=LLM_SYSTEM_FAILURE, errors=errors)
        raise ReportWorkerError(
            f"LLM 시스템 실패로 리포트 미저장: {errors}",
            failure_class=FailureClass.LLM_UNAVAILABLE,
            error_type=_marker_error_type(_last_node_failure(errors)),
        )
    if llm_failure == LLM_REFUSED:
        logger.warning("report refused by llm → blocked", errors=errors)
        raise ReportRefusedError(f"LLM 거절로 리포트 미저장(BLOCKED 기록): {errors}")


async def _mark_needs_reupload(report_id: str) -> None:
    """`reports.status='NEEDS_REUPLOAD'`를 기록한다. 실패는 저장 실패로 분류해 그대로 올린다.

    `mark_needs_reupload`는 DB 쓰기 실패를 삼키지 않는다. 여기서는 실패 저널 분류
    (`persist_failed`)만 붙여 다시 던진다 — 원인 예외는 `__cause__`로 남고, 컨슈머는 그대로
    재전달한다. 컨슈머 로그엔 report_id·사유가 없어 여기서 타입만 남긴다(§9).
    """
    try:
        await mark_needs_reupload(report_id)
    except Exception as exc:  # 분류만 붙여 다시 던진다 — 삼키지 않는다(§8)
        error_type = type(exc).__name__
        logger.warning(
            "report status not saved → redeliver",
            failure=str(FailureClass.PERSIST_FAILED),
            error_type=error_type,
        )
        raise ReportWorkerError(
            "NEEDS_REUPLOAD 상태 기록 실패",
            failure_class=FailureClass.PERSIST_FAILED,
            error_type=error_type,
        ) from exc


async def handle_job(job: ReportJob) -> None:
    """단일 report-job 처리. 성공 시 조용히 반환(→커밋), 실패 시 raise(→재전달 또는 즉시 종결).

    실패 저널을 모른다 — 운영 컨슈머는 이 함수를 감싼 `journaled_handler`를 쓴다.
    """
    bind_context(report_id=job.report_id, job_id=job.job_id)
    try:
        if job.ocr_quality == "needs_reupload":
            # OCR 품질 미달 신호 — 리포트를 생성하지 않는다(계약: contracts.ReportJob.ocr_quality).
            # reports.status='NEEDS_REUPLOAD'로 Backend에 알린다. 기록에 실패하면 삼키지 않고
            # 저장 실패(ReportWorkerError)로 올려 재전달한다(_mark_needs_reupload).
            logger.info("report skipped: ocr needs_reupload", report_id=job.report_id)
            await _mark_needs_reupload(job.report_id)
            return
        state: ReportState = {
            "report_id": job.report_id,
            "ocr_result_id": job.ocr_result_id,
            "claim_id": job.claim_id,
            "user_ref": job.user_ref,
            "doc_type": job.doc_type.value,
        }
        started = perf_counter()
        final = await _graph.ainvoke(state)
        # 저장 여부와 무관하게 남긴다(_raise_if_failed 전) — draft.metrics는 저장된 리포트만
        # 가져서, 실패·거절된 리포트의 지연이 빠진 기준선이 되지 않게 한다.
        _log_graph_finished(final, wall_ms=elapsed_ms(started))
        _raise_if_failed(final)
        logger.info(
            "report generated", report_id=job.report_id, error_count=len(final.get("errors", []))
        )
    finally:
        clear_context()


def journaled_handler(pool: Executor) -> JobHandler:
    """`handle_job`에 실패 저널(`ai.report_job_failures`)을 씌운 컨슈머 핸들러를 만든다.

    메시지 처분은 `handle_job`이 정한 그대로다 — 이 래퍼는 예외를 바꾸지 않고 기록만 덧붙인다.

    - 종결(정상·부분 저장, 입력 차단 BLOCKED, NEEDS_REUPLOAD): 이 리포트의 행을 지운다(회복).
    - 거절(`ReportRefusedError`): 저널에 남기지 않는다. BLOCKED로 이미 사용자에게 보이는 종료
      상태다. 이전 시도가 남긴 행은 위와 같은 이유로 지운다.
    - 하드 실패(`ReportWorkerError`): 그 분류로 비종결 행을 업서트한다(재전달마다 attempts+1).
    - 그 밖의 예외: `unknown`(error_type=클래스명)으로 업서트한다. 컨슈머는 이것도 재전달한다.

    저널 쓰기 실패는 삼키고 타입만 경고로 남긴다 — 처분은 이미 정해졌다. 실패 경로에서 저널
    예외를 올리면 원래 예외를 가리고, 종결 경로에서 올리면 끝난 리포트가 재전달돼 LLM을 다시
    부른다. 놓친 기록은 다음 재전달이 다시 남기고, 끝내 못 남겨도 poison 훅이 확정 행을 만든다.

    Args:
        pool: 저널을 쓸 asyncpg 풀(그래프 노드가 쓰는 전역 풀과 같은 것).

    Returns:
        `SqsConsumer(handler=...)`에 넘길 핸들러.
    """

    async def handle(job: ReportJob) -> None:
        try:
            await handle_job(job)
        except ReportRefusedError:
            await _clear_failure(pool, job)
            raise
        except ReportWorkerError as exc:
            await _record_failure(pool, job, exc, exc.failure_class, exc.error_type)
            raise
        except Exception as exc:  # 분류 밖 예외도 재전달된다 — 기록만 덧붙여 그대로 올린다(§8)
            await _record_failure(pool, job, exc, FailureClass.UNKNOWN, type(exc).__name__)
            raise
        await _clear_failure(pool, job)

    return handle


async def _record_failure(
    pool: Executor,
    job: ReportJob,
    exc: Exception,
    failure_class: FailureClass,
    error_type: str | None,
) -> None:
    """비종결 행을 업서트한다. 저널 실패는 삼킨다 — 원래 예외가 이미 재전달을 부른다."""
    try:
        await record_report_failure(pool, job, failure_class=failure_class, error_type=error_type)
    except Exception as journal_exc:  # 저널 실패가 원래 실패를 가리지 않게(§8) — 타입만 남긴다(§9)
        logger.warning(
            "report_job_failure_journal_failed",
            report_id=job.report_id,
            failure_class=str(failure_class),
            error_type=type(journal_exc).__name__,
            original_error_type=type(exc).__name__,
        )


async def _clear_failure(pool: Executor, job: ReportJob) -> None:
    """종결된 리포트의 행을 지운다. 실패해도 종결을 뒤집지 않는다.

    남은 행은 사용자에게 보이지 않는다. 비종결 행은 Spring이 읽지 않고, terminal 행이 남아도
    Spring은 종결 상태(COMPLETED·BLOCKED·NEEDS_REUPLOAD)를 저널보다 먼저 판정한다.
    """
    try:
        await clear_report_failure(pool, job.report_id)
    except Exception as exc:  # 끝난 리포트를 재전달로 되돌리지 않게(§8) — 타입만 남긴다(§9)
        logger.warning(
            "report_job_failure_clear_failed",
            report_id=job.report_id,
            error_type=type(exc).__name__,
        )
