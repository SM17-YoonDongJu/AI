"""리포트 실패 저널(``ai.report_job_failures``, 마이그레이션 012) 테스트 — DB·SQS 페이크로 격리.

재전달 한도를 다 써 poison으로 걷힌 리포트가 Spring에서 영원히 "처리 중"으로 남지 않게 하는
장치다. 여기서 고정하는 계약:
  (a) 마이그레이션: ai 스키마 테이블 하나, report_id 멱등 키, CHECK = ``FailureClass``,
      terminal 부분 인덱스, 역할이 있을 때만 SELECT GRANT, 매 기동 재실행에 안전한 DDL만.
  (b) 저널 SQL: 비종결 업서트는 attempts를 쌓고 terminal을 건드리지 않는다(단방향). poison
      확정은 terminal=true로 도장 찍되 failure_class를 보존한다. report_id를 모르면 message_id로
      schema_invalid 행을 남긴다. 회복하면 report_id로 지운다.
  (c) 핸들러(``worker.journaled_handler``): 하드 실패는 분류대로 비종결 업서트, 종결 경로는
      삭제, 거절은 저널에 남기지 않는다. 저널 쓰기 실패는 메시지 처분을 바꾸지 않는다.
  (d) poison 훅(``__main__._poison_journal``)·컨슈머 흐름·기동 배선(마이그레이션 적용).

업서트 전이(attempts 증가·terminal 단방향·failure_class 보존) 자체는 SQL 안에서 일어나므로
페이크 풀로는 실행할 수 없다. ``tests/test_repository.py``(008)와 같이 SQL·인자를 문자열로
고정하고, 실제 전이는 실 PG 검증이 확인한다.
"""

import asyncio
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from structlog.testing import capture_logs

from core.config import Settings
from core.contracts import DocType, ReportJob
from core.exceptions import NonRetryableError
from core.sqs.consumer import SqsConsumer
from report_worker import __main__ as report_main
from report_worker import failure_journal, worker
from report_worker.failure_journal import (
    FailureClass,
    clear_report_failure,
    mark_report_failure_terminal,
    record_report_failure,
)
from report_worker.nodes.agents import LLM_REFUSED, LLM_SYSTEM_FAILURE

_REPO_ROOT = Path(__file__).resolve().parents[1]
_MIGRATIONS_AI = _REPO_ROOT / "migrations" / "ai"
_MIGRATION = _MIGRATIONS_AI / "012_report_job_failures.sql"
_QUEUE = "https://sqs.ap-northeast-2.amazonaws.com/123456789012/report-job"

_REPORT_ID = "77777777-7777-7777-7777-777777777777"
_JOB_ID = "88888888-8888-8888-8888-888888888888"
_CLAIM_ID = "claim-1"


def _job(**overrides: Any) -> ReportJob:
    base: dict[str, Any] = {
        "report_id": _REPORT_ID,
        "ocr_result_id": "99999999-9999-9999-9999-999999999999",
        "job_id": _JOB_ID,
        "doc_type": DocType.DIAGNOSIS,
        "user_ref": "u-1",
        "claim_id": _CLAIM_ID,
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    base.update(overrides)
    return ReportJob(**base)


class _JournalPool:
    """asyncpg.Pool 흉내 — 저널 SQL·인자를 포착한다(실행하지 않는다).

    ``fail_on``을 주면 그 문자열을 포함한 SQL에서 연결 오류를 던진다(저널 DB 장애 재현).
    """

    def __init__(self, *, fail_on: str | None = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._fail_on = fail_on

    async def execute(self, sql: str, *args: Any) -> str:
        await asyncio.sleep(0)  # 실제 DB 왕복처럼 루프에 양보한다
        if self._fail_on is not None and self._fail_on in sql:
            raise ConnectionError("journal db down")
        self.calls.append((sql, args))
        return "OK"

    def kinds(self) -> list[str]:
        """포착한 SQL을 저널 동작 이름으로 바꾼다(호출 순서 비교용)."""
        names = {
            failure_journal._UPSERT_FAILURE_SQL: "upsert",
            failure_journal._CLEAR_FAILURE_SQL: "clear",
            failure_journal._MARK_TERMINAL_SQL: "terminal",
            failure_journal._INSERT_UNKEYED_TERMINAL_SQL: "unkeyed",
        }
        return [names.get(sql, "?") for sql, _ in self.calls]


def _stub_graph(monkeypatch: pytest.MonkeyPatch, *outcomes: dict[str, Any] | Exception) -> None:
    """그래프 대역. 호출마다 준비한 최종 상태를 순서대로 돌려준다(예외면 던진다)."""
    remaining = list(outcomes)

    async def ainvoke(_state: dict[str, Any]) -> dict[str, Any]:
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(worker, "_graph", SimpleNamespace(ainvoke=ainvoke))


_LLM_DOWN: dict[str, Any] = {
    "errors": ["diagnosis_failed:AiClientError"],
    "llm_failure": LLM_SYSTEM_FAILURE,
}
_SAVED: dict[str, Any] = {"errors": []}


def _upsert_args(failure_class: FailureClass, error_type: str | None) -> tuple[Any, ...]:
    return (uuid.UUID(_REPORT_ID), _CLAIM_ID, uuid.UUID(_JOB_ID), str(failure_class), error_type)


async def _redeliver_failing(handle: worker.JobHandler, job: ReportJob, *, attempts: int) -> None:
    """같은 메시지가 ``attempts``번 재전달돼 매번 하드 실패한다(재전달 = ``ReportWorkerError``)."""
    for _ in range(attempts):
        with pytest.raises(worker.ReportWorkerError):
            await handle(job)


async def _deliver(consumer: SqsConsumer[ReportJob], body: str, *receive_counts: int) -> None:
    """같은 메시지를 수신 횟수만 바꿔 차례로 처리한다(SQS 재전달 흉내)."""
    for receive_count in receive_counts:
        await consumer._process(_delivery(body, receive_count))


def _sql_code(sql: str) -> str:
    """주석을 걷어낸 SQL(주석 속 단어가 검사에 걸리지 않게)."""
    return re.sub(r"--[^\n]*", "", sql)


# ── (a) 마이그레이션 012 ──────────────────────────────────────
def test_migration_creates_one_table_in_its_own_schema() -> None:
    # Arrange
    sql = _sql_code(_MIGRATION.read_text(encoding="utf-8"))

    # Act
    foreign_refs = re.findall(r"\b(?:core|corpus|public)\.\w+", sql)

    # Assert: ai_owner는 ai 스키마만 만든다(§14) — 다른 owner 오브젝트를 건드리면 기동이 막힌다
    assert "CREATE TABLE IF NOT EXISTS ai.report_job_failures" in sql
    assert foreign_refs == []


def test_migration_pins_columns_spring_reads() -> None:
    # Arrange: 컬럼은 Spring 계약이다(_workspace/16) — 바뀌면 계약 메모도 같이 바꿔야 한다
    sql = _sql_code(_MIGRATION.read_text(encoding="utf-8"))
    body = sql.split("CREATE TABLE IF NOT EXISTS ai.report_job_failures (", 1)[1].split(");", 1)[0]

    # Act
    columns = re.findall(r"^\s{4}(\w+)\s+(?:uuid|text|int|boolean|timestamptz)\b", body, re.M)

    # Assert: 예외 메시지를 담을 컬럼은 없다(§9 — error_type은 클래스명만)
    assert columns == [
        "id",
        "report_id",
        "message_id",
        "claim_id",
        "job_id",
        "failure_class",
        "error_type",
        "attempts",
        "terminal",
        "first_failed_at",
        "last_failed_at",
    ]
    assert re.search(r"^\s{4}report_id\s+uuid UNIQUE,", body, re.M)  # 멱등 키 = Spring 조인 키
    assert re.search(r"^\s{4}terminal\s+boolean NOT NULL DEFAULT false,", body, re.M)


def test_migration_check_lists_exactly_the_failure_classes_code_writes() -> None:
    # Arrange: CHECK와 코드가 어긋나면 저널 쓰기가 CHECK 위반으로 실패한다(= 무음 실패)
    sql = _sql_code(_MIGRATION.read_text(encoding="utf-8"))
    match = re.search(r"CHECK \(failure_class IN\s*\(([^)]*)\)\)", sql)
    assert match is not None

    # Act
    allowed = re.findall(r"'(\w+)'", match.group(1))

    # Assert
    assert sorted(allowed) == sorted(member.value for member in FailureClass)


def test_migration_indexes_terminal_rows_only() -> None:
    # Arrange
    sql = _sql_code(_MIGRATION.read_text(encoding="utf-8"))

    # Act
    index = re.search(
        r"CREATE INDEX IF NOT EXISTS report_job_failures_terminal_idx\s+"
        r"ON ai\.report_job_failures \(last_failed_at\) WHERE terminal;",
        sql,
    )

    # Assert: 운영 조회는 확정 행만 본다 — 곧 지워질 비종결 행은 싣지 않는다
    assert index is not None


@pytest.mark.parametrize(
    ("role_guard", "grant"),
    [
        pytest.param(
            "IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_owner') THEN",
            "EXECUTE 'GRANT SELECT ON ai.report_job_failures TO app_owner'",
            id="app_owner",
        ),
        pytest.param(
            "IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_pgaudit') THEN",
            "EXECUTE 'GRANT SELECT ON ai.report_job_failures TO rds_pgaudit'",
            id="rds_pgaudit",
        ),
    ],
)
def test_migration_grants_select_only_when_role_exists(role_guard: str, grant: str) -> None:
    # Arrange
    sql = _sql_code(_MIGRATION.read_text(encoding="utf-8"))

    # Act
    writes = re.findall(r"GRANT\s+(?:INSERT|UPDATE|DELETE|TRUNCATE|ALL)\b", sql)

    # Assert: 역할이 있을 때만 SELECT(로컬 PG엔 역할이 없다), 쓰기 권한은 절대 주지 않는다(§14)
    assert role_guard in sql
    assert grant in sql
    assert writes == []


def test_migration_is_safe_to_reapply_on_every_worker_boot() -> None:
    # Arrange: OCR·report가 기동마다 이 파일을 다시 돈다
    sql = _sql_code(_MIGRATION.read_text(encoding="utf-8"))

    # Act
    statements = re.findall(r"^(?:CREATE|ALTER|DROP|DO)\b[^\n]*", sql, re.M)

    # Assert: IF NOT EXISTS 생성과 DO 블록(GRANT)뿐 — DROP·ALTER(제약 교체 등 강한 잠금)가 없다
    assert statements == [
        "CREATE TABLE IF NOT EXISTS ai.report_job_failures (",
        "CREATE INDEX IF NOT EXISTS report_job_failures_terminal_idx",
        "DO $$",
        "DO $$",
    ]


def test_ai_migration_numbers_stay_unique() -> None:
    # Arrange: 파일명 순서가 곧 적용 순서다 — 번호가 겹치면 순서가 이름 철자에 맡겨진다
    names = sorted(path.name for path in _MIGRATIONS_AI.glob("*.sql"))

    # Act
    numbers = [name.split("_", 1)[0] for name in names]

    # Assert
    assert len(numbers) == len(set(numbers))
    assert "012_report_job_failures.sql" in names


# ── (b) 저널 SQL ──────────────────────────────────────────────
async def test_record_failure_upserts_non_terminal_row_keyed_by_report_id() -> None:
    # Arrange
    pool = _JournalPool()

    # Act
    await record_report_failure(
        pool,  # type: ignore[arg-type]
        _job(),
        failure_class=FailureClass.LLM_UNAVAILABLE,
        error_type="AiClientError",
    )

    # Assert: 컬럼 목록과 $n 순서가 어긋나면 조용히 뒤섞인다 — 인자 순서를 고정한다
    sql, args = pool.calls[0]
    assert "INSERT INTO ai.report_job_failures" in sql
    assert "ON CONFLICT (report_id) DO UPDATE" in sql
    assert args == _upsert_args(FailureClass.LLM_UNAVAILABLE, "AiClientError")


def test_record_failure_sql_accumulates_attempts_and_never_touches_terminal() -> None:
    # Arrange
    sql = failure_journal._UPSERT_FAILURE_SQL
    insert_part, update_part = sql.split("DO UPDATE SET", 1)

    # Act / Assert: 재전달마다 attempts가 쌓이고, terminal은 SET 절에 없어 true→false가 불가능하다
    assert "VALUES ($1, $2, $3, $4, $5, 1, false)" in insert_part
    assert "attempts = report_job_failures.attempts + 1" in update_part
    assert "terminal" not in update_part
    assert "last_failed_at = now()" in update_part  # 시각은 DB 기준(시계 스큐 회피)
    assert "first_failed_at" not in sql  # 최초 실패 시각은 불변(DEFAULT now())


async def test_record_failure_skips_rows_spring_could_not_join() -> None:
    # Arrange: report_id가 UUID가 아니면 조인도 회복 삭제도 못 하는 행이 재전달마다 쌓인다.
    # 그 메시지는 poison 훅이 message_id로 남긴다.
    pool = _JournalPool()

    # Act
    await record_report_failure(
        pool,  # type: ignore[arg-type]
        _job(report_id="r-1"),
        failure_class=FailureClass.CONTEXT_UNAVAILABLE,
        error_type="ValueError",
    )

    # Assert
    assert pool.calls == []


async def test_record_failure_blanks_only_a_malformed_job_id() -> None:
    # Arrange: 부가 식별자 하나 때문에 실패 기록 전체를 잃지 않는다
    pool = _JournalPool()

    # Act
    await record_report_failure(
        pool,  # type: ignore[arg-type]
        _job(job_id="not-a-uuid"),
        failure_class=FailureClass.PERSIST_FAILED,
        error_type="ConnectionError",
    )

    # Assert
    assert pool.calls[0][1] == (
        uuid.UUID(_REPORT_ID),
        _CLAIM_ID,
        None,
        "persist_failed",
        "ConnectionError",
    )


async def test_clear_failure_deletes_by_report_id() -> None:
    # Arrange
    pool = _JournalPool()

    # Act
    await clear_report_failure(pool, _REPORT_ID)  # type: ignore[arg-type]

    # Assert
    assert pool.calls == [
        ("DELETE FROM ai.report_job_failures WHERE report_id = $1", (uuid.UUID(_REPORT_ID),))
    ]


async def test_clear_failure_skips_db_for_malformed_report_id() -> None:
    # Arrange
    pool = _JournalPool()

    # Act
    await clear_report_failure(pool, "r-1")  # type: ignore[arg-type]

    # Assert: 기록될 수 없었던 키라 지울 행도 없다
    assert pool.calls == []


async def test_mark_terminal_pins_terminal_and_keeps_failure_class() -> None:
    # Arrange
    pool = _JournalPool()

    # Act
    await mark_report_failure_terminal(
        pool,  # type: ignore[arg-type]
        job=_job(),
        message_id="m-1",
        receive_count=4,
    )

    # Assert: 재전달 중 남긴 분류(llm_unavailable 등)를 unknown으로 덮지 않는다
    sql, args = pool.calls[0]
    insert_part, update_part = sql.split("DO UPDATE SET", 1)
    assert "ON CONFLICT (report_id)" in insert_part
    assert "'unknown'" in insert_part  # 기록 없이 poison까지 간 경우에만
    assert "terminal = true" in update_part
    assert "failure_class" not in update_part
    assert "attempts = GREATEST(report_job_failures.attempts, EXCLUDED.attempts)" in update_part
    assert args == (uuid.UUID(_REPORT_ID), "m-1", _CLAIM_ID, uuid.UUID(_JOB_ID), 4)


async def test_mark_terminal_without_job_records_schema_invalid_by_message_id() -> None:
    # Arrange: 역직렬화조차 실패한 poison — report_id를 모르니 message_id로만 추적한다
    pool = _JournalPool()

    # Act
    await mark_report_failure_terminal(
        pool,  # type: ignore[arg-type]
        job=None,
        message_id="m-2",
        receive_count=4,
    )

    # Assert: report_id가 NULL이면 UNIQUE가 안 걸려 업서트가 불가능하다 → 순수 INSERT
    sql, args = pool.calls[0]
    assert "'schema_invalid'" in sql
    assert "ON CONFLICT" not in sql
    assert args == ("m-2", None, None, 4)


async def test_mark_terminal_with_unjoinable_report_id_records_schema_invalid() -> None:
    # Arrange: 본문은 파싱됐지만 report_id가 UUID가 아니다(계약 위반) — Spring이 조인할 수 없다
    pool = _JournalPool()

    # Act
    await mark_report_failure_terminal(
        pool,  # type: ignore[arg-type]
        job=_job(report_id="r-1"),
        message_id="m-3",
        receive_count=4,
    )

    # Assert: 추적용 식별자는 남긴다
    assert pool.kinds() == ["unkeyed"]
    assert pool.calls[0][1] == ("m-3", _CLAIM_ID, uuid.UUID(_JOB_ID), 4)


# ── (c) 핸들러: 기록·회복·거절 ─────────────────────────────────
@pytest.mark.parametrize(
    ("final", "failure_class", "error_type"),
    [
        pytest.param(
            {"errors": ["ocr_result_missing"]},
            FailureClass.CONTEXT_UNAVAILABLE,
            None,
            id="no-documents",
        ),
        pytest.param(
            {"errors": ["load_context_failed:PostgresConnectionError"]},
            FailureClass.CONTEXT_UNAVAILABLE,
            "PostgresConnectionError",
            id="load-context-raises",
        ),
        pytest.param(
            {
                "errors": ["rag_empty", "coverage_analysis_failed:AiClientError"],
                "llm_failure": LLM_SYSTEM_FAILURE,
            },
            FailureClass.LLM_UNAVAILABLE,
            "AiClientError",
            id="llm-system-failure",
        ),
        pytest.param(
            {"errors": ["persist_failed:UniqueViolationError"]},
            FailureClass.PERSIST_FAILED,
            "UniqueViolationError",
            id="persist-failed",
        ),
        pytest.param(
            {
                "errors": ["diagnosis_failed:LlmRefusalError", "persist_refused_failed:OSError"],
                "llm_failure": LLM_REFUSED,
            },
            FailureClass.PERSIST_FAILED,
            "OSError",
            id="blocked-write-after-refusal-failed",
        ),
    ],
)
async def test_hard_failure_upserts_classified_non_terminal_row_and_redelivers(
    monkeypatch: pytest.MonkeyPatch,
    final: dict[str, Any],
    failure_class: FailureClass,
    error_type: str | None,
) -> None:
    # Arrange
    pool = _JournalPool()
    _stub_graph(monkeypatch, final)

    # Act
    with pytest.raises(worker.ReportWorkerError) as caught:
        await worker.journaled_handler(pool)(_job())  # type: ignore[arg-type]

    # Assert: 분류대로 한 행, 처분은 그대로 재전달(종결 아님)
    assert pool.calls == [
        (failure_journal._UPSERT_FAILURE_SQL, _upsert_args(failure_class, error_type))
    ]
    assert not isinstance(caught.value, NonRetryableError)


async def test_each_redelivered_hard_failure_upserts_the_same_row_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 운영 한도(3)만큼 같은 실패가 반복된다
    pool = _JournalPool()
    _stub_graph(monkeypatch, _LLM_DOWN, _LLM_DOWN, _LLM_DOWN)
    handle = worker.journaled_handler(pool)  # type: ignore[arg-type]

    # Act
    await _redeliver_failing(handle, _job(), attempts=3)

    # Assert: 새 행이 아니라 같은 report_id 업서트 3회 — attempts는 SQL이 1→2→3으로 쌓는다
    assert pool.kinds() == ["upsert", "upsert", "upsert"]
    assert {args for _, args in pool.calls} == {
        _upsert_args(FailureClass.LLM_UNAVAILABLE, "AiClientError")
    }


async def test_unexpected_exception_is_journaled_as_unknown_and_reraised_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 분류 밖 예외(그래프 런타임 오류 등)도 컨슈머가 재전달한다
    pool = _JournalPool()
    error = KeyError("state")
    _stub_graph(monkeypatch, error)

    # Act
    with pytest.raises(KeyError) as caught:
        await worker.journaled_handler(pool)(_job())  # type: ignore[arg-type]

    # Assert: 클래스명만 남기고, 원래 예외 객체를 그대로 올린다
    assert caught.value is error
    assert pool.calls == [
        (failure_journal._UPSERT_FAILURE_SQL, _upsert_args(FailureClass.UNKNOWN, "KeyError"))
    ]


async def test_needs_reupload_status_write_failure_is_journaled_as_persist_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: NEEDS_REUPLOAD 기록도 저장이다 — 실패하면 그대로 재전달된다
    pool = _JournalPool()
    cause = ConnectionError("db unavailable")

    async def _fail(_report_id: str) -> None:
        raise cause

    monkeypatch.setattr(worker, "mark_needs_reupload", _fail)

    # Act
    with pytest.raises(worker.ReportWorkerError) as caught:
        await worker.journaled_handler(pool)(_job(ocr_quality="needs_reupload"))  # type: ignore[arg-type]

    # Assert: 원인은 __cause__로 남고 저널엔 클래스명만
    assert caught.value.__cause__ is cause
    assert pool.calls == [
        (
            failure_journal._UPSERT_FAILURE_SQL,
            _upsert_args(FailureClass.PERSIST_FAILED, "ConnectionError"),
        )
    ]


@pytest.mark.parametrize(
    "final",
    [
        pytest.param(_SAVED, id="saved"),
        pytest.param({"errors": ["rag_empty"]}, id="partial-saved"),
        pytest.param({"errors": ["input_blocked:off_domain"]}, id="input-blocked"),
    ],
)
async def test_report_reaching_a_final_status_clears_its_journal_row(
    monkeypatch: pytest.MonkeyPatch, final: dict[str, Any]
) -> None:
    # Arrange
    pool = _JournalPool()
    _stub_graph(monkeypatch, final)

    # Act
    await worker.journaled_handler(pool)(_job())  # type: ignore[arg-type]

    # Assert: 이전 시도의 실패 행이 남으면 끝난 리포트가 실패로 조회된다
    assert pool.calls == [(failure_journal._CLEAR_FAILURE_SQL, (uuid.UUID(_REPORT_ID),))]


async def test_needs_reupload_clears_journal_row(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    pool = _JournalPool()

    async def _mark(_report_id: str) -> None:
        return None

    monkeypatch.setattr(worker, "mark_needs_reupload", _mark)

    # Act
    await worker.journaled_handler(pool)(_job(ocr_quality="needs_reupload"))  # type: ignore[arg-type]

    # Assert
    assert pool.kinds() == ["clear"]


async def test_refusal_is_never_journaled(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 거절은 BLOCKED로 이미 사용자에게 보이는 종료 상태다
    pool = _JournalPool()
    _stub_graph(
        monkeypatch,
        {"errors": ["diagnosis_failed:LlmRefusalError"], "llm_failure": LLM_REFUSED},
    )

    # Act
    with pytest.raises(worker.ReportRefusedError) as caught:
        await worker.journaled_handler(pool)(_job())  # type: ignore[arg-type]

    # Assert: 업서트 없음(이전 시도의 행은 지운다), 처분은 그대로 즉시 종결
    assert pool.kinds() == ["clear"]
    assert isinstance(caught.value, NonRetryableError)


async def test_journal_write_failure_keeps_original_failure_and_logs_types_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 저널 DB가 죽어도 메시지 처분(재전달)은 그대로여야 한다
    pool = _JournalPool(fail_on="INSERT")
    _stub_graph(monkeypatch, _LLM_DOWN)

    # Act
    with capture_logs() as logs, pytest.raises(worker.ReportWorkerError):
        await worker.journaled_handler(pool)(_job())  # type: ignore[arg-type]

    # Assert: 저널 예외가 원래 예외를 가리지 않고, 로그엔 타입·분류만 남는다(§9)
    warned = [entry for entry in logs if entry["event"] == "report_job_failure_journal_failed"]
    assert [
        (entry["error_type"], entry["original_error_type"], entry["failure_class"])
        for entry in warned
    ] == [("ConnectionError", "ReportWorkerError", "llm_unavailable")]
    assert "journal db down" not in repr(logs)


async def test_journal_clear_failure_does_not_fail_a_finished_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 삭제가 실패해도 이미 저장된 리포트를 재전달(LLM 재호출)로 되돌리지 않는다
    pool = _JournalPool(fail_on="DELETE")
    _stub_graph(monkeypatch, _SAVED)

    # Act
    with capture_logs() as logs:
        await worker.journaled_handler(pool)(_job())  # type: ignore[arg-type]

    # Assert: 예외 없이 끝났다(= ack). 경고엔 타입만
    warned = [entry for entry in logs if entry["event"] == "report_job_failure_clear_failed"]
    assert [entry["error_type"] for entry in warned] == ["ConnectionError"]
    assert "journal db down" not in repr(logs)


# ── (d) poison 훅 ─────────────────────────────────────────────
async def test_poison_hook_confirms_terminal_with_consumer_arguments() -> None:
    # Arrange
    pool = _JournalPool()
    hook = report_main._poison_journal(pool)  # type: ignore[arg-type]

    # Act
    await hook(_job(), "m-1", 4)

    # Assert
    assert pool.calls == [
        (
            failure_journal._MARK_TERMINAL_SQL,
            (uuid.UUID(_REPORT_ID), "m-1", _CLAIM_ID, uuid.UUID(_JOB_ID), 4),
        )
    ]


async def test_poison_hook_records_unparsable_message_by_message_id() -> None:
    # Arrange
    pool = _JournalPool()
    hook = report_main._poison_journal(pool)  # type: ignore[arg-type]

    # Act
    await hook(None, "m-2", 4)

    # Assert
    assert pool.kinds() == ["unkeyed"]


async def test_poison_hook_propagates_journal_failure() -> None:
    # Arrange: 컨슈머는 훅의 성공 여부로 삭제/보류를 정한다. 훅이 삼키면 기록 없이 지워진다
    pool = _JournalPool(fail_on="INSERT")
    hook = report_main._poison_journal(pool)  # type: ignore[arg-type]

    # Act / Assert
    with pytest.raises(ConnectionError):
        await hook(_job(), "m-3", 4)


# ── (d) 컨슈머 흐름(실제 SqsConsumer, 운영 한도 3) ────────────────
class _FakeSqs:
    """delete_message만 흉내 내는 boto3 SQS 대역. 삭제됐으면 큐에서 걷힌 것이다."""

    def __init__(self) -> None:
        self.deleted: list[str] = []

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> None:
        self.deleted.append(ReceiptHandle)


def _delivery(body: str, receive_count: int) -> dict[str, Any]:
    return {
        "MessageId": "m-1",
        "ReceiptHandle": f"rh-{receive_count}",
        "Body": body,
        "Attributes": {"ApproximateReceiveCount": str(receive_count)},
    }


def _journaled_consumer(pool: _JournalPool, sqs: _FakeSqs) -> SqsConsumer[ReportJob]:
    """운영 배선(__main__)과 같은 핸들러·훅을 단 컨슈머."""
    return SqsConsumer(
        _QUEUE,
        ReportJob,
        worker.journaled_handler(pool),  # type: ignore[arg-type]
        settings=Settings(sqs_max_receive_count=3),
        client=sqs,
        on_poison=report_main._poison_journal(pool),  # type: ignore[arg-type]
    )


async def test_persistent_failure_is_journaled_per_delivery_then_confirmed_on_poison(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: LLM 장애가 한도 내내 이어진다
    pool = _JournalPool()
    sqs = _FakeSqs()
    _stub_graph(monkeypatch, _LLM_DOWN, _LLM_DOWN, _LLM_DOWN)
    consumer = _journaled_consumer(pool, sqs)
    body = _job().model_dump_json()

    # Act: 수신 1~3은 핸들러가 돌고, 4는 poison(핸들러 없이 걷힘)
    await _deliver(consumer, body, 1, 2, 3, 4)

    # Assert: 비종결 3회 → 걷기 직전 확정 1회. 메시지는 poison 때만 지워졌다
    assert pool.kinds() == ["upsert", "upsert", "upsert", "terminal"]
    assert pool.calls[-1][1] == (uuid.UUID(_REPORT_ID), "m-1", _CLAIM_ID, uuid.UUID(_JOB_ID), 4)
    assert sqs.deleted == ["rh-4"]


async def test_recovery_on_redelivery_clears_the_row(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 첫 시도만 LLM 장애, 두 번째에 저장된다
    pool = _JournalPool()
    sqs = _FakeSqs()
    _stub_graph(monkeypatch, _LLM_DOWN, _SAVED)
    consumer = _journaled_consumer(pool, sqs)
    body = _job().model_dump_json()

    # Act
    await _deliver(consumer, body, 1, 2)

    # Assert
    assert pool.kinds() == ["upsert", "clear"]
    assert sqs.deleted == ["rh-2"]


async def test_unparsable_message_is_journaled_once_when_it_turns_poison() -> None:
    # Arrange: 스키마 위반은 핸들러까지 오지 않는다 — poison 때 message_id로 한 번 남긴다
    pool = _JournalPool()
    sqs = _FakeSqs()
    consumer = _journaled_consumer(pool, sqs)

    # Act
    await _deliver(consumer, "{not json", 1, 2, 3, 4)

    # Assert
    assert pool.kinds() == ["unkeyed"]
    assert pool.calls[0][1] == ("m-1", None, None, 4)
    assert sqs.deleted == ["rh-4"]


async def test_poison_hook_failure_keeps_message_for_next_delivery() -> None:
    # Arrange: 확정 기록이 실패하면 지우지 않는다("기록 없이 유실"보다 "며칠 더 도는 것")
    pool = _JournalPool(fail_on="INSERT")
    sqs = _FakeSqs()
    consumer = _journaled_consumer(pool, sqs)

    # Act: 훅 예외가 소비 루프로 새지 않는다(§8)
    await consumer._process(_delivery(_job().model_dump_json(), 4))

    # Assert
    assert sqs.deleted == []


# ── (d) 기동 배선 ─────────────────────────────────────────────
class _BootRecorder:
    """``__main__.main``의 외부 의존(풀·마이그레이션·컨슈머) 대역. 호출 순서를 쌓는다."""

    def __init__(self, pool: _JournalPool, migration_error: Exception | None = None) -> None:
        self.pool = pool
        self.events: list[str] = []
        self.migrations: list[tuple[object, object]] = []
        self.consumer_kwargs: dict[str, Any] = {}
        self._migration_error = migration_error

    async def init_pool(self, _settings: Settings) -> _JournalPool:
        self.events.append("init_pool")
        return self.pool

    async def run_migrations(self, pool: object, migrations_dir: object) -> list[str]:
        self.events.append("run_migrations")
        self.migrations.append((pool, migrations_dir))
        if self._migration_error is not None:
            raise self._migration_error
        return ["012_report_job_failures.sql"]

    async def close_pool(self) -> None:
        self.events.append("close_pool")

    def consumer(self, *args: Any, **kwargs: Any) -> SimpleNamespace:
        self.events.append("consumer")
        self.consumer_kwargs = {"args": args, **kwargs}

        async def run() -> None:
            self.events.append("consume")

        return SimpleNamespace(run=run)


def _install_boot(monkeypatch: pytest.MonkeyPatch, recorder: _BootRecorder) -> None:
    monkeypatch.setattr(report_main, "configure_logging", lambda: None)
    monkeypatch.setattr(
        report_main, "get_settings", lambda: Settings(sqs_report_job_queue_url=_QUEUE)
    )
    monkeypatch.setattr(report_main, "init_pool", recorder.init_pool)
    monkeypatch.setattr(report_main, "run_migrations", recorder.run_migrations)
    monkeypatch.setattr(report_main, "close_pool", recorder.close_pool)
    monkeypatch.setattr(report_main, "SqsConsumer", recorder.consumer)


async def test_boot_applies_ai_migrations_before_consuming(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: GPU EC2(OCR)가 꺼져 있어도 report가 자기 테이블을 보장해야 한다
    recorder = _BootRecorder(_JournalPool())
    _install_boot(monkeypatch, recorder)
    monkeypatch.chdir(_REPO_ROOT)  # _MIGRATIONS_DIR는 작업 디렉터리 기준(이미지 WORKDIR /app)

    # Act
    await report_main.main()

    # Assert: 자기 스키마 디렉터리만(§14), 같은 풀로, 소비 전에 적용하고 끝에 풀을 닫는다
    assert recorder.events == ["init_pool", "run_migrations", "consumer", "consume", "close_pool"]
    assert recorder.migrations == [(recorder.pool, "migrations/ai")]
    assert (_REPO_ROOT / report_main._MIGRATIONS_DIR / _MIGRATION.name).is_file()


async def test_boot_wires_journaled_handler_and_poison_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    recorder = _BootRecorder(_JournalPool())
    _install_boot(monkeypatch, recorder)
    monkeypatch.chdir(_REPO_ROOT)
    _stub_graph(monkeypatch, _LLM_DOWN)
    await report_main.main()
    queue_url, schema, handler = recorder.consumer_kwargs["args"]
    on_poison = recorder.consumer_kwargs["on_poison"]

    # Act: 배선된 핸들러·훅을 직접 돌려 같은 풀에 저널이 남는지 본다
    with pytest.raises(worker.ReportWorkerError):
        await handler(_job())
    await on_poison(None, "m-9", 4)

    # Assert: 맨 handle_job이 아니라 저널을 씌운 핸들러이고, poison 훅이 달려 있다
    assert (queue_url, schema) == (_QUEUE, ReportJob)
    assert recorder.pool.kinds() == ["upsert", "unkeyed"]


async def test_boot_stops_before_consuming_when_migrations_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 저널 없이 돌면 poison이 다시 무음 실패가 된다 — OCR처럼 기동을 멈춘다
    recorder = _BootRecorder(_JournalPool(), migration_error=RuntimeError("permission denied"))
    _install_boot(monkeypatch, recorder)
    monkeypatch.chdir(_REPO_ROOT)

    # Act
    with pytest.raises(RuntimeError):
        await report_main.main()

    # Assert: 소비하지 않고 풀은 닫았다
    assert recorder.events == ["init_pool", "run_migrations", "close_pool"]


async def test_boot_refuses_to_start_without_migrations_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Arrange: 디렉터리가 없으면 run_migrations는 조용히 0개를 적용한다(이미지 COPY 누락 등)
    recorder = _BootRecorder(_JournalPool())
    _install_boot(monkeypatch, recorder)
    monkeypatch.chdir(tmp_path)

    # Act
    with pytest.raises(FileNotFoundError):
        await report_main.main()

    # Assert: 풀도 만들지 않았다
    assert recorder.events == []


def test_report_image_ships_ai_migrations_at_the_boot_relative_path() -> None:
    # Arrange
    dockerfile = (_REPO_ROOT / "src" / "report_worker" / "Dockerfile").read_text(encoding="utf-8")
    runtime = dockerfile.split("AS runtime", 1)[1]

    # Act
    copies = [line.split()[1:] for line in re.findall(r"^COPY (?!--from).+$", runtime, re.M)]

    # Assert: WORKDIR(/app) 기준 migrations/ai에 report 소유 파일만 — OCR용 003·004·006은 매 기동
    # ai.ocr_results를 배타 잠금하므로 싣지 않고, corpus/는 다른 owner라 싣지 않는다(§14)
    assert "WORKDIR /app" in runtime
    assert copies == [
        ["src", "./src"],
        [
            "migrations/ai/007_report_drafts.sql",
            "migrations/ai/012_report_job_failures.sql",
            "./migrations/ai/",
        ],
    ]


def test_report_shipped_migrations_are_rerun_safe() -> None:
    # Arrange: OCR·report가 같은 파일을 각자 반복 적용한다
    shipped = ["007_report_drafts.sql", "012_report_job_failures.sql"]
    sql = [
        (_REPO_ROOT / "migrations" / "ai" / name).read_text(encoding="utf-8") for name in shipped
    ]

    # Act
    risky = [re.findall(r"\b(DROP|ALTER)\b", body.upper()) for body in sql]

    # Assert: 잠금·정의 되돌림 위험이 있는 DROP/ALTER가 없다
    assert risky == [[], []]
