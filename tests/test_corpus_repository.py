"""corpus_worker.repository 우선순위 큐 SQL 계약 테스트 (이슈 #35 P2).

페이크 풀·커넥션으로 claim(SKIP LOCKED) 트랜잭션과 mark_*·reclaim_stale의 SQL·인자
계약을 검증한다. 실제 상태 전이(CASE 분기·SKIP LOCKED 동시성)는 docker-compose PG로
통합 테스트한다.
"""

import uuid
from typing import Any

from corpus_worker.repository import (
    claim_next_document,
    list_parts,
    mark_document_done,
    mark_document_failed,
    mark_part_uploaded,
    reclaim_stale,
)

PAGE = "33333333-3333-3333-3333-333333333333"
PART0 = "aaaaaaaa-0000-0000-0000-000000000000"
PART1 = "bbbbbbbb-0000-0000-0000-000000000000"


class _FakeTx:
    async def __aenter__(self) -> "_FakeTx":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class FakeConn:
    """asyncpg.Connection 흉내 — claim 트랜잭션의 fetchrow/execute를 기록한다."""

    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row
        self.fetchrow_sql = ""
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> _FakeTx:
        return _FakeTx()

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_sql = sql
        return self._row

    async def execute(self, sql: str, *args: Any) -> str:
        self.executed.append((sql, args))
        return "UPDATE 1"


class _FakeAcquire:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class FakePool:
    """asyncpg.Pool 흉내 — acquire(트랜잭션)·execute·fetch를 함께 지원한다."""

    def __init__(
        self,
        *,
        row: dict[str, Any] | None = None,
        rows: list[dict[str, Any]] | None = None,
        exec_result: str = "UPDATE 0",
    ) -> None:
        self.conn = FakeConn(row)
        self._rows = rows or []
        self._exec_result = exec_result
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.fetch_sql = ""

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)

    async def execute(self, sql: str, *args: Any) -> str:
        self.executed.append((sql, args))
        return self._exec_result

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_sql = sql
        return self._rows


# --------------------------------------------------------------------------- #
# claim_next_document (SKIP LOCKED 트랜잭션)
# --------------------------------------------------------------------------- #


async def test_claim_next_document_selects_priority_and_transitions() -> None:
    # Arrange
    row = {"notion_page_id": uuid.UUID(PAGE), "category": "terms", "part_total": 3}
    pool = FakePool(row=row)

    # Act
    claimed = await claim_next_document(pool)  # type: ignore[arg-type]

    # Assert: 반환 매핑
    assert claimed is not None
    assert claimed.notion_page_id == PAGE
    assert claimed.category == "terms"
    assert claimed.part_total == 3
    # SELECT 계약: pending·우선순위 정렬·SKIP LOCKED
    assert "status = 'pending'" in pool.conn.fetchrow_sql
    assert "ORDER BY priority DESC" in pool.conn.fetchrow_sql
    assert "FOR UPDATE SKIP LOCKED" in pool.conn.fetchrow_sql
    # 백오프 대기 중(next_retry_at 미래)인 문서는 claim 대상에서 제외(retry storm 방지)
    assert "next_retry_at IS NULL OR next_retry_at <= now()" in pool.conn.fetchrow_sql
    # 같은 tx에서 in_progress로 claim 확정
    assert any("status = 'in_progress'" in sql for sql, _ in pool.conn.executed)


async def test_claim_next_document_returns_none_when_queue_empty() -> None:
    # Arrange
    pool = FakePool(row=None)

    # Act / Assert: 없으면 None, 상태 전이도 없음
    assert await claim_next_document(pool) is None  # type: ignore[arg-type]
    assert pool.conn.executed == []


# --------------------------------------------------------------------------- #
# list_parts
# --------------------------------------------------------------------------- #


async def test_list_parts_maps_and_orders_rows() -> None:
    # Arrange
    rows = [
        {"id": uuid.UUID(PART0), "part_order": 0, "sha256": None, "status": "pending"},
        {"id": uuid.UUID(PART1), "part_order": 1, "sha256": "abc", "status": "uploaded"},
    ]
    pool = FakePool(rows=rows)

    # Act
    parts = await list_parts(pool, PAGE)  # type: ignore[arg-type]

    # Assert
    assert [(part.part_order, part.status) for part in parts] == [(0, "pending"), (1, "uploaded")]
    assert parts[0].id == PART0
    assert parts[1].sha256 == "abc"
    assert "ORDER BY part_order" in pool.fetch_sql


# --------------------------------------------------------------------------- #
# mark_* / reclaim_stale SQL 계약
# --------------------------------------------------------------------------- #


async def test_mark_part_uploaded_sets_status_and_meta() -> None:
    # Arrange
    pool = FakePool()

    key = "corpus/terms/sha.pdf"

    # Act
    await mark_part_uploaded(pool, PART0, "sha", key, 123)  # type: ignore[arg-type]

    # Assert
    sql, args = pool.executed[0]
    assert "corpus_file_part" in sql
    assert "status = 'uploaded'" in sql
    assert args == (uuid.UUID(PART0), "sha", key, 123)


async def test_mark_document_done_sets_done_and_part_done() -> None:
    # Arrange
    pool = FakePool()

    # Act
    await mark_document_done(pool, PAGE)  # type: ignore[arg-type]

    # Assert
    sql, args = pool.executed[0]
    assert "status = 'done'" in sql
    assert "part_done = part_total" in sql
    assert args == (uuid.UUID(PAGE),)


async def test_mark_document_failed_encodes_retry_branch() -> None:
    # Arrange
    pool = FakePool()

    # Act
    await mark_document_failed(pool, PAGE, "HTTPError", 5)  # type: ignore[arg-type]

    # Assert: attempts++ 후 상한 도달이면 failed, 아니면 pending(재시도) — CASE로 인코딩
    sql, args = pool.executed[0]
    assert "attempts = attempts + 1" in sql
    assert "CASE WHEN attempts + 1 >= $3 THEN 'failed' ELSE 'pending' END" in sql
    # 재시도는 지수 백오프(30s·2^attempts, 최대 1h)를 next_retry_at에 실어 즉시 재큐 방지
    assert "next_retry_at = CASE" in sql
    assert "make_interval(secs => LEAST(3600, 30 * power(2, attempts)))" in sql
    assert args == (uuid.UUID(PAGE), "HTTPError", 5)


async def test_reclaim_stale_returns_affected_count() -> None:
    # Arrange
    pool = FakePool(exec_result="UPDATE 4")

    # Act
    reclaimed = await reclaim_stale(pool, 900)  # type: ignore[arg-type]

    # Assert
    assert reclaimed == 4
    sql, args = pool.executed[0]
    assert "status = 'in_progress'" in sql
    assert "make_interval(secs => $1)" in sql
    assert "next_retry_at = NULL" in sql  # reclaim 시 백오프 해제 → 즉시 재claim 가능
    assert args == (900.0,)  # 초는 double로 넘긴다
