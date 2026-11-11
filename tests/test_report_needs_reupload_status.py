"""agents.mark_needs_reupload가 reports.status='NEEDS_REUPLOAD'만 갱신하는지 검증
(페이크 DB 풀로 외부 의존 격리, persist_blocked 테스트와 같은 패턴)."""

import uuid

import pytest

from report_worker.nodes import agents


class _FakeConn:
    def __init__(self, executed: list[tuple[str, tuple]]) -> None:
        self.executed = executed

    async def execute(self, query: str, *args: object) -> None:
        self.executed.append((query, args))


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePool:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self._conn = _FakeConn(self.executed)

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._conn)


async def test_mark_needs_reupload_updates_status_only(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    pool = _FakePool()
    monkeypatch.setattr(agents.db, "get_pool", lambda: pool)
    report_id = str(uuid.uuid4())

    # Act
    await agents.mark_needs_reupload(report_id)

    # Assert: status='NEEDS_REUPLOAD' UPDATE 1회, 대상은 이 report_id
    assert len(pool.executed) == 1
    query, args = pool.executed[0]
    assert "status = 'NEEDS_REUPLOAD'" in query
    assert args == (uuid.UUID(report_id),)


async def test_mark_needs_reupload_propagates_db_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: DB 쓰기가 실패하는 상황
    class _FailingConn:
        async def execute(self, query: str, *args: object) -> None:
            raise RuntimeError("db unavailable")

    class _FailingAcquire:
        async def __aenter__(self) -> _FailingConn:
            return _FailingConn()

        async def __aexit__(self, *exc: object) -> bool:
            return False

    class _FailingPool:
        def acquire(self) -> _FailingAcquire:
            return _FailingAcquire()

    monkeypatch.setattr(agents.db, "get_pool", lambda: _FailingPool())

    # Act / Assert: 삼키지 않고 그대로 올라간다(무음 실패 방지)
    with pytest.raises(RuntimeError):
        await agents.mark_needs_reupload(str(uuid.uuid4()))
