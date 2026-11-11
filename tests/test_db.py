"""db.py 풀 lifecycle 테스트.

실제 PG 연결 없이(외부 의존 격리) 미초기화 상태의 계약만 검증한다. 실제 풀 생성/쿼리는
docker-compose.dev 기동 후 통합 테스트로 다룬다.
"""

from pathlib import Path
from typing import Any

import pytest

from core import db


class _FakeConn:
    """asyncpg 연결 흉내 — 인자 없는 SQL(마이그레이션)과 인자 있는 SQL(잠금)을 나눠 포착한다."""

    def __init__(self, pool: "_FakePool") -> None:
        self._pool = pool

    async def execute(self, sql: str, *args: Any) -> str:
        if args:
            self._pool.lock_calls.append((sql, args))
            return "OK"
        if sql == self._pool.fail_on:
            raise RuntimeError("migration failed")
        self._pool.executed.append(sql)
        return "OK"


class _Acquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakePool:
    """asyncpg.Pool 흉내 — acquire()한 연결에서 실행된 SQL을 순서대로 포착한다."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.executed: list[str] = []
        self.lock_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fail_on = fail_on

    def acquire(self) -> _Acquire:
        return _Acquire(_FakeConn(self))


async def test_run_migrations_applies_sql_in_name_order(tmp_path: Path) -> None:
    # Arrange: 이름 순서와 실행 순서가 어긋나도록 일부러 역순 생성
    (tmp_path / "002_second.sql").write_text("SELECT 2;", encoding="utf-8")
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("무시되어야 함", encoding="utf-8")
    pool = _FakePool()

    # Act
    applied = await db.run_migrations(pool, tmp_path)  # type: ignore[arg-type]

    # Assert: .sql만, 파일명 오름차순으로 적용
    assert applied == ["001_first.sql", "002_second.sql"]
    assert pool.executed == ["SELECT 1;", "SELECT 2;"]


async def test_run_migrations_empty_dir_is_noop(tmp_path: Path) -> None:
    # Arrange
    pool = _FakePool()

    # Act
    applied = await db.run_migrations(pool, tmp_path)  # type: ignore[arg-type]

    # Assert
    assert applied == []
    assert pool.executed == []


async def test_run_migrations_holds_directory_lock_around_files(tmp_path: Path) -> None:
    # Arrange: OCR·report가 같은 디렉터리를 동시에 적용할 수 있다
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    pool = _FakePool()

    # Act
    await db.run_migrations(pool, tmp_path)  # type: ignore[arg-type]

    # Assert: 디렉터리 이름 기준 잠금을 잡고 적용한 뒤 푼다
    key = f"migrations:{tmp_path.name}"
    assert pool.lock_calls == [
        (db._MIGRATION_LOCK_SQL, (key,)),
        (db._MIGRATION_UNLOCK_SQL, (key,)),
    ]


async def test_run_migrations_releases_lock_when_a_file_fails(tmp_path: Path) -> None:
    # Arrange
    (tmp_path / "001_bad.sql").write_text("BROKEN;", encoding="utf-8")
    pool = _FakePool(fail_on="BROKEN;")

    # Act / Assert: 실패는 그대로 올리되 잠금은 남기지 않는다
    with pytest.raises(RuntimeError):
        await db.run_migrations(pool, tmp_path)  # type: ignore[arg-type]
    assert [sql for sql, _ in pool.lock_calls] == [db._MIGRATION_LOCK_SQL, db._MIGRATION_UNLOCK_SQL]


def test_get_pool_before_init_raises() -> None:
    # Arrange: 모듈 전역 풀이 미초기화 상태인지 보장
    db._pool = None

    # Act / Assert
    with pytest.raises(db.PoolNotInitializedError):
        db.get_pool()


async def test_close_pool_is_noop_when_uninitialized() -> None:
    # Arrange
    db._pool = None

    # Act (예외 없이 무시되어야 함)
    await db.close_pool()

    # Assert
    assert db._pool is None
