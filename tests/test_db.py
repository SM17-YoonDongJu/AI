"""db.py 풀 lifecycle 테스트.

실제 PG 연결 없이(외부 의존 격리) 미초기화 상태의 계약만 검증한다. 실제 풀 생성/쿼리는
docker-compose.dev 기동 후 통합 테스트로 다룬다.
"""

from pathlib import Path
from typing import Any

import pytest

from core import db


class _FakePool:
    """asyncpg.Pool.execute 흉내 — 실행된 SQL을 순서대로 포착한다."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    async def execute(self, sql: str, *args: Any) -> str:
        self.executed.append(sql)
        return "OK"


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
