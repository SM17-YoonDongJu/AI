"""asyncpg 풀 lifecycle.

AWS RDS(pgvector) 접속 풀을 앱 시작 시 1회 생성하고 전 워커가 재사용한다. 각 커넥션에
pgvector 타입을 등록해 `vector` 컬럼을 파이썬 list[float]로 주고받을 수 있게 한다.
"""

import asyncpg
from pgvector.asyncpg import register_vector

from core.config import settings

DEFAULT_MIN_POOL_SIZE = 1
DEFAULT_MAX_POOL_SIZE = 10

_pool: asyncpg.Pool | None = None


class PoolNotInitializedError(RuntimeError):
    """`init_pool()` 호출 전에 풀에 접근했을 때 발생."""


async def _register_vector(conn: asyncpg.Connection) -> None:
    """신규 커넥션마다 pgvector 타입 코덱을 등록한다(asyncpg `init` 콜백)."""
    await register_vector(conn)


async def init_pool() -> asyncpg.Pool:
    """asyncpg 풀을 생성한다(앱 시작 시 1회). 이미 생성되어 있으면 기존 풀을 반환한다.

    Returns:
        생성/재사용된 asyncpg 풀.
    """
    global _pool
    if _pool is not None:
        return _pool
    _pool = await asyncpg.create_pool(
        dsn=settings.db_dsn,
        min_size=DEFAULT_MIN_POOL_SIZE,
        max_size=DEFAULT_MAX_POOL_SIZE,
        init=_register_vector,
    )
    return _pool


def get_pool() -> asyncpg.Pool:
    """초기화된 풀을 반환한다.

    Returns:
        활성 asyncpg 풀.

    Raises:
        PoolNotInitializedError: `init_pool()`이 먼저 호출되지 않은 경우.
    """
    if _pool is None:
        raise PoolNotInitializedError("init_pool()을 먼저 호출하세요.")
    return _pool


async def close_pool() -> None:
    """풀을 우아하게 종료한다(앱 종료 시 1회). 미초기화 상태면 무시한다."""
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None
