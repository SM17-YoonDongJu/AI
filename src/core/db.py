"""asyncpg 연결 풀 lifecycle — 정본(canonical) 슈퍼셋.

앱 시작 시 1회 풀을 생성해 재사용한다(CODE_CONVENTIONS §7). RDS는 TLS를 요구하므로
`rds_ca_path`가 있으면 CA 번들로 검증하는 SSL 컨텍스트를, 로컬 PG면 SSL을 끈다 — 같은 코드가
로컬·배포 양쪽에서 동작한다. pgvector `vector` 타입을 커넥션마다 등록해 임베딩
(`list[float]` ↔ `vector(1024)`)을 주고받는다(RAG 벡터 검색 필수).

두 사용 패턴을 모두 지원한다:
- 전역 싱글턴: `init_pool()` → `get_pool()` → `close_pool()` (RAG·장수명 서비스)
- 컨텍스트 매니저: `async with db_pool() as pool:` (워커 진입점)
"""

import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from pgvector.asyncpg import register_vector

from core.config import Settings, get_settings
from core.logging import get_logger

logger = get_logger(__name__)

_pool: asyncpg.Pool | None = None


class PoolNotInitializedError(RuntimeError):
    """`init_pool()` 호출 전에 `get_pool()`을 부른 경우."""


def _build_ssl(ca_path: str | None) -> ssl.SSLContext | None:
    """RDS면 CA 번들로 SSL 컨텍스트를, 로컬 PG면 None(SSL 끔)을 반환한다."""
    if not ca_path:
        return None
    return ssl.create_default_context(cafile=ca_path)


async def _init_connection(conn: asyncpg.Connection) -> None:
    """신규 커넥션마다 pgvector 타입을 등록한다(`vector` ↔ `list[float]`)."""
    await register_vector(conn)


async def create_pool(settings: Settings | None = None) -> asyncpg.Pool:
    """asyncpg 풀을 생성한다(SSL·pgvector·pool 크기 반영). 저수준 팩토리."""
    settings = settings or get_settings()
    pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        ssl=_build_ssl(settings.rds_ca_path),
        init=_init_connection,
    )
    logger.info(
        "db pool created",
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
    )
    return pool


async def init_pool(settings: Settings | None = None) -> asyncpg.Pool:
    """전역 풀을 생성한다(앱 시작 1회). 이미 있으면 재사용한다."""
    global _pool
    if _pool is None:
        _pool = await create_pool(settings)
    return _pool


def get_pool() -> asyncpg.Pool:
    """전역 풀을 반환한다. 미초기화 상태면 명확한 예외를 던진다.

    Raises:
        PoolNotInitializedError: `init_pool()` 전에 호출한 경우.
    """
    if _pool is None:
        raise PoolNotInitializedError("init_pool()을 먼저 호출하세요")
    return _pool


async def close_pool() -> None:
    """전역 풀을 종료한다(앱 종료 시). 미초기화면 no-op."""
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None


@asynccontextmanager
async def db_pool(settings: Settings | None = None) -> AsyncIterator[asyncpg.Pool]:
    """풀 수명을 관리하는 async 컨텍스트(워커 진입점용). 종료 시 안전하게 닫는다."""
    pool = await create_pool(settings)
    try:
        yield pool
    finally:
        await pool.close()
        logger.info("db pool closed")
