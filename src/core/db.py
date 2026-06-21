"""asyncpg 연결 풀 lifecycle.

앱 시작 시 1회 풀을 생성해 재사용한다(CODE_CONVENTIONS §7). RDS는 TLS를 요구하므로
`rds_ca_path`가 설정되면 CA 번들로 검증하는 SSL 컨텍스트를 쓰고, 로컬 PG면 SSL을 끈다 —
같은 코드가 로컬·배포 양쪽에서 동작한다.
"""

import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg

from core.config import Settings, get_settings
from core.logging import get_logger

logger = get_logger(__name__)


def _build_ssl(ca_path: str | None) -> ssl.SSLContext | None:
    """RDS면 CA 번들로 SSL 컨텍스트를, 로컬 PG면 None(SSL 끔)을 반환한다."""
    if not ca_path:
        return None
    return ssl.create_default_context(cafile=ca_path)


async def create_pool(settings: Settings | None = None) -> asyncpg.Pool:
    """asyncpg 풀을 생성한다. 앱 시작 시 1회만 호출한다."""
    settings = settings or get_settings()
    pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        ssl=_build_ssl(settings.rds_ca_path),
    )
    logger.info(
        "db pool created",
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
    )
    return pool


@asynccontextmanager
async def db_pool(settings: Settings | None = None) -> AsyncIterator[asyncpg.Pool]:
    """풀 수명을 관리하는 async 컨텍스트. 종료 시 연결을 안전하게 닫는다."""
    pool = await create_pool(settings)
    try:
        yield pool
    finally:
        await pool.close()
        logger.info("db pool closed")
