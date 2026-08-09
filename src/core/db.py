"""asyncpg 연결 풀 lifecycle — 정본(canonical) 슈퍼셋.

앱 시작 시 1회 풀을 생성해 재사용한다(CODE_CONVENTIONS §7). RDS는 TLS를 요구하므로
`rds_ca_path`가 있으면 CA 번들로 검증하는 SSL 컨텍스트를, 로컬 PG면 SSL을 끈다 — 같은 코드가
로컬·배포 양쪽에서 동작한다. pgvector `vector` 타입을 커넥션마다 등록해 임베딩
(`list[float]` ↔ `vector(1024)`)을 주고받는다(RAG 벡터 검색 필수).

두 사용 패턴을 모두 지원한다:
- 전역 싱글턴: `init_pool()` → `get_pool()` → `close_pool()` (RAG·장수명 서비스)
- 컨텍스트 매니저: `async with db_pool() as pool:` (워커 진입점)
"""

import asyncio
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from pgvector.asyncpg import register_vector

from core.config import Settings, get_settings
from core.logging import get_logger

logger = get_logger(__name__)

_pool: asyncpg.Pool | None = None


class PoolNotInitializedError(RuntimeError):
    """`init_pool()` 호출 전에 `get_pool()`을 부른 경우."""


def _build_ssl(ca_path: str | None) -> ssl.SSLContext | bool:
    """RDS면 CA 번들로 SSL 컨텍스트를, 로컬 PG면 False(SSL 완전 비활성)를 반환한다.

    asyncpg에서 ``ssl=None``은 'prefer'(SSL 우선 시도)라 로컬 PG에서도 클라이언트
    인증서 자동탐색(``~/.postgresql/postgresql.crt``)을 시도한다 — 홈 경로가 비-ASCII면
    그 과정에서 ``load_cert_chain``이 실패할 수 있다. CA 경로가 없으면 명시적으로
    ``False``를 반환해 SSL을 끈다(로컬 개발). RDS는 항상 ``rds_ca_path``가 있어 검증한다.
    """
    if not ca_path:
        return False
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


def _load_sql_files(migrations_dir: str | Path) -> list[tuple[str, str]]:
    """``*.sql``을 파일명 오름차순으로 읽어 ``(name, sql)`` 목록으로 돌려준다(동기 I/O)."""
    directory = Path(migrations_dir)
    return [
        (path.name, path.read_text(encoding="utf-8")) for path in sorted(directory.glob("*.sql"))
    ]


async def run_migrations(pool: asyncpg.Pool, migrations_dir: str | Path) -> list[str]:
    """``migrations_dir``의 ``*.sql``을 파일명 오름차순으로 실행한다.

    로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다(이슈 #19). 각 파일의
    DDL은 ``IF NOT EXISTS`` 기반이라 반복 적용해도 안전하다 — 별도 버전 추적 테이블은
    후속 과제로 남기고, 지금은 멱등 DDL로 충분하다. 파라미터 없는 실행이라 한 파일에
    여러 문(semicolon)을 담을 수 있다(asyncpg simple query). 파일 읽기(블로킹)는
    스레드로 격리한다(§7).

    Args:
        pool: asyncpg 연결 풀.
        migrations_dir: ``.sql`` 파일이 있는 디렉터리.

    Returns:
        적용한 파일명 목록(적용 순서).
    """
    sql_files = await asyncio.to_thread(_load_sql_files, migrations_dir)
    applied: list[str] = []
    for name, sql in sql_files:
        await pool.execute(sql)
        applied.append(name)
        logger.info("migration applied", file=name)
    return applied


@asynccontextmanager
async def db_pool(settings: Settings | None = None) -> AsyncIterator[asyncpg.Pool]:
    """풀 수명을 관리하는 async 컨텍스트(워커 진입점용). 종료 시 안전하게 닫는다."""
    pool = await create_pool(settings)
    try:
        yield pool
    finally:
        await pool.close()
        logger.info("db pool closed")
