"""코퍼스 재임베딩 CLI — 적재된 청크의 embedding을 현재 임베딩 설정으로 다시 채운다.

    python -m rag.reembed --dry-run          # 대상 행 수만 센다(임베딩 호출·쓰기 없음)
    python -m rag.reembed                    # corpus 스키마의 세 테이블 전부
    python -m rag.reembed --schema public    # tempVectorDB(로컬 실험 DB)
    python -m rag.reembed --tables case_chunks --after <chunk_id>   # 중단 지점부터 재개

임베딩 모델을 바꿀 때 쓴다(예: qwen3-embedding → voyage-4-large). policy_chunks·case_chunks·
schedule_chunks는 rag.search에서 같은 쿼리 벡터 하나로 검색된다. 그래서 세 테이블을 모두 같은
모델로 재임베딩해야 한다. 일부만 바꾸거나 쿼리 쪽(EMBEDDING_PROVIDER)을 먼저 바꾸면, 두 모델 모두
1024d라 오류 없이 검색 결과만 틀어진다. 순서는 코퍼스 세 테이블 재임베딩 → 쿼리 쪽 전환이다.

설정은 env에서 읽는다(core.config). 임베딩은 EMBEDDING_PROVIDER·EMBEDDING_BASE_URL·
EMBEDDING_MODEL·EMBEDDING_API_KEY·EMBEDDING_DIM, DB는 DATABASE_URL·RDS_CA_PATH다. 운영 RDS의
corpus 테이블은 corpus_owner 소유이고 report 워커 계정(ai_owner)은 SELECT 권한만 있어 UPDATE가
막힌다. DATABASE_URL은 corpus_owner 접속 URL로 준다.

문서는 content 원문을 그대로 input_type="document"로 임베딩한다. 적재기와 같은 방식이고,
instruction 프리픽스는 쿼리에만 붙는다. 배치마다 트랜잭션으로 커밋하고 마지막 chunk_id를 로그에
남긴다. 중단되면 그 값을 --after로 넘겨 이어서 돌린다. 같은 모델로 다시 임베딩하면 결과가 같아
겹쳐 돌려도 안전하다. content가 비어 임베딩할 수 없는 행은 이전 모델 벡터가 남지 않게 NULL로 비운다.
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import asyncpg
import httpx

from core import ai_client
from core.config import get_settings
from core.db import db_pool
from core.logging import configure_logging, get_logger
from rag.search import RagError

logger = get_logger(__name__)

# 재임베딩 대상 화이트리스트. rag.search가 같은 쿼리 벡터로 검색하는 테이블 전부라 함께 바꾼다.
TARGET_TABLES: tuple[str, ...] = ("policy_chunks", "case_chunks", "schedule_chunks")
# 운영 RDS의 코퍼스 스키마. tempVectorDB는 public이다.
DEFAULT_SCHEMA = "corpus"
# 한 페이지(= 트랜잭션 하나)의 행 수. ai_client.embed_batch의 요청당 입력 수(32)와 맞춘다.
DEFAULT_BATCH_SIZE = 32

# 스키마는 식별자라 $N으로 바인딩할 수 없어 SQL 문자열에 넣는다. 그래서 소문자 비인용 식별자만
# 받는다(PostgreSQL 식별자는 최대 63바이트).
_SCHEMA_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}")

# SQL 템플릿. {target}에는 _qualified_table()이 검증한 `schema.table`만 들어간다. 값(chunk_id·
# 임베딩·LIMIT)은 전부 $N 바인딩이다. 첫 페이지는 커서 조건 없이 읽는다. 빈 문자열 커서로
# 시작하면 chunk_id가 ''인 행을 건너뛰기 때문이다.
_FIRST_PAGE_SQL = "SELECT chunk_id, content FROM {target} ORDER BY chunk_id LIMIT $1"
_NEXT_PAGE_SQL = (
    "SELECT chunk_id, content FROM {target} WHERE chunk_id > $1 ORDER BY chunk_id LIMIT $2"
)
# 임베딩은 list[float]로 바인딩한다. core.db 풀이 커넥션마다 pgvector 코덱(halfvec 포함)을 등록하기
# 때문이다. 적재기처럼 '[v1,...]' 문자열을 넘기면 이 코덱이 문자열을 숫자 배열로 못 바꿔 실패한다.
_UPDATE_SQL = "UPDATE {target} SET embedding = $2 WHERE chunk_id = $1"
_COUNT_SQL = "SELECT count(*) FROM {target}"
_COUNT_AFTER_SQL = "SELECT count(*) FROM {target} WHERE chunk_id > $1"

# 재임베딩을 멈추는 실패: 임베딩 호출, DB 쿼리·접속(PostgresError), 클라이언트 상태, 소켓 끊김.
_ABORT_ERRORS: tuple[type[Exception], ...] = (
    ai_client.AiClientError,
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
    OSError,
)


class ReembedAbortedError(RagError):
    """재임베딩이 도중에 멈췄다. 직전 배치까지는 커밋됐고 재개 커서는 로그에 남겼다."""

    def __init__(self, table: str, resume_after: str | None) -> None:
        super().__init__(f"{table} 재임베딩 중단(재개 커서: {resume_after})")
        self.table = table
        self.resume_after = resume_after


@dataclass(slots=True)
class _Progress:
    """테이블 하나의 진행 상태."""

    table: str
    cursor: str | None  # 마지막으로 커밋된 chunk_id(재개 커서). None이면 처음부터
    updated: int = 0  # 이번 실행에서 커밋된 행 수(NULL로 비운 행 포함)
    nulled: int = 0  # content가 비어 embedding을 NULL로 비운 행 수


def _qualified_table(schema: str, table: str) -> str:
    """검증한 스키마·테이블을 SQL에 넣을 `schema.table`로 만든다.

    Raises:
        ValueError: 화이트리스트 밖 테이블이거나 스키마가 소문자 식별자가 아닌 경우.
    """
    if table not in TARGET_TABLES:
        allowed = ", ".join(TARGET_TABLES)
        raise ValueError(f"재임베딩 대상 테이블이 아니다: {table!r} (허용: {allowed})")
    if not _SCHEMA_RE.fullmatch(schema):
        raise ValueError(f"스키마 이름은 소문자·숫자·밑줄 식별자여야 한다: {schema!r}")
    return f"{schema}.{table}"


def _resolve_tables(schema: str, tables: Sequence[str], after: str | None) -> list[str]:
    """대상 테이블을 검증하고 중복을 뺀다(입력 순서 유지). DB에 닿기 전에 부른다.

    Raises:
        ValueError: 테이블·스키마가 잘못됐거나, 비었거나, after를 여러 테이블에 건 경우.
    """
    resolved = list(dict.fromkeys(tables))
    if not resolved:
        raise ValueError("재임베딩할 테이블이 없다")
    for table in resolved:
        _qualified_table(schema, table)
    if after is not None and len(resolved) != 1:
        # 커서는 테이블마다 다르다. 다른 테이블에 걸리면 그 커서 앞의 행을 조용히 건너뛴다.
        raise ValueError("after(--after)는 테이블을 하나만 지정할 때만 쓸 수 있다")
    return resolved


async def _count_rows(pool: asyncpg.Pool, target: str, after: str | None) -> int:
    """대상 행 수. after가 있으면 그 chunk_id 다음 행만 센다."""
    if after is None:
        return int(await pool.fetchval(_COUNT_SQL.format(target=target)))
    return int(await pool.fetchval(_COUNT_AFTER_SQL.format(target=target), after))


async def _fetch_page(
    pool: asyncpg.Pool, target: str, cursor: str | None, batch_size: int
) -> list[Any]:
    """chunk_id 순 keyset 페이지 하나를 읽는다(cursor 다음부터 batch_size행)."""
    if cursor is None:
        return list(await pool.fetch(_FIRST_PAGE_SQL.format(target=target), batch_size))
    return list(await pool.fetch(_NEXT_PAGE_SQL.format(target=target), cursor, batch_size))


async def _embedding_params(rows: Sequence[Any]) -> list[tuple[str, list[float] | None]]:
    """페이지 행을 문서로 임베딩해 UPDATE 파라미터 (chunk_id, embedding)를 만든다.

    빈 content는 임베딩하지 않고 NULL을 쓴다. 이전 모델 벡터를 남기면 벡터 공간이 섞인다.
    """
    embeddable = [row for row in rows if (row["content"] or "").strip()]
    vectors = await ai_client.embed_batch(
        [row["content"] for row in embeddable], input_type="document"
    )
    by_chunk_id = {row["chunk_id"]: vector for row, vector in zip(embeddable, vectors, strict=True)}
    return [(row["chunk_id"], by_chunk_id.get(row["chunk_id"])) for row in rows]


async def _reembed_table(
    pool: asyncpg.Pool, target: str, progress: _Progress, batch_size: int
) -> None:
    """테이블 하나를 페이지 단위로 재임베딩한다. 페이지마다 트랜잭션 하나로 커밋한다.

    임베딩은 트랜잭션 밖에서 먼저 끝낸다. 외부 API 호출 동안 행 잠금을 쥐지 않기 위해서다.
    """
    total = await _count_rows(pool, target, progress.cursor)
    logger.info(
        "rag.reembed.table_started", table=progress.table, rows=total, after=progress.cursor
    )
    while True:
        rows = await _fetch_page(pool, target, progress.cursor, batch_size)
        if not rows:
            break
        params = await _embedding_params(rows)
        async with pool.acquire() as conn, conn.transaction():
            await conn.executemany(_UPDATE_SQL.format(target=target), params)
        progress.cursor = rows[-1]["chunk_id"]
        progress.updated += len(rows)
        progress.nulled += sum(1 for _, vector in params if vector is None)
        logger.info(
            "rag.reembed.batch_committed",
            table=progress.table,
            updated=progress.updated,
            rows=total,
            nulled=progress.nulled,
            last_chunk_id=progress.cursor,
        )
        if len(rows) < batch_size:
            break
    logger.info(
        "rag.reembed.table_done",
        table=progress.table,
        updated=progress.updated,
        nulled=progress.nulled,
        last_chunk_id=progress.cursor,
    )


def _http_status(exc: BaseException) -> int | None:
    """임베딩 HTTP 오류의 상태 코드(401 키 오류·429 한도 초과 등 구분용). 아니면 None."""
    cause = exc.__cause__
    if isinstance(cause, httpx.HTTPStatusError):
        return cause.response.status_code
    return None


def _log_aborted(progress: _Progress, remaining: Sequence[str], exc: BaseException) -> None:
    """중단 로그. 재개 커서와 남은 테이블을 남긴다. 예외 메시지·본문은 남기지 않는다(§9)."""
    logger.error(
        "rag.reembed.aborted",
        table=progress.table,
        updated=progress.updated,
        resume_after=progress.cursor,
        remaining_tables=list(remaining),
        error_type=type(exc).__name__,
        cause_type=type(exc.__cause__).__name__ if exc.__cause__ is not None else None,
        http_status=_http_status(exc),
    )


async def count_targets(
    pool: asyncpg.Pool,
    *,
    schema: str = DEFAULT_SCHEMA,
    tables: Sequence[str] = TARGET_TABLES,
    after: str | None = None,
) -> dict[str, int]:
    """재임베딩 대상 행 수를 테이블별로 센다(dry-run). 임베딩 호출·쓰기는 하지 않는다.

    Args:
        pool: asyncpg 풀.
        schema: 코퍼스 스키마(운영 corpus, tempVectorDB public).
        tables: 대상 테이블(TARGET_TABLES 안에서). 기본은 전부.
        after: 이 chunk_id 다음 행만 센다. 테이블을 하나만 줄 때만 쓸 수 있다.

    Returns:
        테이블별 대상 행 수.

    Raises:
        ValueError: 스키마·테이블·after 조합이 잘못된 경우(DB 접근 전).
    """
    counts: dict[str, int] = {}
    for table in _resolve_tables(schema, tables, after):
        counts[table] = await _count_rows(pool, _qualified_table(schema, table), after)
        logger.info("rag.reembed.dry_run", table=table, rows=counts[table], after=after)
    return counts


async def reembed(
    pool: asyncpg.Pool,
    *,
    schema: str = DEFAULT_SCHEMA,
    tables: Sequence[str] = TARGET_TABLES,
    batch_size: int = DEFAULT_BATCH_SIZE,
    after: str | None = None,
) -> dict[str, int]:
    """대상 테이블의 embedding을 현재 임베딩 설정으로 다시 채운다(문서 = content 원문).

    테이블마다 chunk_id 순 keyset 페이지(batch_size행)를 읽어 임베딩하고, 페이지 하나를
    트랜잭션 하나로 UPDATE한다. 커밋할 때마다 마지막 chunk_id를 로그에 남긴다.

    Args:
        pool: asyncpg 풀(core.db — pgvector 코덱 등록).
        schema: 코퍼스 스키마(운영 corpus, tempVectorDB public).
        tables: 대상 테이블(TARGET_TABLES 안에서). 기본은 전부.
        batch_size: 페이지(= 트랜잭션) 행 수.
        after: 이 chunk_id 다음부터 재개한다. 테이블을 하나만 줄 때만 쓸 수 있다.

    Returns:
        테이블별 갱신 행 수(content가 비어 NULL로 비운 행 포함).

    Raises:
        ValueError: 스키마·테이블·batch_size·after 조합이 잘못된 경우(DB 접근 전).
        ReembedAbortedError: 임베딩·DB 오류로 멈춘 경우. 직전 배치까지는 커밋돼 있다.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size는 1 이상이어야 한다: {batch_size}")
    targets = _resolve_tables(schema, tables, after)
    updated: dict[str, int] = {}
    for index, table in enumerate(targets):
        progress = _Progress(table=table, cursor=after)
        try:
            await _reembed_table(pool, _qualified_table(schema, table), progress, batch_size)
        except _ABORT_ERRORS as exc:
            _log_aborted(progress, targets[index + 1 :], exc)
            raise ReembedAbortedError(table, progress.cursor) from exc
        updated[table] = progress.updated
    return updated


def _schema_arg(value: str) -> str:
    """--schema 값 검증(argparse type)."""
    if not _SCHEMA_RE.fullmatch(value):
        raise argparse.ArgumentTypeError(f"소문자·숫자·밑줄 식별자여야 한다: {value!r}")
    return value


def _batch_size_arg(value: str) -> int:
    """--batch-size 값 검증(argparse type)."""
    try:
        size = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"정수여야 한다: {value!r}") from exc
    if size < 1:
        raise argparse.ArgumentTypeError(f"1 이상이어야 한다: {size}")
    return size


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """CLI 인자를 읽는다. 잘못된 값은 argparse가 사용법과 함께 종료 코드 2로 끝낸다."""
    parser = argparse.ArgumentParser(
        prog="python -m rag.reembed",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--schema",
        type=_schema_arg,
        default=DEFAULT_SCHEMA,
        help="코퍼스 스키마. 기본 %(default)s(운영 RDS), tempVectorDB는 public",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=TARGET_TABLES,
        default=list(TARGET_TABLES),
        metavar="TABLE",
        help=(
            f"대상 테이블. 기본은 전부({', '.join(TARGET_TABLES)}) — "
            "같은 쿼리 벡터로 검색되므로 모두 같은 모델로 바꿔야 한다"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=_batch_size_arg,
        default=DEFAULT_BATCH_SIZE,
        help="한 트랜잭션에 쓰는 행 수(기본 %(default)s). 요청당 토큰 한도 오류가 나면 줄인다",
    )
    parser.add_argument(
        "--after",
        metavar="CHUNK_ID",
        help="이 chunk_id 다음부터 재개한다(중단 로그의 resume_after). --tables를 하나만 줄 때",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="대상 행 수만 센다(임베딩 호출·쓰기 없음)"
    )
    args = parser.parse_args(argv)
    if args.after is not None and len(set(args.tables)) != 1:
        parser.error("--after는 --tables에 테이블을 하나만 줄 때만 쓸 수 있다")
    return args


async def _run(args: argparse.Namespace) -> None:
    """설정을 로그로 남기고 DB 풀을 열어 dry-run 또는 재임베딩을 실행한다."""
    settings = get_settings()
    logger.info(
        "rag.reembed.started",
        embedding_provider=settings.embedding_provider,
        embedding_model=settings.embedding_model,
        embedding_dim=settings.embedding_dim,
        schema=args.schema,
        tables=args.tables,
        batch_size=args.batch_size,
        after=args.after,
        dry_run=args.dry_run,
    )
    try:
        async with db_pool(settings) as pool:
            if args.dry_run:
                await count_targets(pool, schema=args.schema, tables=args.tables, after=args.after)
                return
            updated = await reembed(
                pool,
                schema=args.schema,
                tables=args.tables,
                batch_size=args.batch_size,
                after=args.after,
            )
            logger.info("rag.reembed.completed", updated=updated)
    finally:
        await ai_client.close_client()


def main(argv: Sequence[str] | None = None) -> int:
    """CLI 진입점.

    Returns:
        종료 코드. 0=완료, 1=중단·실패(재개 커서는 로그에 있다). 인자 오류는 argparse가 2로 끝낸다.
    """
    args = _parse_args(argv)
    configure_logging()
    try:
        asyncio.run(_run(args))
    except ReembedAbortedError:
        return 1  # 재개 커서는 rag.reembed.aborted 로그에 남겼다
    except _ABORT_ERRORS as exc:
        # 테이블 처리 전 실패(DB 접속·dry-run 집계 등) — 쓴 행이 없다.
        logger.error("rag.reembed.failed", error_type=type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
