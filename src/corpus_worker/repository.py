"""corpus_* 미러 테이블 영속화 (이슈 #35 P1) — notion_page_id 멱등 업서트.

Notion에서 파싱·매핑한 레코드를 corpus_source·corpus_file·corpus_file_part에 멱등
반영한다(순수 I/O 경계). 동기화는 **Notion 메타만** 갱신하고, 파생·상태 컬럼
(demand_score·demand_last_at·priority·status·attempts·part_done·fail_reason)은
건드리지 않는다 — 우선순위/상태는 별도 경로(priority.compute·우선순위 큐)가 관리한다.

설계 메모:
- **멱등**: 재동기화 시 같은 ``notion_page_id``는 한 행만 유지한다(ON CONFLICT DO UPDATE).
- **하드 삭제 금지**: Notion에서 사라진 행은 삭제하지 않고 ``status='archived'``로 전이한다.
- **증분 커서**: 별도 커서 테이블 없이 ``max(notion_last_edited)``로 다음 사이클 기준을 잡는다.
"""

import uuid
from dataclasses import dataclass
from datetime import date, datetime

import asyncpg

from core.exceptions import CorpusSyncError
from core.logging import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class SourceRecord:
    """``corpus_source`` 한 행(출처 카탈로그 미러, Notion 메타)."""

    notion_page_id: str
    name: str
    category: str | None = None
    priority_tier: str | None = None
    phase: str | None = None
    access_method: str | None = None
    mvp_tier: str | None = None
    legal_review: bool = False
    license: str | None = None
    reliability: int | None = None
    operator: str | None = None
    source_url: str | None = None
    memo: str | None = None
    notion_last_edited: datetime | None = None


@dataclass(slots=True)
class FileRecord:
    """``corpus_file``의 Notion 메타 부분(파생·상태 컬럼은 제외)."""

    notion_page_id: str
    source_id: str | None = None
    category: str = "terms"
    product_name: str | None = None
    company: str | None = None
    product_type: str | None = None
    product_code: str | None = None
    version_no: str | None = None
    effective_date: date | None = None
    license: str | None = None
    source_url: str | None = None
    is_latest: bool | None = None
    collect_status: str | None = None
    part_total: int = 0
    notion_last_edited: datetime | None = None


@dataclass(slots=True)
class PartRecord:
    """``corpus_file_part`` 한 파트의 Notion 메타(이름·순서만)."""

    part_order: int
    notion_file_name: str | None = None


_UPSERT_SOURCE_SQL = """
INSERT INTO corpus_source (
    notion_page_id, name, category, priority_tier, phase, access_method,
    mvp_tier, legal_review, license, reliability, operator, source_url, memo,
    notion_last_edited, synced_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now())
ON CONFLICT (notion_page_id) DO UPDATE SET
    name = EXCLUDED.name,
    category = EXCLUDED.category,
    priority_tier = EXCLUDED.priority_tier,
    phase = EXCLUDED.phase,
    access_method = EXCLUDED.access_method,
    mvp_tier = EXCLUDED.mvp_tier,
    legal_review = EXCLUDED.legal_review,
    license = EXCLUDED.license,
    reliability = EXCLUDED.reliability,
    operator = EXCLUDED.operator,
    source_url = EXCLUDED.source_url,
    memo = EXCLUDED.memo,
    notion_last_edited = EXCLUDED.notion_last_edited,
    synced_at = now()
"""

# 파생·상태 컬럼(demand_score, demand_last_at, priority, status, attempts, part_done,
# fail_reason)은 DO UPDATE에서 의도적으로 제외한다 — 동기화가 큐/수요 상태를 덮어쓰지 않게.
_UPSERT_FILE_SQL = """
INSERT INTO corpus_file (
    notion_page_id, source_id, category, product_name, company, product_type,
    product_code, version_no, effective_date, license, source_url, is_latest,
    collect_status, part_total, notion_last_edited, synced_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, now(), now()
)
ON CONFLICT (notion_page_id) DO UPDATE SET
    source_id = EXCLUDED.source_id,
    category = EXCLUDED.category,
    product_name = EXCLUDED.product_name,
    company = EXCLUDED.company,
    product_type = EXCLUDED.product_type,
    product_code = EXCLUDED.product_code,
    version_no = EXCLUDED.version_no,
    effective_date = EXCLUDED.effective_date,
    license = EXCLUDED.license,
    source_url = EXCLUDED.source_url,
    is_latest = EXCLUDED.is_latest,
    collect_status = EXCLUDED.collect_status,
    part_total = EXCLUDED.part_total,
    notion_last_edited = EXCLUDED.notion_last_edited,
    synced_at = now(),
    updated_at = now()
"""

_UPDATE_PRIORITY_SQL = """
UPDATE corpus_file SET priority = $2, updated_at = now() WHERE notion_page_id = $1
"""

_UPSERT_PART_SQL = """
INSERT INTO corpus_file_part (file_page_id, part_order, notion_file_name, status)
VALUES ($1, $2, $3, 'pending')
ON CONFLICT (file_page_id, part_order) DO UPDATE SET
    notion_file_name = EXCLUDED.notion_file_name
"""

# Notion에서 파트 수가 줄면 초과 순서의 잔여 파트를 정리한다.
_DELETE_EXTRA_PARTS_SQL = """
DELETE FROM corpus_file_part WHERE file_page_id = $1 AND part_order >= $2
"""

_MAX_FILE_EDITED_SQL = "SELECT max(notion_last_edited) AS cursor FROM corpus_file"

# 아카이브 지원 테이블(status 컬럼 보유). corpus_source는 status가 없어 제외한다.
_ARCHIVE_SQL_BY_TABLE = {
    "corpus_file": """
        UPDATE corpus_file SET status = 'archived', updated_at = now()
        WHERE status <> 'archived' AND NOT (notion_page_id = ANY($1::uuid[]))
    """,
}


async def upsert_source(pool: asyncpg.Pool, record: SourceRecord) -> None:
    """``corpus_source``에 멱등 업서트한다(notion_page_id 충돌 시 갱신).

    Args:
        pool: asyncpg 연결 풀(core.db).
        record: 저장할 출처 메타.
    """
    await pool.execute(
        _UPSERT_SOURCE_SQL,
        _uuid(record.notion_page_id),
        record.name,
        record.category,
        record.priority_tier,
        record.phase,
        record.access_method,
        record.mvp_tier,
        record.legal_review,
        record.license,
        record.reliability,
        record.operator,
        record.source_url,
        record.memo,
        record.notion_last_edited,
    )


async def upsert_file(pool: asyncpg.Pool, record: FileRecord) -> None:
    """``corpus_file``에 Notion 메타를 멱등 업서트한다(파생·상태 컬럼 미변경).

    Args:
        pool: asyncpg 연결 풀(core.db).
        record: 저장할 약관 문서 메타.
    """
    await pool.execute(
        _UPSERT_FILE_SQL,
        _uuid(record.notion_page_id),
        _uuid(record.source_id) if record.source_id else None,
        record.category,
        record.product_name,
        record.company,
        record.product_type,
        record.product_code,
        record.version_no,
        record.effective_date,
        record.license,
        record.source_url,
        record.is_latest,
        record.collect_status,
        record.part_total,
        record.notion_last_edited,
    )


async def update_priority(pool: asyncpg.Pool, file_page_id: str, priority: float) -> None:
    """``corpus_file.priority``만 갱신한다(동기화의 우선순위 반영 경로).

    Args:
        pool: asyncpg 연결 풀(core.db).
        file_page_id: 대상 약관 문서 Notion page id.
        priority: priority.compute 결과 점수.
    """
    await pool.execute(_UPDATE_PRIORITY_SQL, _uuid(file_page_id), priority)


async def sync_parts(
    pool: asyncpg.Pool, file_page_id: str, parts: list[PartRecord]
) -> None:
    """한 약관 문서의 파트를 (file_page_id, part_order) 기준 멱등 동기화한다.

    이름·순서만 반영하고 sha256/s3_key/status(업로드 상태)는 건드리지 않는다. Notion에서
    파트 수가 줄면 초과 순서의 잔여 행을 정리한다.

    Args:
        pool: asyncpg 연결 풀(core.db).
        file_page_id: 소속 약관 문서 Notion page id.
        parts: 현재 첨부 파트 목록(순서·이름).
    """
    file_id = _uuid(file_page_id)
    for part in parts:
        await pool.execute(_UPSERT_PART_SQL, file_id, part.part_order, part.notion_file_name)
    await pool.execute(_DELETE_EXTRA_PARTS_SQL, file_id, len(parts))


async def archive_missing(
    pool: asyncpg.Pool, data_source: str, seen_ids: set[str]
) -> int:
    """이번 사이클에 안 보인 기존 행을 ``status='archived'``로 전이한다(하드 삭제 금지).

    부분(증분) 조회로는 삭제를 판별할 수 없으므로, 호출자는 전체 조회(full sync)에서만
    호출해야 한다. ``seen_ids``가 비면 전수 아카이브 사고를 막기 위해 no-op으로 둔다.

    Args:
        pool: asyncpg 연결 풀(core.db).
        data_source: 대상 테이블명(status 컬럼 보유 테이블만 지원 — ``corpus_file``).
        seen_ids: 이번 사이클에 관측한 notion_page_id 집합.

    Returns:
        아카이브된 행 수.

    Raises:
        CorpusSyncError: 아카이브를 지원하지 않는 테이블을 지정한 경우.
    """
    sql = _ARCHIVE_SQL_BY_TABLE.get(data_source)
    if sql is None:
        raise CorpusSyncError(f"아카이브 미지원 테이블: {data_source}")
    if not seen_ids:
        return 0
    result = await pool.execute(sql, [_uuid(page_id) for page_id in seen_ids])
    archived = _row_count(result)
    logger.info("corpus_archive_missing", table=data_source, archived=archived)
    return archived


async def get_file_cursor(pool: asyncpg.Pool) -> datetime | None:
    """다음 증분 동기화 기준 시각(= ``max(corpus_file.notion_last_edited)``)을 조회한다.

    Args:
        pool: asyncpg 연결 풀(core.db).

    Returns:
        가장 최근 ``notion_last_edited``. 미러가 비어 있으면 ``None``(전체 조회 신호).
    """
    row = await pool.fetchrow(_MAX_FILE_EDITED_SQL)
    return row["cursor"] if row else None


def _uuid(value: str) -> uuid.UUID:
    """문자열 page id를 uuid 컬럼 코덱 모호성 없이 넘길 UUID로 변환한다."""
    return uuid.UUID(value)


def _row_count(status: str) -> int:
    """asyncpg 명령 상태 문자열(예: ``"UPDATE 5"``)에서 영향 행 수를 파싱한다."""
    tail = status.split()[-1] if status else ""
    return int(tail) if tail.isdigit() else 0
