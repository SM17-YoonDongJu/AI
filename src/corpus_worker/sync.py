"""카탈로그 동기화 한 사이클 오케스트레이션 (이슈 #35 P1).

Notion 두 데이터베이스를 PG 미러로 반영하는 한 사이클을 조립한다. 부수효과(HTTP 조회·DB
업서트)와 순수 매핑 로직(Notion 한글 property → 영문 컬럼)을 분리해 매핑·필터·skip 규칙을
DB·네트워크 없이 단위테스트할 수 있게 한다.

사이클 흐름:
1. 카탈로그 전체 조회 → category ∈ corpus_categories 필터 → ``corpus_source`` 업서트.
2. 약관 파일 조회(기본 증분: max(notion_last_edited) 이후 / ``full=True``: 전수) →
   첨부 있는 행만 → ``corpus_file`` 업서트 + 파트 동기화 + priority 반영.
3. (``full=True`` 재조정에서만) 미노출 파일을 ``archived``로 전이해 삭제를 반영한다.

부분실패 격리: 한 행 처리 실패는 로깅 후 건너뛰고 사이클을 계속한다(구체 예외만 포착).
"""

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

import asyncpg

from core.config import Settings, get_settings
from core.exceptions import CorpusSyncError
from core.logging import get_logger
from corpus_worker import priority
from corpus_worker.notion_source import NotionSource, parse_props
from corpus_worker.repository import (
    FileRecord,
    PartRecord,
    SourceRecord,
    archive_missing,
    get_file_cursor,
    sync_parts,
    update_priority,
    upsert_file,
    upsert_source,
)

logger = get_logger(__name__)

DEFAULT_FILE_CATEGORY = "terms"
_ARCHIVE_TABLE = "corpus_file"

# --- "데이터 출처 카탈로그" Notion property 이름 → corpus_source ---
_CAT_TITLE = "이름"
_CAT_CATEGORY = "카테고리"
_CAT_PRIORITY_TIER = "우선순위"
_CAT_PHASE = "수집단계"
_CAT_ACCESS = "접근방식"
_CAT_MVP = "MVP"
_CAT_LEGAL_REVIEW = "법률검토필요"
_CAT_LICENSE = "라이선스"
_CAT_RELIABILITY = "신뢰도"
_CAT_OPERATOR = "운영주체"
_CAT_URL = "URL"
_CAT_MEMO = "메모"

# --- "보험 약관 파일" Notion property 이름 → corpus_file ---
_FILE_PRODUCT_NAME = "상품명"
_FILE_COMPANY = "회사"
_FILE_PRODUCT_TYPE = "상품종류"
_FILE_PRODUCT_CODE = "상품코드"
_FILE_VERSION_NO = "버전번호"
_FILE_EFFECTIVE_DATE = "시행일"
_FILE_LICENSE = "라이선스"
_FILE_SOURCE_URL = "출처URL"
_FILE_IS_LATEST = "최신판"
_FILE_COLLECT_STATUS = "수집상태"
_FILE_SOURCE_RELATION = "출처"
_FILE_ATTACHMENTS = "파일"


@dataclass(slots=True)
class SyncStats:
    """한 사이클 결과 요약."""

    sources_synced: int = 0
    files_synced: int = 0
    files_skipped: int = 0
    archived: int = 0


# --------------------------------------------------------------------------- #
# 순수 로직 — 파싱·매핑·필터(네트워크·DB 없이 테스트 가능)
# --------------------------------------------------------------------------- #


def parse_categories(raw: str) -> set[str]:
    """CSV 카테고리 필터 문자열을 집합으로 파싱한다(공백·빈 토큰 제거)."""
    return {token.strip() for token in raw.split(",") if token.strip()}


def category_allowed(category: str | None, allowed: set[str]) -> bool:
    """카테고리가 허용 집합에 드는지 판정한다(None·미지정은 제외)."""
    return category in allowed


def has_attachment(parsed: dict[str, Any]) -> bool:
    """약관 파일 행에 ``파일`` 첨부가 하나라도 있는지 판정한다."""
    return bool(parsed.get(_FILE_ATTACHMENTS))


def parse_reliability(value: str | None) -> int | None:
    """신뢰도 select 값("1"/"2")을 정수로 파싱한다(파싱 불가·None → None)."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_notion_datetime(value: str | None) -> datetime | None:
    """Notion ISO-8601 타임스탬프(``...Z``)를 tz-aware datetime으로 파싱한다."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_notion_date(value: str | None) -> date | None:
    """Notion date start(날짜 또는 datetime)를 날짜로 파싱한다(앞 10자)."""
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def map_source(parsed: dict[str, Any]) -> SourceRecord:
    """파싱된 카탈로그 행을 ``SourceRecord``로 매핑한다(한글 property → 영문 컬럼)."""
    return SourceRecord(
        notion_page_id=parsed["id"],
        name=parsed.get(_CAT_TITLE) or "",
        category=parsed.get(_CAT_CATEGORY),
        priority_tier=parsed.get(_CAT_PRIORITY_TIER),
        phase=parsed.get(_CAT_PHASE),
        access_method=parsed.get(_CAT_ACCESS),
        mvp_tier=parsed.get(_CAT_MVP),
        legal_review=bool(parsed.get(_CAT_LEGAL_REVIEW)),
        license=parsed.get(_CAT_LICENSE),
        reliability=parse_reliability(parsed.get(_CAT_RELIABILITY)),
        operator=parsed.get(_CAT_OPERATOR),
        source_url=parsed.get(_CAT_URL),
        memo=parsed.get(_CAT_MEMO),
        notion_last_edited=parse_notion_datetime(parsed.get("last_edited_time")),
    )


def map_file(parsed: dict[str, Any], source_map: dict[str, SourceRecord]) -> FileRecord:
    """파싱된 약관 행을 ``FileRecord``로 매핑한다.

    출처 relation의 첫 값을 source_id로 쓰되, 그 출처가 이번에 동기화된(필터 통과) 것이
    아니면 FK 위반을 피하기 위해 source_id를 비운다. category는 연결 출처의 값을 쓰고
    없으면 기본값(terms)을 쓴다.
    """
    relations = parsed.get(_FILE_SOURCE_RELATION) or []
    source_page_id = relations[0] if relations else None
    source = source_map.get(source_page_id) if source_page_id else None
    resolved_source_id = source_page_id if source is not None else None
    category = source.category if (source and source.category) else DEFAULT_FILE_CATEGORY
    attachments = parsed.get(_FILE_ATTACHMENTS) or []
    return FileRecord(
        notion_page_id=parsed["id"],
        source_id=resolved_source_id,
        category=category,
        product_name=parsed.get(_FILE_PRODUCT_NAME),
        company=parsed.get(_FILE_COMPANY),
        product_type=parsed.get(_FILE_PRODUCT_TYPE),
        product_code=parsed.get(_FILE_PRODUCT_CODE),
        version_no=parsed.get(_FILE_VERSION_NO),
        effective_date=parse_notion_date(parsed.get(_FILE_EFFECTIVE_DATE)),
        license=parsed.get(_FILE_LICENSE),
        source_url=parsed.get(_FILE_SOURCE_URL),
        is_latest=parsed.get(_FILE_IS_LATEST),
        collect_status=parsed.get(_FILE_COLLECT_STATUS),
        part_total=len(attachments),
        notion_last_edited=parse_notion_datetime(parsed.get("last_edited_time")),
    )


def extract_parts(attachments: list[dict[str, Any]]) -> list[PartRecord]:
    """``파일`` 첨부 목록을 순서 보존한 ``PartRecord`` 목록으로 만든다."""
    ordered = sorted(attachments, key=lambda item: item.get("order", 0))
    return [
        PartRecord(part_order=item.get("order", index), notion_file_name=item.get("name"))
        for index, item in enumerate(ordered)
    ]


# --------------------------------------------------------------------------- #
# 부수효과 — 한 사이클 오케스트레이션
# --------------------------------------------------------------------------- #


async def run_sync_cycle(
    pool: asyncpg.Pool,
    source: NotionSource,
    settings: Settings | None = None,
    *,
    full: bool = False,
) -> SyncStats:
    """카탈로그·약관 미러 동기화 한 사이클을 실행한다.

    두 모드가 있으며, 어느 쪽으로 호출할지는 스케줄러(P2 워커 루프)가 정한다:

    - ``full=False``(기본, 증분): ``max(notion_last_edited)`` 이후만 조회한다. add/update만
      저비용으로 반영하고 **삭제는 감지하지 않는다**(미관측 ≠ 삭제).
    - ``full=True``(전수 재조정): 커서를 무시하고 전 행을 조회한 뒤, 이번에 관측되지 않은
      기존 행을 ``archived``로 전이해 **Notion 삭제를 반영**한다. 스케줄러가 주기적으로
      (예: 하루 1회) 호출한다.

    Args:
        pool: asyncpg 연결 풀(core.db).
        source: Notion REST 소스(주입 — 테스트에서 페이크).
        settings: 설정. None이면 ``get_settings()``.
        full: 전수 재조정 여부. True면 커서를 무시하고 삭제 아카이브까지 수행한다.

    Returns:
        이 사이클 결과 요약(SyncStats).
    """
    settings = settings or get_settings()
    allowed = parse_categories(settings.corpus_categories)

    source_map = await _sync_catalog(pool, source, settings, allowed)
    # 전수 재조정은 커서를 무시해야 전 행을 관측해 삭제를 판별할 수 있다.
    cursor = None if full else await get_file_cursor(pool)
    seen_files, observed_files, skipped = await _sync_files(
        pool, source, settings, source_map, cursor
    )

    # 삭제 반영은 전수(full) 관측에서만 안전하다 — 증분은 미관측이 삭제인지 알 수 없어 생략.
    # 첨부가 순간 빈 행도 observed에 있어, 그 순간만으로 삭제 오판돼 archived 되지 않는다.
    archived = 0
    if full and observed_files:
        archived = await archive_missing(pool, _ARCHIVE_TABLE, observed_files)

    stats = SyncStats(
        sources_synced=len(source_map),
        files_synced=len(seen_files),
        files_skipped=skipped,
        archived=archived,
    )
    logger.info("corpus_sync_cycle_done", full=full, **asdict(stats))
    return stats


async def _sync_catalog(
    pool: asyncpg.Pool,
    source: NotionSource,
    settings: Settings,
    allowed: set[str],
) -> dict[str, SourceRecord]:
    """카탈로그를 전체 조회해 허용 카테고리 출처만 업서트하고 맵으로 돌려준다."""
    source_map: dict[str, SourceRecord] = {}
    async for page in source.iter_rows(settings.notion_catalog_database_id):
        record = map_source(parse_props(page))
        if not category_allowed(record.category, allowed):
            continue
        try:
            await upsert_source(pool, record)
        except (asyncpg.PostgresError, CorpusSyncError) as exc:
            logger.warning(
                "corpus_source_upsert_failed", page_id=record.notion_page_id, error=str(exc)
            )
            continue
        source_map[record.notion_page_id] = record
    return source_map


async def _sync_files(
    pool: asyncpg.Pool,
    source: NotionSource,
    settings: Settings,
    source_map: dict[str, SourceRecord],
    since: datetime | None,
) -> tuple[set[str], set[str], int]:
    """약관 파일을 조회해 첨부 있는 행만 업서트·파트·priority 반영한다.

    반환의 ``observed``는 이번 조회에서 관측한 **모든** 문서(첨부 없어 skip한 것 포함)다.
    archive는 이 observed 기준이라, 순간적으로 ``파일`` 첨부가 빈 행(편집 중 일시 제거)이
    삭제로 오판돼 ``archived`` 되지 않는다.
    """
    seen: set[str] = set()
    observed: set[str] = set()
    skipped = 0
    async for page in source.iter_rows(settings.notion_corpus_database_id, since=since):
        parsed = parse_props(page)
        page_id = parsed.get("id")
        if page_id:
            observed.add(page_id)  # Notion에 존재 = 관측됨(첨부 유무 무관, archive 기준)
        if not has_attachment(parsed):
            skipped += 1
            continue
        record = map_file(parsed, source_map)
        parts = extract_parts(parsed.get(_FILE_ATTACHMENTS) or [])
        linked_source = source_map.get(record.source_id) if record.source_id else None
        try:
            await upsert_file(pool, record)
            await sync_parts(pool, record.notion_page_id, parts)
            await update_priority(
                pool,
                record.notion_page_id,
                priority.compute(linked_source, record, settings=settings),
            )
        except (asyncpg.PostgresError, CorpusSyncError) as exc:
            logger.warning("corpus_file_sync_failed", page_id=record.notion_page_id, error=str(exc))
            continue
        seen.add(record.notion_page_id)
    return seen, observed, skipped
