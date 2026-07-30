"""corpus_worker sync 매핑·오케스트레이션 + repository upsert 계약 테스트 (이슈 #35 P1).

순수 매핑/필터/skip 로직과, 페이크 풀·페이크 소스로 격리한 한 사이클 흐름·멱등 upsert
SQL 계약을 검증한다. 실제 PG upsert/스키마는 docker-compose 기동 후 통합 테스트로 다룬다.
"""

import uuid
from datetime import UTC, date, datetime
from typing import Any

import pytest

from core.config import Settings
from core.exceptions import CorpusSyncError
from corpus_worker import priority, sync
from corpus_worker.repository import (
    FileRecord,
    PartRecord,
    SourceRecord,
    archive_missing,
    get_file_cursor,
    sync_parts,
    upsert_file,
    upsert_source,
)

SRC_TERMS = "11111111-1111-1111-1111-111111111111"
SRC_PRECEDENT = "22222222-2222-2222-2222-222222222222"
FILE_WITH_ATTACHMENT = "33333333-3333-3333-3333-333333333333"
FILE_NO_ATTACHMENT = "44444444-4444-4444-4444-444444444444"


class FakePool:
    """asyncpg.Pool.execute/fetchrow 흉내 — 호출을 기록하고 고정 커서를 돌려준다."""

    def __init__(self, cursor: datetime | None = None, exec_result: str = "UPDATE 0") -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self._cursor = cursor
        self._exec_result = exec_result

    async def execute(self, sql: str, *args: Any) -> str:
        self.executed.append((sql, args))
        return self._exec_result

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any]:
        return {"cursor": self._cursor}


class FakeSource:
    """NotionSource.iter_rows 흉내 — 데이터소스별 준비된 페이지를 흘려보낸다."""

    def __init__(self, pages_by_ds: dict[str, list[dict[str, Any]]]) -> None:
        self._pages = pages_by_ds
        self.calls: list[tuple[str, datetime | None]] = []

    async def iter_rows(self, data_source_id: str, since: datetime | None = None):
        self.calls.append((data_source_id, since))
        for page in self._pages.get(data_source_id, []):
            yield page


def _settings() -> Settings:
    return Settings(
        corpus_categories="terms",
        notion_catalog_database_id="catalog-ds",
        notion_corpus_database_id="corpus-ds",
        notion_token="test-token",
    )


def _source_page(page_id: str, name: str, category: str, tier: str | None) -> dict[str, Any]:
    return {
        "id": page_id,
        "last_edited_time": "2024-05-01T00:00:00.000Z",
        "properties": {
            "이름": {"type": "title", "title": [{"plain_text": name}]},
            "카테고리": {"type": "select", "select": {"name": category}},
            "우선순위": {"type": "select", "select": {"name": tier} if tier else None},
        },
    }


def _file_page(page_id: str, source_rel: str | None, *, attachment: bool) -> dict[str, Any]:
    files = [{"name": "part.pdf", "type": "file", "file": {"url": "x"}}] if attachment else []
    return {
        "id": page_id,
        "last_edited_time": "2024-05-02T00:00:00.000Z",
        "properties": {
            "상품명": {"type": "rich_text", "rich_text": [{"plain_text": "실손 표준약관"}]},
            "출처": {
                "type": "relation",
                "relation": [{"id": source_rel}] if source_rel else [],
            },
            "파일": {"type": "files", "files": files},
        },
    }


# --------------------------------------------------------------------------- #
# 순수 로직 — 파싱·매핑·필터
# --------------------------------------------------------------------------- #


def test_parse_categories_splits_and_trims() -> None:
    assert sync.parse_categories("terms, precedent ,, legal") == {"terms", "precedent", "legal"}


def test_category_allowed_excludes_none_and_others() -> None:
    allowed = {"terms"}
    assert sync.category_allowed("terms", allowed) is True
    assert sync.category_allowed("precedent", allowed) is False
    assert sync.category_allowed(None, allowed) is False


def test_has_attachment_reflects_files_presence() -> None:
    from corpus_worker.notion_source import parse_props

    with_file = parse_props(_file_page(FILE_WITH_ATTACHMENT, SRC_TERMS, attachment=True))
    without_file = parse_props(_file_page(FILE_NO_ATTACHMENT, SRC_TERMS, attachment=False))
    assert sync.has_attachment(with_file) is True
    assert sync.has_attachment(without_file) is False


def test_parse_reliability_parses_or_none() -> None:
    assert sync.parse_reliability("2") == 2
    assert sync.parse_reliability(None) is None
    assert sync.parse_reliability("bad") is None


def test_parse_notion_date_and_datetime() -> None:
    assert sync.parse_notion_date("2024-01-01") == date(2024, 1, 1)
    assert sync.parse_notion_date(None) is None
    parsed = sync.parse_notion_datetime("2024-05-01T00:00:00.000Z")
    assert parsed == datetime(2024, 5, 1, tzinfo=UTC)


def test_map_source_maps_korean_props() -> None:
    from corpus_worker.notion_source import parse_props

    record = sync.map_source(parse_props(_source_page(SRC_TERMS, "손보협회", "terms", "P1")))
    assert record.notion_page_id == SRC_TERMS
    assert record.name == "손보협회"
    assert record.category == "terms"
    assert record.priority_tier == "P1"
    assert record.notion_last_edited == datetime(2024, 5, 1, tzinfo=UTC)


def test_map_file_resolves_source_and_category() -> None:
    from corpus_worker.notion_source import parse_props

    source_map = {SRC_TERMS: SourceRecord(notion_page_id=SRC_TERMS, name="s", category="terms")}
    record = sync.map_file(
        parse_props(_file_page(FILE_WITH_ATTACHMENT, SRC_TERMS, attachment=True)), source_map
    )
    assert record.source_id == SRC_TERMS
    assert record.category == "terms"
    assert record.part_total == 1


def test_map_file_drops_unknown_source_id() -> None:
    from corpus_worker.notion_source import parse_props

    # 출처가 이번 사이클 맵에 없으면 FK 위반 회피를 위해 source_id를 비운다.
    record = sync.map_file(
        parse_props(_file_page(FILE_WITH_ATTACHMENT, SRC_PRECEDENT, attachment=True)), {}
    )
    assert record.source_id is None
    assert record.category == sync.DEFAULT_FILE_CATEGORY


def test_extract_parts_preserves_order() -> None:
    parts = sync.extract_parts([{"name": "b.pdf", "order": 1}, {"name": "a.pdf", "order": 0}])
    assert [(part.part_order, part.notion_file_name) for part in parts] == [
        (0, "a.pdf"),
        (1, "b.pdf"),
    ]


# --------------------------------------------------------------------------- #
# repository upsert SQL 계약 (페이크 풀)
# --------------------------------------------------------------------------- #


async def test_upsert_source_is_idempotent_on_conflict() -> None:
    pool = FakePool()
    record = SourceRecord(notion_page_id=SRC_TERMS, name="s")
    await upsert_source(pool, record)  # type: ignore[arg-type]
    sql, args = pool.executed[0]
    assert "ON CONFLICT (notion_page_id) DO UPDATE" in sql
    assert args[0] == uuid.UUID(SRC_TERMS)


async def test_upsert_file_preserves_state_columns() -> None:
    pool = FakePool()
    record = FileRecord(notion_page_id=FILE_WITH_ATTACHMENT)
    await upsert_file(pool, record)  # type: ignore[arg-type]
    sql, _ = pool.executed[0]
    assert "ON CONFLICT (notion_page_id) DO UPDATE" in sql
    # 파생·상태 컬럼은 DO UPDATE에서 제외되어야 한다(별도 경로 관리).
    for column in (
        "priority",
        "status",
        "demand_score",
        "demand_last_at",
        "attempts",
        "part_done",
        "fail_reason",
    ):
        assert f"    {column} = EXCLUDED" not in sql


async def test_sync_parts_deletes_shrunk_tail() -> None:
    pool = FakePool()
    await sync_parts(  # type: ignore[arg-type]
        pool, FILE_WITH_ATTACHMENT, [PartRecord(0, "a.pdf"), PartRecord(1, "b.pdf")]
    )
    delete_sql, delete_args = pool.executed[-1]
    assert "DELETE FROM corpus_file_part" in delete_sql
    assert delete_args[1] == 2  # part_order >= 2 정리


async def test_archive_missing_empty_is_noop() -> None:
    pool = FakePool()
    archived = await archive_missing(pool, "corpus_file", set())  # type: ignore[arg-type]
    assert archived == 0
    assert pool.executed == []


async def test_archive_missing_counts_affected_rows() -> None:
    pool = FakePool(exec_result="UPDATE 3")
    seen = {FILE_WITH_ATTACHMENT}
    archived = await archive_missing(pool, "corpus_file", seen)  # type: ignore[arg-type]
    assert archived == 3


async def test_archive_missing_rejects_unsupported_table() -> None:
    pool = FakePool()
    with pytest.raises(CorpusSyncError):
        await archive_missing(pool, "corpus_source", {SRC_TERMS})  # type: ignore[arg-type]


async def test_get_file_cursor_returns_max() -> None:
    moment = datetime(2024, 6, 1, tzinfo=UTC)
    pool = FakePool(cursor=moment)
    assert await get_file_cursor(pool) == moment  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# run_sync_cycle 흐름 (페이크 풀 + 페이크 소스)
# --------------------------------------------------------------------------- #


def _cycle_source() -> FakeSource:
    return FakeSource(
        {
            "catalog-ds": [
                _source_page(SRC_TERMS, "손보협회", "terms", "P1"),
                _source_page(SRC_PRECEDENT, "대법원", "precedent", "P0"),
            ],
            "corpus-ds": [
                _file_page(FILE_WITH_ATTACHMENT, SRC_TERMS, attachment=True),
                _file_page(FILE_NO_ATTACHMENT, SRC_TERMS, attachment=False),
            ],
        }
    )


async def test_run_sync_cycle_filters_and_skips() -> None:
    # Arrange
    pool = FakePool()  # cursor None → 전체 조회 사이클
    source = _cycle_source()

    # Act
    stats = await sync.run_sync_cycle(pool, source, _settings())  # type: ignore[arg-type]

    # Assert
    assert stats.sources_synced == 1  # terms만(precedent 필터)
    assert stats.files_synced == 1  # 첨부 있는 1건
    assert stats.files_skipped == 1  # 첨부 없는 1건 skip
    assert stats.archived == 0  # full=False(기본) → 아카이브 미실행


async def test_run_sync_cycle_writes_tier_priority() -> None:
    # Arrange
    pool = FakePool()
    source = _cycle_source()

    # Act
    await sync.run_sync_cycle(pool, source, _settings())  # type: ignore[arg-type]

    # Assert: terms 카테고리 base + P1 tier + 기본 수요/긴급도 합성 점수
    settings = _settings()
    expected = (
        priority.CATEGORY_BASE["terms"]
        + priority.TIER_BASE_SCORES["P1"]
        + settings.corpus_w_demand * priority.DEFAULT_DEMAND
        + settings.corpus_w_urgency * priority.DEFAULT_URGENCY
    )
    priority_updates = [args for sql, args in pool.executed if "SET priority" in sql]
    assert priority_updates
    assert priority_updates[0][1] == expected


async def test_run_sync_cycle_incremental_skips_archive() -> None:
    # Arrange: 커서가 있으면 증분 → 삭제 판별 불가 → 아카이브 생략
    pool = FakePool(cursor=datetime(2024, 6, 1, tzinfo=UTC))
    source = _cycle_source()

    # Act
    await sync.run_sync_cycle(pool, source, _settings())  # type: ignore[arg-type]

    # Assert: corpus 조회에 since 커서가 전달됨
    corpus_calls = [since for ds, since in source.calls if ds == "corpus-ds"]
    assert corpus_calls == [datetime(2024, 6, 1, tzinfo=UTC)]
    # 아카이브 UPDATE(status='archived')는 실행되지 않아야 한다
    assert not any("status = 'archived'" in sql for sql, _ in pool.executed)


async def test_run_sync_cycle_full_reconcile_archives() -> None:
    # Arrange: full=True → 커서 무시 전수 조회 + 미노출 행 아카이브(삭제 반영)
    pool = FakePool(cursor=datetime(2024, 6, 1, tzinfo=UTC), exec_result="UPDATE 2")
    source = _cycle_source()

    # Act
    stats = await sync.run_sync_cycle(pool, source, _settings(), full=True)

    # Assert: 커서를 무시해 corpus 조회에 since=None이 전달된다
    corpus_calls = [since for ds, since in source.calls if ds == "corpus-ds"]
    assert corpus_calls == [None]
    # 미노출 행 아카이브가 실행된다(UPDATE 2 → 2건)
    assert stats.archived == 2
    assert any("status = 'archived'" in sql for sql, _ in pool.executed)
    # 첨부 없는 행도 observed에 포함 → archive 기준에 들어가 삭제 오판 안 됨(#4 회귀)
    archive_args = next(a for s, a in pool.executed if "status = 'archived'" in s)
    assert uuid.UUID(FILE_NO_ATTACHMENT) in archive_args[0]
