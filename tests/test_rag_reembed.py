"""rag.reembed(코퍼스 재임베딩 CLI) 테스트 — DB·임베딩을 페이크로 격리.

페이크 풀은 테이블별 행을 메모리에 두고 실제로 받은 SQL 문자열을 기록한다. keyset 페이지와
트랜잭션 커밋·롤백도 흉내 내서, 중단 시 어디까지 반영됐는지 확인할 수 있다. 임베딩은
ai_client.embed_batch를 monkeypatch한다.
"""

from __future__ import annotations

import re
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import httpx
import pytest

import rag.reembed as reembed_mod
import rag.search  # noqa: F401 — 서브모듈 등록(패키지가 search 함수로 이름을 가림)
from core import ai_client
from core.config import settings

search_mod = sys.modules["rag.search"]

_OLD = [-1.0]  # 재임베딩 전(이전 모델) 벡터 표식
_FIRST_PAGE = "SELECT chunk_id, content FROM corpus.policy_chunks ORDER BY chunk_id LIMIT $1"
_NEXT_PAGE = (
    "SELECT chunk_id, content FROM corpus.policy_chunks "
    "WHERE chunk_id > $1 ORDER BY chunk_id LIMIT $2"
)
_UPDATE = "UPDATE corpus.policy_chunks SET embedding = $2 WHERE chunk_id = $1"


def _vector(text: str) -> list[float]:
    """텍스트마다 다른 결정적 가짜 벡터."""
    return [float(len(text)), float(ord(text[-1]))]


def _rows(*chunk_ids: str) -> dict[str, dict[str, Any]]:
    return {cid: {"content": f"본문 {cid}", "embedding": _OLD} for cid in chunk_ids}


class _FakeConn:
    """풀에서 빌린 커넥션 대역. 쓰기는 트랜잭션이 커밋될 때만 반영한다."""

    def __init__(self, pool: _FakePool) -> None:
        self._pool = pool
        self._pending: list[tuple[str, str, list[float] | None]] = []

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        self._pool.events.append("begin")
        try:
            yield
        except BaseException:
            self._pool.events.append("rollback")
            raise
        for target, chunk_id, vector in self._pending:
            self._pool.tables[target][chunk_id]["embedding"] = vector
        self._pool.events.append("commit")

    async def executemany(self, sql: str, params: list[tuple[str, list[float] | None]]) -> None:
        self._pool.executemany_calls.append((sql, list(params)))
        if self._pool.executemany_error is not None:
            raise self._pool.executemany_error
        target = _target_of(sql)
        self._pending.extend((target, chunk_id, vector) for chunk_id, vector in params)


def _target_of(sql: str) -> str:
    match = re.search(r"(?:FROM|UPDATE) (\S+)", sql)
    assert match is not None, sql
    return match.group(1)


class _FakePool:
    """테이블별 행을 메모리에 두는 asyncpg 풀 대역. SQL·트랜잭션 이벤트를 기록한다."""

    def __init__(self, tables: dict[str, dict[str, dict[str, Any]]]) -> None:
        self.tables = tables  # "schema.table" → chunk_id → {"content", "embedding"}
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchval_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[str, list[float] | None]]]] = []
        self.events: list[str] = []
        self.executemany_error: Exception | None = None

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((sql, args))
        *cursor, limit = args
        rows = self.tables[_target_of(sql)]
        ids = sorted(cid for cid in rows if not cursor or cid > cursor[0])[:limit]
        return [{"chunk_id": cid, "content": rows[cid]["content"]} for cid in ids]

    async def fetchval(self, sql: str, *args: Any) -> int:
        self.fetchval_calls.append((sql, args))
        return sum(1 for cid in self.tables[_target_of(sql)] if not args or cid > args[0])

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_FakeConn]:
        yield _FakeConn(self)


def _pool(
    policy: tuple[str, ...] = (),
    case: tuple[str, ...] = (),
    schedule: tuple[str, ...] = (),
    schema: str = "corpus",
) -> _FakePool:
    return _FakePool(
        {
            f"{schema}.policy_chunks": _rows(*policy),
            f"{schema}.case_chunks": _rows(*case),
            f"{schema}.schedule_chunks": _rows(*schedule),
        }
    )


class _FakeEmbedder:
    """ai_client.embed_batch 대역. input_type은 기본값이 없어 호출자가 명시해야 한다."""

    def __init__(self, *, fail_on_call: int | None = None, error: Exception | None = None) -> None:
        self.calls: list[tuple[list[str], str]] = []
        self._fail_on_call = fail_on_call
        self._error = error

    async def __call__(
        self, texts: list[str], *, input_type: ai_client.EmbedInputType
    ) -> list[list[float]]:
        self.calls.append((list(texts), input_type))
        if self._error is not None and len(self.calls) == self._fail_on_call:
            raise self._error
        return [_vector(text) for text in texts]


class _RecordingLogger:
    """structlog 로거 대역 — (레벨, 이벤트, 필드)를 기록한다."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def info(self, event: str, **fields: Any) -> None:
        self.records.append(("info", event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.records.append(("error", event, fields))

    def events(self, name: str) -> list[dict[str, Any]]:
        return [fields for _, event, fields in self.records if event == name]


@pytest.fixture
def log(monkeypatch: pytest.MonkeyPatch) -> _RecordingLogger:
    recorder = _RecordingLogger()
    monkeypatch.setattr(reembed_mod, "logger", recorder)
    return recorder


@pytest.fixture
def embedder(monkeypatch: pytest.MonkeyPatch) -> _FakeEmbedder:
    fake = _FakeEmbedder()
    monkeypatch.setattr(ai_client, "embed_batch", fake)
    return fake


# ── 재임베딩 본 흐름 ─────────────────────────────────────────────────────────────


async def test_reembeds_all_three_tables_as_documents(
    embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Arrange
    pool = _pool(policy=("a", "b", "c"), case=("k1",), schedule=("s1", "s2"))

    # Act: 기본 대상은 세 테이블 전부
    updated = await reembed_mod.reembed(pool, batch_size=2)

    # Assert: 모든 행이 자기 content의 새 벡터로 바뀌고, 문서는 원문 그대로 document로 임베딩
    assert updated == {"policy_chunks": 3, "case_chunks": 1, "schedule_chunks": 2}
    assert all(
        row["embedding"] == _vector(row["content"])
        for rows in pool.tables.values()
        for row in rows.values()
    )
    assert {input_type for _, input_type in embedder.calls} == {"document"}
    assert embedder.calls[0][0] == ["본문 a", "본문 b"]


async def test_reads_keyset_pages_and_commits_each_batch(
    embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Arrange
    pool = _pool(policy=("a", "b", "c"))

    # Act
    await reembed_mod.reembed(pool, tables=["policy_chunks"], batch_size=2)

    # Assert: 첫 페이지는 커서 없이, 다음 페이지는 직전 페이지의 마지막 chunk_id 다음부터
    assert pool.fetch_calls == [(_FIRST_PAGE, (2,)), (_NEXT_PAGE, ("b", 2))]
    assert pool.executemany_calls == [
        (_UPDATE, [("a", _vector("본문 a")), ("b", _vector("본문 b"))]),
        (_UPDATE, [("c", _vector("본문 c"))]),
    ]
    assert pool.events == ["begin", "commit", "begin", "commit"]
    # 진행 로그: 누적 건수와 재개용 마지막 chunk_id
    committed = log.events("rag.reembed.batch_committed")
    assert [(f["table"], f["updated"], f["last_chunk_id"]) for f in committed] == [
        ("policy_chunks", 2, "b"),
        ("policy_chunks", 3, "c"),
    ]


@pytest.mark.parametrize(
    ("chunk_ids", "expected_fetches", "expected_batches"),
    [
        # 행 수가 배치의 배수면 빈 페이지를 한 번 더 읽고 끝난다
        (("a", "b", "c", "d"), 3, [["본문 a", "본문 b"], ["본문 c", "본문 d"]]),
        # 마지막 페이지가 짧으면 추가 조회 없이 끝난다
        (("a", "b", "c", "d", "e"), 3, [["본문 a", "본문 b"], ["본문 c", "본문 d"], ["본문 e"]]),
        ((), 1, []),
    ],
)
async def test_page_boundaries(
    embedder: _FakeEmbedder,
    log: _RecordingLogger,
    chunk_ids: tuple[str, ...],
    expected_fetches: int,
    expected_batches: list[list[str]],
) -> None:
    # Arrange
    pool = _pool(policy=chunk_ids)

    # Act
    updated = await reembed_mod.reembed(pool, tables=["policy_chunks"], batch_size=2)

    # Assert
    assert len(pool.fetch_calls) == expected_fetches
    assert [texts for texts, _ in embedder.calls] == expected_batches
    assert updated == {"policy_chunks": len(chunk_ids)}


async def test_after_resumes_from_cursor_and_keeps_earlier_rows(
    embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Arrange
    pool = _pool(policy=("a", "b", "c", "d"))

    # Act
    updated = await reembed_mod.reembed(pool, tables=["policy_chunks"], batch_size=2, after="b")

    # Assert: 커서 다음 행만 바뀌고 앞 행(이미 처리된 행)은 그대로
    rows = pool.tables["corpus.policy_chunks"]
    assert updated == {"policy_chunks": 2}
    assert pool.fetch_calls[0] == (_NEXT_PAGE, ("b", 2))
    assert rows["a"]["embedding"] == _OLD
    assert rows["b"]["embedding"] == _OLD
    assert rows["c"]["embedding"] == _vector("본문 c")
    assert rows["d"]["embedding"] == _vector("본문 d")


async def test_public_schema_for_temp_vector_db(
    embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Arrange: tempVectorDB는 public 스키마
    pool = _pool(policy=("a",), schema="public")

    # Act
    await reembed_mod.reembed(pool, schema="public", tables=["policy_chunks"])

    # Assert
    assert pool.executemany_calls[0][0] == (
        "UPDATE public.policy_chunks SET embedding = $2 WHERE chunk_id = $1"
    )


async def test_blank_content_is_nulled_instead_of_embedded(
    embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Arrange: 빈 content는 임베딩할 수 없다 — 이전 모델 벡터를 남기지 않고 NULL로 비운다
    pool = _pool(policy=("a", "b"))
    pool.tables["corpus.policy_chunks"]["b"]["content"] = "  \n"

    # Act
    await reembed_mod.reembed(pool, tables=["policy_chunks"])

    # Assert
    assert embedder.calls == [(["본문 a"], "document")]
    assert pool.tables["corpus.policy_chunks"]["a"]["embedding"] == _vector("본문 a")
    assert pool.tables["corpus.policy_chunks"]["b"]["embedding"] is None
    assert log.events("rag.reembed.table_done")[0]["nulled"] == 1


async def test_logs_never_contain_chunk_content(
    embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Arrange
    pool = _pool(policy=("a", "b", "c"))

    # Act
    await reembed_mod.reembed(pool, tables=["policy_chunks"], batch_size=2)

    # Assert: 진행 로그는 남되 본문은 어디에도 없다
    assert log.events("rag.reembed.batch_committed")
    assert "본문" not in repr(log.records)


# ── dry-run ─────────────────────────────────────────────────────────────────────


async def test_dry_run_counts_rows_without_embedding_or_writing(
    embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Arrange
    pool = _pool(policy=("a", "b"), case=("k1",))

    # Act
    counts = await reembed_mod.count_targets(pool)

    # Assert
    assert counts == {"policy_chunks": 2, "case_chunks": 1, "schedule_chunks": 0}
    assert pool.fetchval_calls[0] == ("SELECT count(*) FROM corpus.policy_chunks", ())
    assert embedder.calls == []
    assert pool.fetch_calls == []
    assert pool.executemany_calls == []


async def test_dry_run_with_after_counts_only_remaining_rows(
    embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Arrange
    pool = _pool(policy=("a", "b", "c"))

    # Act
    counts = await reembed_mod.count_targets(pool, tables=["policy_chunks"], after="a")

    # Assert
    assert counts == {"policy_chunks": 2}
    assert pool.fetchval_calls == [
        ("SELECT count(*) FROM corpus.policy_chunks WHERE chunk_id > $1", ("a",))
    ]


# ── 입력 검증 ───────────────────────────────────────────────────────────────────


def test_target_tables_match_search_namespaces() -> None:
    # Assert: search가 같은 쿼리 벡터로 검색하는 테이블이 늘면 재임베딩 대상도 함께 늘어야 한다
    assert set(reembed_mod.TARGET_TABLES) == set(search_mod._NS_TABLE.values())


@pytest.mark.parametrize("tables", [["users"], ["policy_chunks", "core.users"], []])
async def test_rejects_tables_outside_whitelist(embedder: _FakeEmbedder, tables: list[str]) -> None:
    # Arrange
    pool = _pool()

    # Act / Assert: DB에 닿기 전에 거절
    with pytest.raises(ValueError):
        await reembed_mod.reembed(pool, tables=tables)
    assert pool.fetch_calls == []
    assert pool.fetchval_calls == []


@pytest.mark.parametrize(
    "schema",
    [
        "corpus; DROP TABLE corpus.policy_chunks",
        "Corpus",
        "1corpus",
        "",
        "corpus.x",
        'corpus"',
        "a" * 64,
    ],
)
async def test_rejects_invalid_schema(embedder: _FakeEmbedder, schema: str) -> None:
    # Arrange
    pool = _pool()

    # Act / Assert
    with pytest.raises(ValueError):
        await reembed_mod.reembed(pool, schema=schema)
    with pytest.raises(ValueError):
        await reembed_mod.count_targets(pool, schema=schema)
    assert pool.fetchval_calls == []


async def test_rejects_after_with_multiple_tables(embedder: _FakeEmbedder) -> None:
    # Arrange: 커서는 테이블마다 달라, 다른 테이블에 걸리면 그 앞 행을 조용히 건너뛴다
    pool = _pool(policy=("a",), case=("k1",))

    # Act / Assert
    with pytest.raises(ValueError):
        await reembed_mod.reembed(pool, after="a")
    assert pool.fetch_calls == []


async def test_rejects_non_positive_batch_size(embedder: _FakeEmbedder) -> None:
    # Arrange
    pool = _pool(policy=("a",))

    # Act / Assert
    with pytest.raises(ValueError):
        await reembed_mod.reembed(pool, batch_size=0)
    assert pool.fetch_calls == []


# ── 중단·재개 ───────────────────────────────────────────────────────────────────


async def test_embedding_failure_keeps_committed_batches_and_logs_resume_cursor(
    monkeypatch: pytest.MonkeyPatch, log: _RecordingLogger
) -> None:
    # Arrange: 두 번째 배치 임베딩에서 실패
    embedder = _FakeEmbedder(fail_on_call=2, error=ai_client.AiClientError("embed 호출 실패"))
    monkeypatch.setattr(ai_client, "embed_batch", embedder)
    pool = _pool(policy=("a", "b", "c", "d", "e"), case=("k1",))

    # Act
    with pytest.raises(reembed_mod.ReembedAbortedError) as excinfo:
        await reembed_mod.reembed(pool, batch_size=2)

    # Assert: 첫 배치만 커밋됐고, 뒤 행과 다음 테이블은 그대로다
    rows = pool.tables["corpus.policy_chunks"]
    assert rows["b"]["embedding"] == _vector("본문 b")
    assert rows["c"]["embedding"] == _OLD
    assert pool.tables["corpus.case_chunks"]["k1"]["embedding"] == _OLD
    assert pool.events == ["begin", "commit"]
    assert excinfo.value.resume_after == "b"
    # 재개 커서·남은 테이블이 로그에 남고, 예외 메시지는 남지 않는다
    assert log.events("rag.reembed.aborted") == [
        {
            "table": "policy_chunks",
            "updated": 2,
            "resume_after": "b",
            "remaining_tables": ["case_chunks", "schedule_chunks"],
            "error_type": "AiClientError",
            "cause_type": None,
            "http_status": None,
        }
    ]


async def test_abort_log_carries_http_status_of_embedding_error(
    monkeypatch: pytest.MonkeyPatch, log: _RecordingLogger
) -> None:
    # Arrange: 첫 배치부터 429(한도 초과) — ai_client가 httpx 오류를 원인으로 감싼 형태
    request = httpx.Request("POST", "https://ai.mongodb.com/v1/embeddings")
    error = ai_client.AiClientError("embed 호출 실패")
    error.__cause__ = httpx.HTTPStatusError(
        "Too Many Requests", request=request, response=httpx.Response(429, request=request)
    )
    monkeypatch.setattr(ai_client, "embed_batch", _FakeEmbedder(fail_on_call=1, error=error))
    pool = _pool(policy=("a", "b"))

    # Act
    with pytest.raises(reembed_mod.ReembedAbortedError) as excinfo:
        await reembed_mod.reembed(pool, tables=["policy_chunks"], after="a")

    # Assert: 아무것도 커밋되지 않았으면 재개 커서는 시작 커서 그대로
    aborted = log.events("rag.reembed.aborted")[0]
    assert excinfo.value.resume_after == "a"
    assert aborted["resume_after"] == "a"
    assert aborted["http_status"] == 429
    assert aborted["cause_type"] == "HTTPStatusError"


async def test_db_write_failure_rolls_back_batch_and_aborts(
    embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Arrange: corpus 테이블에 UPDATE 권한이 없는 계정(ai_owner)으로 돌린 경우
    pool = _pool(policy=("a", "b"))
    pool.executemany_error = asyncpg.exceptions.InsufficientPrivilegeError(
        "permission denied for table policy_chunks"
    )

    # Act
    with pytest.raises(reembed_mod.ReembedAbortedError) as excinfo:
        await reembed_mod.reembed(pool, tables=["policy_chunks"])

    # Assert
    assert pool.events == ["begin", "rollback"]
    assert pool.tables["corpus.policy_chunks"]["a"]["embedding"] == _OLD
    assert excinfo.value.resume_after is None
    assert log.events("rag.reembed.aborted")[0]["error_type"] == "InsufficientPrivilegeError"
    assert "permission denied" not in repr(log.records)


# ── CLI ─────────────────────────────────────────────────────────────────────────


@pytest.fixture
def cli_pool(
    monkeypatch: pytest.MonkeyPatch, log: _RecordingLogger, embedder: _FakeEmbedder
) -> _FakePool:
    pool = _pool(policy=("a", "b", "c"))

    @asynccontextmanager
    async def fake_db_pool(settings: object = None) -> AsyncIterator[_FakePool]:
        yield pool

    monkeypatch.setattr(reembed_mod, "db_pool", fake_db_pool)
    # 전역 로깅 설정을 바꾸지 않는다(다른 테스트 격리)
    monkeypatch.setattr(reembed_mod, "configure_logging", lambda: None)
    return pool


def test_cli_logs_embedding_settings_and_reembeds(
    cli_pool: _FakePool, log: _RecordingLogger, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.setattr(settings, "embedding_provider", "voyage")
    monkeypatch.setattr(settings, "embedding_model", "voyage-4-large")

    # Act
    exit_code = reembed_mod.main(["--tables", "policy_chunks"])

    # Assert: 시작 로그에 임베딩 설정이 남는다
    started = log.events("rag.reembed.started")[0]
    assert exit_code == 0
    assert started["embedding_provider"] == "voyage"
    assert started["embedding_model"] == "voyage-4-large"
    assert started["embedding_dim"] == settings.embedding_dim
    assert log.events("rag.reembed.completed") == [{"updated": {"policy_chunks": 3}}]


def test_cli_dry_run_counts_only(
    cli_pool: _FakePool, embedder: _FakeEmbedder, log: _RecordingLogger
) -> None:
    # Act
    exit_code = reembed_mod.main(["--dry-run"])

    # Assert
    assert exit_code == 0
    assert [f["rows"] for f in log.events("rag.reembed.dry_run")] == [3, 0, 0]
    assert embedder.calls == []
    assert cli_pool.executemany_calls == []


def test_cli_exits_1_with_resume_cursor_when_embedding_fails(
    cli_pool: _FakePool, log: _RecordingLogger, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    failing = _FakeEmbedder(fail_on_call=2, error=ai_client.AiClientError("embed 호출 실패"))
    monkeypatch.setattr(ai_client, "embed_batch", failing)

    # Act
    exit_code = reembed_mod.main(["--batch-size", "2"])

    # Assert
    assert exit_code == 1
    assert log.events("rag.reembed.aborted")[0]["resume_after"] == "b"
    assert log.events("rag.reembed.completed") == []


@pytest.mark.parametrize(
    "argv",
    [
        ["--tables", "users"],
        ["--schema", "corpus;"],
        ["--after", "a"],  # 테이블 셋 전부에 커서를 걸 수 없다
        ["--batch-size", "0"],
        ["--batch-size", "many"],
    ],
)
def test_cli_rejects_bad_arguments(cli_pool: _FakePool, argv: list[str]) -> None:
    # Act / Assert: argparse가 종료 코드 2로 끝내고 DB에 닿지 않는다
    with pytest.raises(SystemExit) as excinfo:
        reembed_mod.main(argv)
    assert excinfo.value.code == 2
    assert cli_pool.fetchval_calls == []


def test_cli_help_says_all_tables_need_the_same_model(capsys: pytest.CaptureFixture[str]) -> None:
    # Act
    with pytest.raises(SystemExit):
        reembed_mod.main(["--help"])

    # Assert: 세 테이블·같은 모델·corpus_owner 안내가 --help에 있다
    out = capsys.readouterr().out
    assert all(table in out for table in reembed_mod.TARGET_TABLES)
    assert "같은 모델" in out
    assert "corpus_owner" in out
