"""search() 엔드투엔드 테스트 — 풀·임베딩을 페이크/monkeypatch로 격리.

실제 PG·Ollama 없이 라우팅→오타보정(치환 없음)→병렬검색→RRF→인용까지 조립을 검증한다.
"""

import sys

import pytest

import rag.search  # noqa: F401 - 서브모듈 등록(패키지가 search 함수로 이름을 가림)
from core import ai_client
from core.config import settings

# 패키지 rag는 search 함수를 re-export해 동명 서브모듈 속성을 가린다. 내부 monkeypatch를
# 위해 sys.modules에서 모듈 객체를 직접 가져온다.
search_mod = sys.modules["rag.search"]

# namespace·리트리버별 페이크 행. dict는 asyncpg Record처럼 키 접근을 지원한다.
_TERMS_KEYWORD = [
    {"chunk_id": "t1", "content": "약관 t1", "clause_no": "제3조", "exhibit": "별표2"},
    {"chunk_id": "t2", "content": "약관 t2", "clause_no": "제5조", "exhibit": None},
]
_TERMS_VECTOR = [
    {"chunk_id": "t2", "content": "약관 t2", "clause_no": "제5조", "exhibit": None},
    {"chunk_id": "t3", "content": "약관 t3", "clause_no": "제7조", "exhibit": None},
]
_CASE_KEYWORD = [
    {"chunk_id": "c1", "content": "사례 c1", "clause_no": "2021다1234", "exhibit": None},
]
_CASE_VECTOR = [
    {"chunk_id": "c1", "content": "사례 c1", "clause_no": "2021다1234", "exhibit": None},
    {"chunk_id": "c2", "content": "사례 c2", "clause_no": "2020다9999", "exhibit": None},
]


class _FakePool:
    """search_terms 조회는 항상 미스(보정 없음), 검색은 SQL 내용으로 분기."""

    async def fetchrow(self, sql: str, *args: object) -> None:
        return None

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        is_keyword = "plainto_tsquery" in sql
        if "policy_chunks" in sql:
            return _TERMS_KEYWORD if is_keyword else _TERMS_VECTOR
        return _CASE_KEYWORD if is_keyword else _CASE_VECTOR


@pytest.fixture
def fake_pool(monkeypatch: pytest.MonkeyPatch) -> _FakePool:
    pool = _FakePool()
    monkeypatch.setattr(search_mod, "get_pool", lambda: pool)
    return pool


async def test_search_returns_fused_chunks_and_citations(
    fake_pool: _FakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange: 임베딩은 계약 차원의 0벡터
    async def fake_embed(text: str) -> list[float]:
        return [0.0] * settings.embedding_dim

    monkeypatch.setattr(ai_client, "embed", fake_embed)

    # Act
    result = await search_mod.search("후유장해 보상", namespaces=["terms", "case"], top_k=8)

    # Assert: 통합 청크와 인용이 생성됨
    assert len(result.ranked_chunks) > 0
    namespaces = {chunk.namespace for chunk in result.ranked_chunks}
    assert namespaces <= {"terms", "case"}
    clause_numbers = {c.clause_no for c in result.citations}
    assert "제3조" in clause_numbers
    # RRF: 점수 내림차순 정렬 보장
    scores = [chunk.score for chunk in result.ranked_chunks]
    assert scores == sorted(scores, reverse=True)


async def test_out_of_scope_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    # Act: 비신체보험 → 라우터가 차단(풀·임베딩 호출 없이 빈 결과)
    result = await search_mod.search("자동차 사고 보상")

    # Assert
    assert result.ranked_chunks == []
    assert result.citations == []


async def test_degrades_to_keyword_only_when_embedding_fails(
    fake_pool: _FakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange: 1차·폴백 임베딩 모두 실패 → 키워드 검색만으로 degrade
    async def boom_embed(text: str) -> list[float]:
        raise ai_client.AiClientError("primary down")

    def boom_bge() -> object:
        raise ImportError("sentence_transformers 미설치")

    monkeypatch.setattr(ai_client, "embed", boom_embed)
    monkeypatch.setattr(search_mod, "_load_bge_model", boom_bge)

    # Act
    result = await search_mod.search("후유장해", namespaces=["terms"], top_k=8)

    # Assert: 벡터 없이도 키워드 결과는 반환된다
    assert len(result.ranked_chunks) > 0
    assert {chunk.namespace for chunk in result.ranked_chunks} == {"terms"}
