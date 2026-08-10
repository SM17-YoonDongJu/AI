"""search() 엔드투엔드 테스트 — 풀·임베딩을 페이크/monkeypatch로 격리.

실제 PG·Ollama 없이 라우팅→오타보정(치환 없음)→병렬검색→RRF→인용까지 조립을 검증한다.
"""

import datetime
import sys

import pytest

import rag.router
import rag.search  # 서브모듈 등록(패키지가 search 함수로 이름을 가림)
from core import ai_client
from core.config import settings

# 패키지 rag는 search 함수를 re-export해 동명 서브모듈 속성을 가린다. 내부 monkeypatch를
# 위해 sys.modules에서 모듈 객체를 직접 가져온다.
search_mod = sys.modules["rag.search"]


# namespace·리트리버별 페이크 행. dict는 asyncpg Record처럼 키 접근을 지원한다.
# 검색 SELECT가 article_number/product_name/chunk_type 별칭을 내므로 행에도 포함.
def _terms(cid: str, clause: str, exhibit: str | None, ctype: str = "clause") -> dict[str, object]:
    return {
        "chunk_id": cid,
        "content": f"약관 {cid}",
        "clause_no": clause,
        "exhibit": exhibit,
        "article_number": clause,
        "product_name": "다모아상해보험",
        "chunk_type": ctype,
    }


def _case(cid: str, clause: str, article: str) -> dict[str, object]:
    return {
        "chunk_id": cid,
        "content": f"사례 {cid}",
        "clause_no": clause,
        "exhibit": None,
        "article_number": article,
        "product_name": None,
        "chunk_type": "case",
    }


def _level(cid: str, section: str, version: str) -> dict[str, object]:
    # schedule_chunks SELECT 별칭: clause_no←section, exhibit←version_label,
    # article_number←section, product_name←NULL, chunk_type←chunk_type.
    return {
        "chunk_id": cid,
        "content": f"장해분류 {cid}",
        "clause_no": section,
        "exhibit": version,
        "article_number": section,
        "product_name": None,
        "chunk_type": "schedule",
    }


_TERMS_KEYWORD = [_terms("t1", "제3조", "별표2"), _terms("t2", "제5조", None)]
_TERMS_VECTOR = [_terms("t2", "제5조", None), _terms("t3", "제7조", None, "schedule")]
_CASE_KEYWORD = [_case("c1", "2021다1234", "판시사항")]
_CASE_VECTOR = [_case("c1", "2021다1234", "판시사항"), _case("c2", "2020다9999", "본문")]
_LEVEL_KEYWORD = [_level("s1", "눈", "2018.4 개정")]
_LEVEL_VECTOR = [_level("s1", "눈", "2018.4 개정"), _level("s2", "귀", "2018.4 개정")]


class _FakePool:
    """search_terms 조회는 항상 미스(보정 없음), 검색은 SQL 내용으로 분기. fetch args 기록."""

    def __init__(self) -> None:
        self.fetch_args: list[tuple[object, ...]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, sql: str, *args: object) -> None:
        return None

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.fetch_args.append(args)
        self.fetch_calls.append((sql, args))
        is_keyword = "plainto_tsquery" in sql
        if "policy_chunks" in sql:
            return _TERMS_KEYWORD if is_keyword else _TERMS_VECTOR
        if "schedule_chunks" in sql:
            return _LEVEL_KEYWORD if is_keyword else _LEVEL_VECTOR
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
    # 새 파생 메타(chunk_type 등)가 Chunk까지 전달됨
    assert any(chunk.chunk_type for chunk in result.ranked_chunks)


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


async def test_insurer_product_filter_reaches_sql(
    fake_pool: _FakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    async def fake_embed(text: str) -> list[float]:
        return [0.0] * settings.embedding_dim

    monkeypatch.setattr(ai_client, "embed", fake_embed)

    # Act: insurer/product를 넘기면 SQL 파라미터로 전달돼야 한다
    await search_mod.search(
        "후유장해",
        namespaces=["terms"],
        top_k=8,
        insurer="메리츠화재",
        product="다모아상해보험",
    )

    # Assert: 어떤 fetch 호출이든 필터 값이 args에 실려 있다
    flat = [a for args in fake_pool.fetch_args for a in args]
    assert "메리츠화재" in flat
    assert "다모아상해보험" in flat


async def test_insurer_product_filter_skips_case_namespace(
    fake_pool: _FakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    async def fake_embed(text: str) -> list[float]:
        return [0.0] * settings.embedding_dim

    monkeypatch.setattr(ai_client, "embed", fake_embed)

    # Act: terms·case 동시 검색에 insurer/product 필터를 건다
    await search_mod.search(
        "후유장해",
        namespaces=["terms", "case"],
        top_k=8,
        insurer="메리츠화재",
        product="다모아상해보험",
    )

    # Assert: case_chunks SQL에는 insurer/product 절도, 그 파라미터도 붙지 않는다
    # (case의 insurer/product_name은 nullable 참고 메타 — 걸면 recall이 0으로 붕괴).
    case_calls = [(sql, args) for sql, args in fake_pool.fetch_calls if "case_chunks" in sql]
    assert case_calls, "case_chunks 검색이 실행돼야 한다"
    for sql, args in case_calls:
        assert "insurer =" not in sql
        assert "product_name =" not in sql
        assert "메리츠화재" not in args
        assert "다모아상해보험" not in args

    # 반대로 policy_chunks(terms) SQL에는 필터가 정상 적용된다
    terms_calls = [(sql, args) for sql, args in fake_pool.fetch_calls if "policy_chunks" in sql]
    assert terms_calls, "policy_chunks 검색이 실행돼야 한다"
    assert any("메리츠화재" in args and "다모아상해보험" in args for _, args in terms_calls)


def test_router_accepts_level_namespace() -> None:
    # Arrange / Act: level을 명시하면 유효 namespace로 통과해야 한다
    decision = rag.router.route("후유장해분류표", namespaces=["level"])

    # Assert
    assert decision.in_scope is True
    assert decision.namespaces == ["level"]
    assert "level" in rag.router.VALID_NAMESPACES
    # level은 기본 검색 대상은 아니다(명시 요청 시에만)
    assert "level" not in rag.router.DEFAULT_NAMESPACES


async def test_level_search_binds_contract_date_version_filter(
    fake_pool: _FakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    async def fake_embed(text: str) -> list[float]:
        return [0.0] * settings.embedding_dim

    monkeypatch.setattr(ai_client, "embed", fake_embed)
    contract_date = datetime.date(2019, 5, 1)

    # Act: contract_date를 주면 level SQL에 적용기간 필터가 붙고 날짜가 파라미터로 실린다
    await search_mod.search(
        "후유장해분류표", namespaces=["level"], top_k=8, contract_date=contract_date
    )

    # Assert: schedule_chunks SQL에 applies_from/applies_to 경계 절 + 날짜 파라미터
    level_calls = [(sql, args) for sql, args in fake_pool.fetch_calls if "schedule_chunks" in sql]
    assert level_calls, "schedule_chunks 검색이 실행돼야 한다"
    for sql, args in level_calls:
        assert "applies_from <=" in sql
        assert "applies_to IS NULL OR" in sql
        assert contract_date in args


async def test_level_search_defaults_to_current_version_when_no_contract_date(
    fake_pool: _FakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    async def fake_embed(text: str) -> list[float]:
        return [0.0] * settings.embedding_dim

    monkeypatch.setattr(ai_client, "embed", fake_embed)

    # Act: contract_date 없이 level 검색 → 현행판(applies_to IS NULL)만
    await search_mod.search("후유장해분류표", namespaces=["level"], top_k=8)

    # Assert
    level_calls = [(sql, args) for sql, args in fake_pool.fetch_calls if "schedule_chunks" in sql]
    assert level_calls, "schedule_chunks 검색이 실행돼야 한다"
    for sql, args in level_calls:
        assert "applies_to IS NULL" in sql
        # 현행판 경로는 날짜 파라미터를 바인딩하지 않는다(date 값이 args에 없어야 함)
        assert not any(isinstance(a, datetime.date) for a in args)


async def test_contract_date_does_not_affect_terms_or_case(
    fake_pool: _FakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    async def fake_embed(text: str) -> list[float]:
        return [0.0] * settings.embedding_dim

    monkeypatch.setattr(ai_client, "embed", fake_embed)
    contract_date = datetime.date(2019, 5, 1)

    # Act: terms·case에 contract_date를 넘겨도 버전 필터가 붙지 않아야 한다
    await search_mod.search(
        "후유장해", namespaces=["terms", "case"], top_k=8, contract_date=contract_date
    )

    # Assert: terms/case SQL에는 버전 절도, 날짜 파라미터도 없다
    other_calls = [
        (sql, args)
        for sql, args in fake_pool.fetch_calls
        if "policy_chunks" in sql or "case_chunks" in sql
    ]
    assert other_calls, "terms·case 검색이 실행돼야 한다"
    for sql, args in other_calls:
        assert "applies_from" not in sql
        assert "applies_to" not in sql
        assert contract_date not in args
