"""Hybrid RAG 검색 조립 — 라우팅·오타보정·병렬검색·RRF·인용 역추적.

report_worker·chatbot이 함수 호출로 공유하는 단일 진입점 `search()`를 제공한다. 순수
함수형 인터페이스(in/out 명확, DB·임베딩 외 부수효과 없음)로, 호출자가 조립하기 쉽다.

파이프라인(04):
  1. 라우터 — namespace 결정, 비신체보험 범위 외 빈 결과
  2. trigram 오타보정 — search_terms 정규 용어 치환
  3. tsvector ∥ pgvector 병렬 검색 — namespace별 top_k
  4. RRF 통합 — score = Σ weight/(RRF_K + rank), 키워드·벡터 0.5:0.5
  5. 메타데이터 역추적 — 상위 청크에서 Citation 생성
"""

import asyncio
from dataclasses import dataclass

import asyncpg

from core import ai_client
from core.config import settings
from core.contracts import Chunk, Citation, RagResult
from core.db import get_pool
from core.logging import get_logger
from rag.fusion import KEYWORD_WEIGHT, VECTOR_WEIGHT, rrf_fuse
from rag.router import route
from rag.typo import correct_query

logger = get_logger(__name__)

DEFAULT_TOP_K = 8

# BGE-M3 폴백 모델(qwen3:embedding 실패 시). 1024d로 차원 일치.
_BGE_MODEL_NAME = "BAAI/bge-m3"

# namespace별 검색 SQL. clause_no/exhibit 별칭으로 행 파싱을 통일한다.
# 테이블·컬럼명은 내부 상수(사용자 입력 아님) — SQL 인젝션 위험 없음.
_KEYWORD_SQL: dict[str, str] = {
    "terms": (
        "SELECT chunk_id, content, article_number AS clause_no, section AS exhibit, "
        "ts_rank(content_tsv, plainto_tsquery('simple', $1)) AS rank "
        "FROM policy_chunks "
        "WHERE content_tsv @@ plainto_tsquery('simple', $1) "
        "ORDER BY rank DESC LIMIT $2"
    ),
    "case": (
        "SELECT chunk_id, content, case_number AS clause_no, NULL::text AS exhibit, "
        "ts_rank(content_tsv, plainto_tsquery('simple', $1)) AS rank "
        "FROM case_chunks "
        "WHERE content_tsv @@ plainto_tsquery('simple', $1) "
        "ORDER BY rank DESC LIMIT $2"
    ),
}
_VECTOR_SQL: dict[str, str] = {
    "terms": (
        "SELECT chunk_id, content, article_number AS clause_no, section AS exhibit "
        "FROM policy_chunks "
        "WHERE embedding IS NOT NULL "
        "ORDER BY embedding <=> $1 LIMIT $2"
    ),
    "case": (
        "SELECT chunk_id, content, case_number AS clause_no, NULL::text AS exhibit "
        "FROM case_chunks "
        "WHERE embedding IS NOT NULL "
        "ORDER BY embedding <=> $1 LIMIT $2"
    ),
}

# RRF 통합 시 namespace+chunk_id를 합쳐 충돌을 방지하는 구분자(id에 없는 NUL 문자).
_KEY_SEP = "\x00"

_bge_model: object | None = None


class RagError(RuntimeError):
    """RAG 검색 파이프라인 실패의 기반 예외."""


@dataclass(slots=True)
class ChunkRow:
    """검색 행을 namespace와 함께 표준화한 내부 표현(인용 역추적 입력)."""

    chunk_id: str
    namespace: str
    content: str
    clause_no: str | None
    exhibit: str | None


def _row_to_chunk_row(row: asyncpg.Record, namespace: str) -> ChunkRow:
    """DB 행을 `ChunkRow`로 변환한다(키워드·벡터 공통 별칭 사용)."""
    return ChunkRow(
        chunk_id=row["chunk_id"],
        namespace=namespace,
        content=row["content"],
        clause_no=row["clause_no"],
        exhibit=row["exhibit"],
    )


def _key(namespace: str, chunk_id: str) -> str:
    """RRF 통합용 namespace-한정 키."""
    return f"{namespace}{_KEY_SEP}{chunk_id}"


def build_citations(rows: list[ChunkRow]) -> list[Citation]:
    """상위 청크 행에서 인용 근거를 역추적한다(순수 함수).

    조항/사례 번호·별표가 모두 없는 행은 건너뛰고, 동일 인용은 한 번만 담는다.

    Args:
        rows: 점수 상위 청크 행(순서 유지).

    Returns:
        중복 제거된 `Citation` 리스트(입력 순서 보존).
    """
    seen: set[tuple[str | None, str | None]] = set()
    citations: list[Citation] = []
    for row in rows:
        if row.clause_no is None and row.exhibit is None:
            continue
        key = (row.clause_no, row.exhibit)
        if key in seen:
            continue
        seen.add(key)
        citations.append(Citation(clause_no=row.clause_no, exhibit=row.exhibit))
    return citations


async def _keyword_search(
    pool: asyncpg.Pool, namespace: str, query: str, top_k: int
) -> list[ChunkRow]:
    """namespace의 tsvector 키워드 검색 상위 top_k."""
    if not query.strip():
        return []
    rows = await pool.fetch(_KEYWORD_SQL[namespace], query, top_k)
    return [_row_to_chunk_row(row, namespace) for row in rows]


async def _vector_search(
    pool: asyncpg.Pool, namespace: str, embedding: list[float], top_k: int
) -> list[ChunkRow]:
    """namespace의 pgvector 코사인 유사도 검색 상위 top_k."""
    rows = await pool.fetch(_VECTOR_SQL[namespace], embedding, top_k)
    return [_row_to_chunk_row(row, namespace) for row in rows]


def _load_bge_model() -> object:
    """BGE-M3 폴백 모델을 지연 로드한다(블로킹 — to_thread에서 호출)."""
    global _bge_model
    if _bge_model is None:
        from sentence_transformers import SentenceTransformer

        _bge_model = SentenceTransformer(_BGE_MODEL_NAME)
    return _bge_model


async def _bge_embed(text: str) -> list[float]:
    """BGE-M3로 쿼리를 임베딩한다(폴백 경로). 차원 검증 포함."""
    model = await asyncio.to_thread(_load_bge_model)
    vector: list[float] = await asyncio.to_thread(
        lambda: model.encode(text, normalize_embeddings=True).tolist()  # type: ignore[attr-defined]
    )
    if len(vector) != settings.embedding_dim:
        raise RagError(f"BGE-M3 임베딩 차원 불일치: {len(vector)} != {settings.embedding_dim}")
    return vector


async def _embed_query(text: str) -> list[float] | None:
    """쿼리 임베딩. qwen3:embedding 실패 시 BGE-M3 폴백, 둘 다 실패면 None(degrade).

    Returns:
        1024d 임베딩 벡터, 또는 둘 다 실패 시 None(키워드 검색만으로 진행).
    """
    try:
        return await ai_client.embed(text)
    except ai_client.AiClientError as exc:
        logger.warning("rag.embed.primary_failed", exc_info=exc)

    try:
        return await _bge_embed(text)
    # 폴백 경계: 어떤 실패(ImportError·런타임)든 키워드 검색으로 degrade(원칙 8 예외).
    except Exception as exc:
        logger.warning("rag.embed.fallback_failed", exc_info=exc)
        return None


async def search(
    query: str,
    insurance_type: str | None = None,
    namespaces: list[str] | None = None,
    top_k: int = DEFAULT_TOP_K,
) -> RagResult:
    """Hybrid RAG 검색 진입점. ranked chunks + citations를 반환한다.

    Args:
        query: 사용자 쿼리 텍스트.
        insurance_type: 신체보험 유형 힌트(비신체는 범위 외 빈 결과).
        namespaces: 검색 namespace. None이면 라우터가 결정(terms·case).
        top_k: 반환 청크 최대 개수.

    Returns:
        통합 순위 청크와 인용 근거를 담은 `RagResult`. 범위 밖이면 빈 결과.
    """
    decision = route(query, insurance_type, namespaces)
    if not decision.in_scope:
        log_event_out_of_scope(decision.reason)
        return RagResult(ranked_chunks=[], citations=[])

    pool = get_pool()
    corrected = await correct_query(pool, query)

    # 키워드 검색(namespace별)과 임베딩 생성을 동시에 시작한다(독립 I/O 병렬화).
    embed_task = asyncio.create_task(_embed_query(corrected.embed_text))
    keyword_results = await asyncio.gather(
        *(_keyword_search(pool, ns, corrected.keyword_query, top_k) for ns in decision.namespaces)
    )
    embedding = await embed_task

    if embedding is not None:
        vector_results = await asyncio.gather(
            *(_vector_search(pool, ns, embedding, top_k) for ns in decision.namespaces)
        )
    else:
        # 임베딩 degrade: 키워드 검색만으로 진행.
        vector_results = [[] for _ in decision.namespaces]

    # 행 인덱스: 키(namespace+chunk_id) → ChunkRow. 키워드·벡터 어느 쪽이든 동일 content.
    row_index: dict[str, ChunkRow] = {}
    keyword_rankings: list[tuple[list[str], float]] = []
    vector_rankings: list[tuple[list[str], float]] = []
    for rows in keyword_results:
        keyword_rankings.append(([_index_row(row_index, row) for row in rows], KEYWORD_WEIGHT))
    for rows in vector_results:
        vector_rankings.append(([_index_row(row_index, row) for row in rows], VECTOR_WEIGHT))

    fused = rrf_fuse([*keyword_rankings, *vector_rankings])
    top = fused[:top_k]

    top_rows = [row_index[key] for key, _ in top]
    chunks = [
        Chunk(
            text=row.content,
            namespace=row.namespace,
            score=score,
            source_ref=row.chunk_id,
        )
        for (_, score), row in zip(top, top_rows, strict=True)
    ]
    citations = build_citations(top_rows)

    log_event_completed(decision.namespaces, top_k, len(chunks), embedding is None)
    return RagResult(ranked_chunks=chunks, citations=citations)


def _index_row(row_index: dict[str, ChunkRow], row: ChunkRow) -> str:
    """행을 인덱스에 등록하고 RRF 키를 반환한다(중복 키는 기존 행 유지)."""
    key = _key(row.namespace, row.chunk_id)
    row_index.setdefault(key, row)
    return key


def log_event_out_of_scope(reason: str | None) -> None:
    """범위 밖 쿼리 이벤트 로깅(쿼리 본문은 남기지 않는다 — PII 방지)."""
    logger.info("rag.search.out_of_scope", event_type="rag.search.out_of_scope", reason=reason)


def log_event_completed(namespaces: list[str], top_k: int, n_chunks: int, degraded: bool) -> None:
    """검색 완료 이벤트 로깅(쿼리 본문 제외)."""
    logger.info(
        "rag.search.completed",
        event_type="rag.search.completed",
        namespaces=namespaces,
        top_k=top_k,
        n_chunks=n_chunks,
        degraded=degraded,
    )
