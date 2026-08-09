"""trigram 오타 보정 — kiwipiepy 토큰화 + search_terms 정규 용어 치환.

쿼리를 형태소 분석해 내용어(명사·동사·형용사 어간 등)를 뽑고, 각 내용어를 search_terms의
pg_trgm 인덱스로 조회해 `similarity(term, 입력) > SIMILARITY_THRESHOLD`인 가장 유사한 정규
용어 1건으로 치환한다(04 오타 보정 단계). 보정 결과가 tsvector·임베딩 검색에 쓰인다.
"""

import asyncio
from dataclasses import dataclass, field

import asyncpg
from kiwipiepy import Kiwi

# trigram 유사도 임계값. 이 값 초과인 정규 용어만 치환에 사용한다.
SIMILARITY_THRESHOLD = 0.4

# 내용어로 간주할 Kiwi 품사 태그 접두사(명사·동사·형용사·어근·외국어·숫자).
# 조사·어미·기호 등 기능어는 제외해 핵심 용어만 보정·검색 대상으로 남긴다.
_CONTENT_POS_PREFIXES: tuple[str, ...] = ("NN", "NR", "VV", "VA", "XR", "SL", "SN")

# 가장 유사한 정규 용어 1건 조회. % 연산자가 아닌 명시적 similarity로 임계값을 제어한다.
_TERM_LOOKUP_SQL = (
    "SELECT term FROM search_terms "
    "WHERE similarity(term, $1) > $2 "
    "ORDER BY similarity(term, $1) DESC "
    "LIMIT 1"
)

_kiwi: Kiwi | None = None


@dataclass(slots=True)
class Correction:
    """단일 용어 치환 기록(관측·디버깅용)."""

    original: str  # 입력 내용어
    canonical: str  # 치환된 정규 용어


@dataclass(slots=True)
class CorrectedQuery:
    """오타 보정 결과. 키워드 검색·임베딩 검색이 각각 다른 표현을 쓴다."""

    keyword_query: str  # 공백 구분 보정 내용어(plainto_tsquery 입력)
    embed_text: str  # 보정된 자연어 쿼리(임베딩 입력)
    corrections: list[Correction] = field(default_factory=list)


def _get_kiwi() -> Kiwi:
    """Kiwi 인스턴스를 지연 생성해 재사용한다(초기화 비용 1회)."""
    global _kiwi
    if _kiwi is None:
        _kiwi = Kiwi()
    return _kiwi


def extract_content_tokens(query: str) -> list[str]:
    """쿼리에서 내용어 표면형 목록을 추출한다(순수·동기, CPU 바운드).

    Args:
        query: 원본 쿼리 텍스트.

    Returns:
        내용어 표면형 리스트(입력 순서 보존).
    """
    kiwi = _get_kiwi()
    return [
        token.form for token in kiwi.tokenize(query) if token.tag.startswith(_CONTENT_POS_PREFIXES)
    ]


async def _lookup_canonical(conn: asyncpg.Pool, token: str) -> str | None:
    """search_terms에서 token과 가장 유사한 정규 용어를 1건 조회한다."""
    row = await conn.fetchrow(_TERM_LOOKUP_SQL, token, SIMILARITY_THRESHOLD)
    return None if row is None else row["term"]


async def correct_query(pool: asyncpg.Pool, query: str) -> CorrectedQuery:
    """쿼리의 내용어를 정규 용어로 치환한 보정 결과를 만든다.

    Args:
        pool: asyncpg 풀(search_terms 조회용).
        query: 원본 쿼리 텍스트.

    Returns:
        키워드 검색용 토큰 문자열과 임베딩용 보정 텍스트를 담은 `CorrectedQuery`.
    """
    # 토큰화는 CPU 바운드라 스레드로 격리(async 경로 블로킹 금지).
    tokens = await asyncio.to_thread(extract_content_tokens, query)

    corrected_tokens: list[str] = []
    corrections: list[Correction] = []
    embed_text = query
    for token in tokens:
        canonical = await _lookup_canonical(pool, token)
        if canonical is not None and canonical != token:
            corrected_tokens.append(canonical)
            corrections.append(Correction(original=token, canonical=canonical))
            # 임베딩 텍스트도 동일하게 치환(첫 출현만).
            embed_text = embed_text.replace(token, canonical, 1)
        else:
            corrected_tokens.append(token)

    return CorrectedQuery(
        keyword_query=" ".join(corrected_tokens),
        embed_text=embed_text,
        corrections=corrections,
    )
