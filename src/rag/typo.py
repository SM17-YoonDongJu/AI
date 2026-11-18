"""trigram 오타 보정 — kiwipiepy 토큰화 + search_terms 정규 용어 치환.

쿼리를 형태소 분석해 내용어(명사·동사·형용사 어간 등)를 뽑고, 각 내용어를 search_terms의
pg_trgm 인덱스로 조회해 `similarity(term, 입력) > SIMILARITY_THRESHOLD`인 가장 유사한 정규
용어 1건으로 치환한다(04 오타 보정 단계). 보정 결과가 tsvector·임베딩 검색에 쓰인다.

Kiwi는 프로세스당 한 벌만 만든다. 한 벌이 최고 RSS 약 550~580MB다. 보장·장해 줄기(R6)가 동시에
첫 검색에 들어오면서 두 벌이 만들어졌고, report 파드가 한도(당시 768Mi)를 넘어 OOMKilled됐다(#113).
그래서 생성은 `_get_kiwi`의 이중 확인 락으로 1회로 묶고, 워커는 기동할 때 `preload_kiwi`로 미리
올려 둔다.
"""

import asyncio
import resource
import sys
import threading
from dataclasses import dataclass, field
from time import perf_counter

import asyncpg
from kiwipiepy import Kiwi

from core.logging import elapsed_ms, get_logger

logger = get_logger(__name__)

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

# 생성 직후 한 번 분석해 둘 문장. Kiwi는 첫 분석 때 지연 초기화를 한다(kiwipiepy 0.23.2·macOS
# 실측: 약 0.4초, RSS +125MB, 최고 RSS +200MB). 생성과 같은 락 안에서 끝내야 그 비용이 첫 작업으로
# 넘어가지 않고, 다른 스레드는 초기화를 마친 인스턴스만 받는다.
_WARMUP_TEXT = "보험금을 청구합니다"

_kiwi: Kiwi | None = None
# Kiwi 생성을 한 스레드로 묶는다. correct_query가 토큰화를 asyncio.to_thread로 띄우므로, 동시에 돈
# 검색들이 _get_kiwi에 함께 들어온다. 락이 없으면 모두 빈 _kiwi를 보고 각자 한 벌씩 만든다(#113).
_kiwi_lock = threading.Lock()


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
    """프로세스에 하나뿐인 Kiwi를 돌려준다. 아직 없으면 만든다(이중 확인 락).

    이미 있으면 락 없이 돌려준다(검색마다 락을 잡지 않는다). 비어 있으면 락을 잡고 다시 확인한다 —
    락을 기다리는 사이 다른 스레드가 만들었으면 그것을 쓴다. 생성이 실패하면 빈 채로 예외를 올리고,
    다음 호출이 다시 만든다.
    """
    global _kiwi
    if _kiwi is not None:
        return _kiwi
    with _kiwi_lock:
        if _kiwi is None:
            _kiwi = _load_kiwi()
        return _kiwi


def _load_kiwi() -> Kiwi:
    """Kiwi를 만들고 첫 분석의 지연 초기화까지 끝낸다. 걸린 시간과 최고 RSS를 로그로 남긴다.

    `_kiwi_lock` 안에서만 부른다. 그래서 `kiwi_loaded` 로그 수가 곧 생성 횟수다(프로세스당 1번).
    """
    started = perf_counter()
    kiwi = Kiwi()
    kiwi.tokenize(_WARMUP_TEXT)
    logger.info("kiwi_loaded", duration_ms=elapsed_ms(started), peak_rss_mb=_peak_rss_mb())
    return kiwi


def _peak_rss_mb() -> int:
    """이 프로세스의 최고 RSS(MiB). `ru_maxrss` 단위가 Linux는 KiB, macOS는 바이트라 맞춰 준다."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(peak / (1024 * 1024 if sys.platform == "darwin" else 1024))


def preload_kiwi() -> None:
    """Kiwi를 미리 만들어 둔다. 이미 있으면 아무것도 하지 않는다.

    동기 함수이고 1초 안팎 걸린다 — async 경로에선 `asyncio.to_thread`로 부른다. 워커가 소비를
    시작하기 전에 부르면 첫 작업이 생성·초기화 시간을 떠안지 않는다. 메모리가 모자라도 메시지를
    받기 전, 기동 단계에서 죽는다. 생성이 실패하면 그 예외를 그대로 올린다(기동 실패).
    """
    _get_kiwi()


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
