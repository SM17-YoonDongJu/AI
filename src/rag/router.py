"""쿼리 라우터 — 검색할 namespace 조합을 룰 기반으로 결정한다.

현재 구현 대상은 신체 관련 보험 약관(terms, POLICY_CHUNKS)·분쟁조정사례(case,
CASE_CHUNKS)·후유장해분류표(level, SCHEDULE_CHUNKS) 3종이다. medical(수가·KCD)은 테이블
미존재로 향후 확장. 비신체보험(자동차·화재 등)은 범위 외로 안내한다.
"""

from dataclasses import dataclass

# 현재 검색 가능한 namespace. 소스 테이블로 부여되는 파생값과 동일 체계.
VALID_NAMESPACES: frozenset[str] = frozenset({"terms", "case", "level"})
# 힌트가 없을 때 기본 검색 대상. level(장해분류표)은 계약 체결일 버전 매칭이 필요해
# 명시 요청(namespaces=["level"]) 시에만 검색한다.
DEFAULT_NAMESPACES: tuple[str, ...] = ("terms", "case")

# 비신체보험(범위 외) 힌트 키워드. insurance_type 또는 쿼리 본문에서 탐지한다.
NON_BODILY_HINTS: tuple[str, ...] = (
    "자동차",
    "자차",
    "차량",
    "화재",
    "재물",
    "배상책임",
    "해상",
    "운송",
    "항공",
    "선박",
)

_OUT_OF_SCOPE_REASON = "비신체보험(자동차·화재 등) 관련 쿼리는 현재 검색 범위 밖입니다."
_NO_NAMESPACE_REASON = "검색 가능한 namespace가 없습니다(terms·case·level만 지원)."


@dataclass(slots=True)
class RouteDecision:
    """라우팅 결과. `in_scope=False`면 호출자는 빈 결과를 반환한다."""

    namespaces: list[str]  # 검색 대상 namespace(범위 밖이면 빈 리스트)
    in_scope: bool  # 신체보험 범위 내 여부
    reason: str | None  # 범위 밖/빈 결과 사유(in_scope=True면 None)


def _is_non_bodily(query: str, insurance_type: str | None) -> bool:
    """비신체보험 힌트가 보험 유형 또는 쿼리 본문에 있으면 True."""
    haystacks = [query]
    if insurance_type:
        haystacks.append(insurance_type)
    return any(hint in text for text in haystacks for hint in NON_BODILY_HINTS)


def route(
    query: str,
    insurance_type: str | None = None,
    namespaces: list[str] | None = None,
) -> RouteDecision:
    """검색할 namespace 조합을 결정한다.

    Args:
        query: 사용자 쿼리 텍스트.
        insurance_type: 신체보험 유형 힌트(비신체면 범위 외).
        namespaces: 명시 namespace. None이면 기본값(terms·case)을 쓴다.

    Returns:
        라우팅 결과(`RouteDecision`). 비신체보험이면 `in_scope=False` + 사유.
    """
    if _is_non_bodily(query, insurance_type):
        return RouteDecision(namespaces=[], in_scope=False, reason=_OUT_OF_SCOPE_REASON)

    candidates = namespaces if namespaces is not None else list(DEFAULT_NAMESPACES)
    # 유효 namespace만 남기되 입력 순서를 보존한다.
    selected = [ns for ns in candidates if ns in VALID_NAMESPACES]
    if not selected:
        return RouteDecision(namespaces=[], in_scope=False, reason=_NO_NAMESPACE_REASON)

    return RouteDecision(namespaces=selected, in_scope=True, reason=None)
