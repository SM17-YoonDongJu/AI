"""RRF(Reciprocal Rank Fusion) 순수 함수.

tsvector 키워드 검색과 pgvector 벡터 검색의 순위 리스트를 하나로 통합한다. 부수효과가
없어 단위 테스트가 쉽다(랭크→점수, 다중 리스트 통합 순위). 04 전략의 통합 단계.
"""

from collections.abc import Sequence

# RRF 상수. score = Σ weight_i / (RRF_K + rank_i). 60은 RRF 원논문 권장값으로,
# 상위권 순위 차이는 살리되 하위 노이즈의 영향을 완만하게 누른다.
RRF_K = 60

# tsvector·벡터 기본 가중치(0.5:0.5). namespace별로 호출자가 조정할 수 있다.
KEYWORD_WEIGHT = 0.5
VECTOR_WEIGHT = 0.5


def rrf_fuse(
    rankings: Sequence[tuple[Sequence[str], float]],
    *,
    k: int = RRF_K,
) -> list[tuple[str, float]]:
    """여러 순위 리스트를 RRF로 통합해 점수 내림차순 결과를 반환한다.

    Args:
        rankings: `(정렬된_id_리스트, 가중치)` 튜플들. 각 리스트는 점수가 높은 순으로
            정렬되어 있어야 한다(rank는 0-based 위치 + 1로 계산).
        k: RRF 완충 상수. 기본 `RRF_K`(60).

    Returns:
        `(id, 통합점수)` 리스트. 통합점수 내림차순, 동점이면 id 오름차순으로 안정 정렬.
    """
    scores: dict[str, float] = {}
    for ids, weight in rankings:
        for position, doc_id in enumerate(ids):
            rank = position + 1  # 1-based
            scores[doc_id] = scores.get(doc_id, 0.0) + weight / (k + rank)
    # 점수 내림차순, 동점은 id 오름차순(결정적 출력).
    return sorted(scores.items(), key=lambda item: (-item[1], item[0]))
