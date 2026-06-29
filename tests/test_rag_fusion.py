"""RRF 융합 순수 함수 테스트 (외부 의존 없음).

랭크→점수 계산과 다중 리스트 통합 순위를 직접 검증한다.
"""

from rag.fusion import RRF_K, rrf_fuse


def test_rrf_k_constant_is_sixty() -> None:
    # Assert: 04 전략 상수 고정
    assert RRF_K == 60


def test_single_list_ranks_descending_by_position() -> None:
    # Arrange
    ranking = (["a", "b", "c"], 1.0)

    # Act
    fused = rrf_fuse([ranking])

    # Assert: 앞 순위일수록 높은 점수, 입력 순서 유지
    ids = [doc_id for doc_id, _ in fused]
    assert ids == ["a", "b", "c"]
    assert fused[0][1] == 1.0 / (RRF_K + 1)
    assert fused[1][1] == 1.0 / (RRF_K + 2)


def test_two_lists_fuse_by_summed_reciprocal_rank() -> None:
    # Arrange: b는 두 리스트에서 모두 상위 → 통합 1위가 되어야 함
    list_one = (["a", "b", "c"], 0.5)
    list_two = (["b", "c", "a"], 0.5)

    # Act
    fused = rrf_fuse([list_one, list_two])

    # Assert
    assert fused[0][0] == "b"


def test_weights_bias_toward_heavier_retriever() -> None:
    # Arrange: 가중치 큰 리스트의 1위가 최종 1위로 올라온다
    keyword = (["k_top", "shared"], 0.9)
    vector = (["shared", "k_top"], 0.1)

    # Act
    fused = rrf_fuse([keyword, vector])

    # Assert
    assert fused[0][0] == "k_top"


def test_tie_breaks_by_id_ascending() -> None:
    # Arrange: 완전 대칭 → 동점, id 오름차순으로 안정 정렬
    fused = rrf_fuse([(["a", "b"], 0.5), (["b", "a"], 0.5)])

    # Assert
    assert [doc_id for doc_id, _ in fused] == ["a", "b"]
