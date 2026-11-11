"""쿼리 라우터 테스트 — namespace 결정·비신체보험 범위 외 차단."""

from rag.router import DEFAULT_NAMESPACES, route


def test_non_bodily_query_is_out_of_scope() -> None:
    # Arrange / Act: 자동차(비신체) 힌트 포함 쿼리
    decision = route("자동차 사고 후 후유장해 보상")

    # Assert
    assert decision.in_scope is False
    assert decision.namespaces == []
    assert decision.reason is not None


def test_non_bodily_insurance_type_is_out_of_scope() -> None:
    # Arrange / Act: insurance_type 힌트가 비신체
    decision = route("후유장해 보상 범위", insurance_type="화재보험")

    # Assert
    assert decision.in_scope is False
    assert decision.reason is not None


def test_default_namespaces_when_none() -> None:
    # Act
    decision = route("상해 후유장해 지급 기준")

    # Assert
    assert decision.in_scope is True
    assert decision.namespaces == list(DEFAULT_NAMESPACES)


def test_explicit_namespaces_are_filtered_and_ordered() -> None:
    # Act: 유효 namespace만 입력 순서대로 남는다
    decision = route("질병 진단 기준", namespaces=["case", "terms", "bogus"])

    # Assert
    assert decision.namespaces == ["case", "terms"]
    assert decision.in_scope is True


def test_no_valid_namespace_is_out_of_scope() -> None:
    # Act
    decision = route("상해 보상", namespaces=["medical"])

    # Assert
    assert decision.in_scope is False
    assert decision.namespaces == []
    assert decision.reason is not None
