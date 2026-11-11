"""Kiwi 토큰화 테스트 (kiwipiepy 실설치) — 내용어 추출 동작 검증."""

from rag.typo import extract_content_tokens


def test_extracts_content_nouns_and_drops_particles() -> None:
    # Arrange / Act: 명사는 남고 조사(을)는 빠진다
    tokens = extract_content_tokens("보험금을 청구합니다")

    # Assert
    assert "보험금" in tokens
    assert "을" not in tokens


def test_returns_nonempty_for_domain_query() -> None:
    # Act
    tokens = extract_content_tokens("교통사고 후유장해 보상 기준")

    # Assert: 도메인 내용어가 일부라도 추출된다
    assert len(tokens) >= 2
