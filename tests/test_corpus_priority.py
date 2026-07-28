"""priority.compute 우선순위 점수 테스트 (이슈 #35 P1).

순수 함수라 네트워크·DB 없이 tier_base 매핑과 null→P2 규칙만 검증한다.
"""

from corpus_worker import priority
from corpus_worker.repository import FileRecord, SourceRecord


def _source(tier: str | None) -> SourceRecord:
    return SourceRecord(notion_page_id="s", name="출처", priority_tier=tier)


def _file() -> FileRecord:
    return FileRecord(notion_page_id="f")


def test_tier_base_scores_mapping() -> None:
    # Arrange / Act / Assert: P0~P3 → 고정 점수
    assert priority.compute(_source("P0"), _file()) == 3000.0
    assert priority.compute(_source("P1"), _file()) == 2000.0
    assert priority.compute(_source("P2"), _file()) == 1000.0
    assert priority.compute(_source("P3"), _file()) == 0.0


def test_none_tier_defaults_to_p2() -> None:
    # Arrange / Act
    score = priority.compute(_source(None), _file())

    # Assert
    assert score == priority.DEFAULT_TIER_SCORE
    assert score == 1000.0


def test_none_source_defaults_to_p2() -> None:
    # Arrange / Act / Assert: 출처가 없으면 P2 취급
    assert priority.compute(None, _file()) == 1000.0


def test_unknown_tier_defaults_to_p2() -> None:
    # Arrange / Act / Assert: 계약 밖 tier 문자열도 안전하게 P2로
    assert priority.compute(_source("P9"), _file()) == 1000.0
