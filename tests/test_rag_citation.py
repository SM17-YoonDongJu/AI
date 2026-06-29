"""인용 역추적 테스트 — 행→Citation 매핑·중복 제거·빈 메타 스킵."""

from rag.search import ChunkRow, build_citations


def _row(chunk_id: str, clause_no: str | None, exhibit: str | None) -> ChunkRow:
    return ChunkRow(
        chunk_id=chunk_id,
        namespace="terms",
        content="본문",
        clause_no=clause_no,
        exhibit=exhibit,
    )


def test_maps_clause_and_exhibit() -> None:
    # Arrange
    rows = [_row("t1", "제3조", "별표2")]

    # Act
    citations = build_citations(rows)

    # Assert
    assert len(citations) == 1
    assert citations[0].clause_no == "제3조"
    assert citations[0].exhibit == "별표2"


def test_skips_rows_without_metadata() -> None:
    # Arrange: 메타 없는 행은 인용에서 제외
    rows = [_row("t1", None, None), _row("t2", "제5조", None)]

    # Act
    citations = build_citations(rows)

    # Assert
    assert len(citations) == 1
    assert citations[0].clause_no == "제5조"


def test_deduplicates_identical_citations() -> None:
    # Arrange: 동일 (조항, 별표) 중복
    rows = [_row("t1", "제3조", "별표2"), _row("t2", "제3조", "별표2")]

    # Act
    citations = build_citations(rows)

    # Assert
    assert len(citations) == 1
