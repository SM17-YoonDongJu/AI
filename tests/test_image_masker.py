"""이미지 마스킹 트랙 단위 테스트 (이슈 #18).

PIL 없이 검증한다 — "어느 라인이 PII인가"(``pii_line_indices``)와 "그 라인 어느
구간이 PII인가"(``line_local_spans``)는 순수 함수이고, ``ImageMasker``는 detect·
redactor를 주입해 렌더(PIL)를 배제한 채 오케스트레이션만 본다. 스팬은 실제 검출
대신 오프셋·라벨을 직접 만들어 라인 매핑 로직을 고립 검증한다.
"""

import pytest

from ocr_worker.masking.image_masker import (
    ImageMasker,
    line_local_spans,
    pii_line_indices,
    reading_order,
    reading_order_text,
)
from ocr_worker.masking.spans import PiiLabel, Span
from ocr_worker.ocr import OcrLine, OcrPage, OcrResult


def _line(text: str, *, bbox: tuple[float, float, float, float] = (0.0, 0.0, 10.0, 5.0)) -> OcrLine:
    return OcrLine(
        text=text,
        bbox=bbox,
        polygon=((0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)),
        confidence=1.0,
    )


def _page(texts: list[str]) -> OcrPage:
    # 라인 텍스트: "aaaa","bbbb","cccc" → 동일 bbox라 검출 순서 == 리딩오더
    # → reading_order_text 오프셋 [0,4) [5,9) [10,14)
    return OcrPage(index=0, width=100, height=50, lines=tuple(_line(t) for t in texts))


# ── pii_line_indices (순수) ──────────────────────────────────────
def test_span_maps_to_its_line() -> None:
    page = _page(["aaaa", "bbbb", "cccc"])
    order = reading_order(page)

    # line1 = [5,9) 안의 스팬
    indices = pii_line_indices(page, [Span(5, 7, PiiLabel.NAME)], order)

    assert indices == {1}


def test_span_crossing_newline_marks_both_lines() -> None:
    page = _page(["aaaa", "bbbb", "cccc"])
    order = reading_order(page)

    # [3,6)은 line0([0,4))과 line1([5,9)) 양쪽에 걸침 → 둘 다 가린다(부분 노출 방지)
    indices = pii_line_indices(page, [Span(3, 6, PiiLabel.RRN)], order)

    assert indices == {0, 1}


def test_label_filter_selects_only_matching_groups() -> None:
    page = _page(["aaaa", "bbbb", "cccc"])
    order = reading_order(page)
    spans = [Span(0, 2, PiiLabel.NAME), Span(10, 12, PiiLabel.RRN)]

    # 고유식별정보(RRN)만 가리는 프로파일 → line2만
    indices = pii_line_indices(page, spans, order, labels={PiiLabel.RRN})

    assert indices == {2}


def test_no_spans_returns_empty() -> None:
    page = _page(["aaaa", "bbbb"])

    assert pii_line_indices(page, [], reading_order(page)) == set()


def test_reading_order_sorts_by_visual_position_not_detection_order() -> None:
    # 검출 순서: "value"(라벨 아래, y=10)가 "label"(y=0)보다 먼저 나옴 — surya가 흔히
    # 만드는 뒤섞인 순서. reading_order는 bbox(y0, x0) 기준으로 재정렬해야 한다.
    page = OcrPage(
        index=0,
        width=100,
        height=50,
        lines=(
            _line("value", bbox=(0.0, 10.0, 10.0, 15.0)),
            _line("label", bbox=(0.0, 0.0, 10.0, 5.0)),
        ),
    )

    order = reading_order(page)

    assert order == [1, 0]  # 원래 인덱스 1("label")이 시각적으로 먼저


def test_pii_line_indices_maps_back_to_original_index_after_reorder() -> None:
    # 리딩오더로는 "label"(원래 인덱스1)이 먼저, "value"(원래 인덱스0)가 나중.
    page = OcrPage(
        index=0,
        width=100,
        height=50,
        lines=(
            _line("target", bbox=(0.0, 10.0, 10.0, 15.0)),  # 원래 인덱스 0
            _line("label", bbox=(0.0, 0.0, 10.0, 5.0)),  # 원래 인덱스 1
        ),
    )
    order = reading_order(page)  # [1, 0] → 조인 텍스트 "label\ntarget"
    text = reading_order_text(page, order)
    assert text == "label\ntarget"
    start = text.index("target")

    indices = pii_line_indices(page, [Span(start, start + 6, PiiLabel.NAME)], order)

    assert indices == {0}  # "target"의 원래 라인 인덱스(0)를 정확히 되짚는다


# ── line_local_spans (순수) ───────────────────────────────────────
def test_line_local_span_has_line_local_offset() -> None:
    page = _page(["aaaa", "bbbb", "cccc"])
    order = reading_order(page)

    # line1 = [5,9) 안의 스팬 [5,7) → 라인 로컬 오프셋으로는 [0,2)
    result = line_local_spans(page, [Span(5, 7, PiiLabel.NAME)], order)

    assert result == {1: [Span(0, 2, PiiLabel.NAME)]}


def test_line_local_span_crossing_newline_clips_each_line() -> None:
    page = _page(["aaaa", "bbbb", "cccc"])
    order = reading_order(page)

    # [3,6)은 line0([0,4))과 line1([5,9)) 양쪽에 걸침 → 각자 라인 로컬 오프셋으로 클리핑
    result = line_local_spans(page, [Span(3, 6, PiiLabel.RRN)], order)

    assert result == {
        0: [Span(3, 4, PiiLabel.RRN)],
        1: [Span(0, 1, PiiLabel.RRN)],
    }


def test_line_local_spans_no_spans_returns_empty_dict() -> None:
    page = _page(["aaaa", "bbbb"])

    assert line_local_spans(page, [], reading_order(page)) == {}


def test_line_local_spans_maps_back_to_original_index_after_reorder() -> None:
    # 리딩오더로는 "label"(원래 인덱스1)이 먼저, "target"(원래 인덱스0)이 나중.
    page = OcrPage(
        index=0,
        width=100,
        height=50,
        lines=(
            _line("target", bbox=(0.0, 10.0, 10.0, 15.0)),  # 원래 인덱스 0
            _line("label", bbox=(0.0, 0.0, 10.0, 5.0)),  # 원래 인덱스 1
        ),
    )
    order = reading_order(page)  # [1, 0] → 조인 텍스트 "label\ntarget"
    text = reading_order_text(page, order)
    start = text.index("target")

    result = line_local_spans(page, [Span(start, start + 6, PiiLabel.NAME)], order)

    # "target"의 원래 라인 인덱스(0)로 되짚되, 로컬 오프셋은 그 라인 기준 [0,6)
    assert result == {0: [Span(0, 6, PiiLabel.NAME)]}


# ── ImageMasker 오케스트레이션 (PIL 없이, 주입) ──────────────────
def test_redact_pages_only_redacts_pii_pages() -> None:
    page_pii = OcrPage(0, 100, 50, (_line("pii here"),))  # detect가 스팬 반환
    page_clean = OcrPage(1, 100, 50, (_line("clean"),))  # detect가 빈 리스트
    result = OcrResult(pages=(page_pii, page_clean))
    images = ["img0", "img1"]

    calls: list[tuple[str, set[int]]] = []

    def fake_detect(text: str) -> list[Span]:
        return [Span(0, 3, PiiLabel.NAME)] if "pii" in text else []

    def fake_redactor(image: object, page: OcrPage, indices: set[int]) -> str:
        calls.append((str(image), indices))
        return f"redacted:{sorted(indices)}"

    masker = ImageMasker(detect=fake_detect, redactor=fake_redactor)
    out = masker.redact_pages(images, result)

    assert out == ["redacted:[0]", "img1"]  # PII 페이지만 사본, clean은 원본 그대로
    assert calls == [("img0", {0})]  # redactor는 PII 페이지에만 호출


def test_redact_pages_raises_on_count_mismatch() -> None:
    result = OcrResult(pages=(OcrPage(0, 100, 50, (_line("x"),)),))

    masker = ImageMasker(detect=lambda _t: [], redactor=lambda i, _p, _idx: i)
    with pytest.raises(ValueError, match="수 불일치"):
        masker.redact_pages(["a", "b"], result)
