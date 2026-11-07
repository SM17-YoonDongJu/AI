"""이미지 마스킹 트랙 단위 테스트 (이슈 #18).

PIL 없이 검증한다 — "어느 라인이 PII인가"(``pii_line_indices``)와 "그 라인 어느
구간이 PII인가"(``line_local_spans``)는 순수 함수이고, ``ImageMasker``는 detect·
redactor·reread를 주입해 렌더(PIL)·VLM을 배제한 채 오케스트레이션만 본다. 스팬은 실제 검출
대신 오프셋·라벨을 직접 만들어 라인 매핑 로직을 고립 검증한다.

VLM 라인 재판독(예전 grounding의 대체)은 두 가지를 고정한다: 신뢰도 게이트(고신뢰 문서는
호출조차 안 함)와, 재판독 텍스트로 찾은 PII 라인이 **라인 인덱스로** OCR 엔진 판정에 더해지는지
(5.1 커버리지가 그 라인을 재는 전제). 재판독 실패는 아무것도 보태지 않을 뿐 기존 판정을 깎지 않는다.
"""

from collections.abc import Sequence

import pytest

from ocr_worker.masking.image_masker import (
    ImageMasker,
    line_local_spans,
    pii_line_indices,
    reading_order,
    reading_order_text,
)
from ocr_worker.masking.masker import Masker
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
    # 검출 순서: "value"(라벨 아래, y=10)가 "label"(y=0)보다 먼저 나옴 — OCR 엔진이 흔히
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

    # PII 페이지만 사본, clean은 원본 그대로
    assert [page.image for page in out] == ["redacted:[0]", "img1"]
    assert calls == [("img0", {0})]  # redactor는 PII 페이지에만 호출
    # 검증(5.1)이 쓸 근거가 함께 나온다 — 가린 라인과 "실제로 가렸는가".
    assert [page.line_indices for page in out] == [{0}, set()]
    assert [page.redacted for page in out] == [True, False]


def test_redact_pages_raises_on_count_mismatch() -> None:
    result = OcrResult(pages=(OcrPage(0, 100, 50, (_line("x"),)),))

    masker = ImageMasker(detect=lambda _t: [], redactor=lambda i, _p, _idx: i)
    with pytest.raises(ValueError, match="수 불일치"):
        masker.redact_pages(["a", "b"], result)


# ── VLM 라인 재판독 병합(redact_pages, PIL 없이) ─────────────────
def _detect_name(text: str) -> list[Span]:
    """'홍길동'만 PII로 보는 검출기 대역 — 어느 라인이 가려지는지를 테스트가 통제한다."""
    start = text.find("홍길동")
    return [Span(start, start + 3, PiiLabel.NAME)] if start >= 0 else []


def _identity_redactor(image: object, page: OcrPage, indices: set[int]) -> object:
    return image


def _misread_name_page() -> OcrPage:
    # 시각적 순서 = 검출 순서: 라벨(위) → 값(아래). OCR 엔진이 이름을 "828"로 오독한 상황.
    return OcrPage(
        0,
        100,
        50,
        (
            _line("환자 성명", bbox=(0.0, 0.0, 40.0, 10.0)),
            _line("828", bbox=(0.0, 20.0, 40.0, 30.0)),
        ),
    )


def test_redact_pages_adds_reread_pii_line_that_surya_misread() -> None:
    # Arrange: OCR 엔진 텍스트로는 PII가 없지만 재판독 텍스트는 이름을 읽었다.
    result = OcrResult(pages=(_misread_name_page(),))
    calls: list[set[int]] = []

    def redactor(image: object, page: OcrPage, indices: set[int]) -> str:
        calls.append(set(indices))
        return "redacted"

    masker = ImageMasker(detect=_detect_name, redactor=redactor)

    # Act
    out = masker.redact_pages(["img0"], result, reread_texts=[[None, "홍길동"]])

    # Assert: 그 라인이 검은블록 대상(line_indices)에 들어간다 — 5.1 커버리지가 이 값으로 잰다.
    assert calls == [{1}]
    assert out[0].image == "redacted"
    assert out[0].line_indices == {1}
    assert out[0].reread_indices == frozenset({1})
    assert out[0].redacted is True


def test_redact_pages_reread_uses_page_context_for_split_label_and_value() -> None:
    # Arrange: 실제 정규식 마스커. 라벨("환자 성명")과 값이 다른 라인이라, 재판독한 값 라인만
    # 따로 검출하면 라벨이 없어 놓친다 — 텍스트만 바꾼 **페이지** 단위로 검출해야 잡힌다.
    result = OcrResult(pages=(_misread_name_page(),))
    masker = ImageMasker(detect=Masker(use_ner=False).detect, redactor=_identity_redactor)

    # Act
    engine_only = masker.redact_pages(["img0"], result)
    with_reread = masker.redact_pages(["img0"], result, reread_texts=[[None, "홍길동"]])

    # Assert
    assert engine_only[0].line_indices == set()
    assert with_reread[0].line_indices == {1}


def test_redact_pages_reread_adds_to_surya_lines_instead_of_replacing() -> None:
    # Arrange: OCR 엔진이 이미 잡은 라인(0)은 재판독이 그 라인을 달리 읽어도 그대로 가린다(합집합).
    page = OcrPage(
        0,
        100,
        50,
        (
            _line("홍길동", bbox=(0.0, 0.0, 40.0, 10.0)),
            _line("828", bbox=(0.0, 20.0, 40.0, 30.0)),
        ),
    )
    result = OcrResult(pages=(page,))
    masker = ImageMasker(detect=_detect_name, redactor=_identity_redactor)

    # Act
    out = masker.redact_pages(["img0"], result, reread_texts=[["흥길돔", "홍길동"]])

    # Assert
    assert out[0].line_indices == {0, 1}
    assert out[0].reread_indices == frozenset({1})  # 재판독으로만 찾은 라인


def test_redact_pages_failed_reread_lines_keep_surya_text() -> None:
    # Arrange: 재판독이 전부 실패(None) — 보탤 근거가 없을 뿐 OCR 엔진 판정은 그대로다.
    result = OcrResult(pages=(_misread_name_page(),))
    calls: list[set[int]] = []

    def redactor(image: object, page: OcrPage, indices: set[int]) -> str:
        calls.append(set(indices))
        return "redacted"

    masker = ImageMasker(detect=_detect_name, redactor=redactor)

    # Act
    out = masker.redact_pages(["img0"], result, reread_texts=[[None, None]])

    # Assert
    assert out[0].line_indices == set()
    assert out[0].redacted is False
    assert out[0].image == "img0"  # 가릴 게 없으면 원본 그대로
    assert calls == []


def test_redact_pages_reread_respects_label_filter() -> None:
    # Arrange: 소비자 프로파일(labels)은 재판독으로 찾은 라인에도 똑같이 걸린다(이름은 유지).
    result = OcrResult(pages=(_misread_name_page(),))
    masker = ImageMasker(detect=_detect_name, redactor=_identity_redactor, labels={PiiLabel.RRN})

    # Act
    out = masker.redact_pages(["img0"], result, reread_texts=[[None, "홍길동"]])

    # Assert
    assert out[0].line_indices == set()


def test_redact_pages_raises_on_reread_page_count_mismatch() -> None:
    result = OcrResult(pages=(_misread_name_page(),))
    masker = ImageMasker(detect=_detect_name, redactor=_identity_redactor)

    with pytest.raises(ValueError, match="reread_texts"):
        masker.redact_pages(["img0"], result, reread_texts=[[None, None], [None]])


def test_redact_pages_raises_on_reread_line_count_mismatch() -> None:
    result = OcrResult(pages=(_misread_name_page(),))
    masker = ImageMasker(detect=_detect_name, redactor=_identity_redactor)

    with pytest.raises(ValueError, match="재판독 텍스트 수 불일치"):
        masker.redact_pages(["img0"], result, reread_texts=[[None]])


# ── reread_pages (신뢰도 게이팅) ──────────────────────────────────
class _RecordingReread:
    """``vlm_client.reread_lines`` 대역 — 받은 인자를 기록하고 고정 텍스트를 돌려준다."""

    def __init__(self, texts: list[list[str | None]]) -> None:
        self._texts = texts
        self.calls: list[tuple[list[object], list[OcrPage]]] = []

    async def __call__(
        self, images: Sequence[object], pages: Sequence[OcrPage]
    ) -> list[list[str | None]]:
        self.calls.append((list(images), list(pages)))
        return self._texts


def _result_with_confidence(confidence: float) -> OcrResult:
    line = OcrLine(
        text="828",
        bbox=(0.0, 0.0, 10.0, 5.0),
        polygon=((0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)),
        confidence=confidence,
    )
    return OcrResult(pages=(OcrPage(0, 100, 50, (line,)),))


async def test_reread_pages_skips_when_reread_not_provided() -> None:
    masker = ImageMasker(detect=lambda _t: [])  # reread 주입 안 함

    out = await masker.reread_pages(["img0"], _result_with_confidence(0.5))

    assert out is None


async def test_reread_pages_skips_high_confidence_document() -> None:
    # Arrange: 고신뢰 문서는 OCR 엔진 검출로 충분하다고 본다 — VLM을 아예 부르지 않는다(비용).
    reread = _RecordingReread([["x"]])
    masker = ImageMasker(detect=lambda _t: [], reread=reread, reread_confidence_threshold=0.90)

    # Act
    out = await masker.reread_pages(["img0"], _result_with_confidence(0.95))

    # Assert
    assert out is None
    assert reread.calls == []


async def test_reread_pages_skips_document_exactly_at_threshold() -> None:
    # Arrange: 쪽 전사 게이트(< 0.90)와 같은 경계 — 임계값과 같으면 시도하지 않는다.
    reread = _RecordingReread([["x"]])
    masker = ImageMasker(detect=lambda _t: [], reread=reread, reread_confidence_threshold=0.90)

    # Act
    out = await masker.reread_pages(["img0"], _result_with_confidence(0.90))

    # Assert
    assert out is None
    assert reread.calls == []


async def test_reread_pages_calls_reread_for_low_confidence_document() -> None:
    # Arrange
    reread = _RecordingReread([["홍길동"]])
    result = _result_with_confidence(0.5)
    masker = ImageMasker(detect=lambda _t: [], reread=reread, reread_confidence_threshold=0.90)

    # Act
    out = await masker.reread_pages(["img0"], result)

    # Assert: 문서 전체(이미지·페이지)를 한 번에 넘긴다 — 우선순위·예산은 reread 쪽이 정한다.
    assert out == [["홍길동"]]
    assert reread.calls == [(["img0"], list(result.pages))]


async def test_reread_pages_default_threshold_never_rereads() -> None:
    # Arrange: 기본 임계값 0.0은 "항상 시도 안 함" — 켜려면 호출측이 임계값을 넘겨야 한다.
    reread = _RecordingReread([["x"]])
    masker = ImageMasker(detect=lambda _t: [], reread=reread)

    # Act
    out = await masker.reread_pages(["img0"], _result_with_confidence(0.5))

    # Assert
    assert out is None
    assert reread.calls == []


async def test_reread_pages_rejects_image_page_count_mismatch() -> None:
    reread = _RecordingReread([["x"]])
    masker = ImageMasker(detect=lambda _t: [], reread=reread, reread_confidence_threshold=0.90)

    with pytest.raises(ValueError, match="수 불일치"):
        await masker.reread_pages(["img0", "img1"], _result_with_confidence(0.5))
