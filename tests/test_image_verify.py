"""이미지 마스킹 검증 단위 테스트 (이슈 #18, 노션 5장).

PIL/numpy·GPU 없이 검증한다 — 판정(``decide``)은 순수이고, ``verify_redacted_page``는
커버리지 측정(measure)과 OCR 엔진을 주입해 5.1+5.2 흐름만 본다. 잔류 검사는 실제
``find_residual_pii``로 돌려 재OCR 텍스트의 PII 탐지까지 함께 검증한다.
"""

from ocr_worker.masking.image_verify import (
    COVERAGE_THRESHOLD,
    decide,
    verify_redacted_page,
)
from ocr_worker.masking.spans import PiiLabel, Span
from ocr_worker.ocr import OcrLine, OcrPage


def _line(text: str) -> OcrLine:
    return OcrLine(text=text, bbox=(0.0, 0.0, 10.0, 5.0), polygon=(), confidence=1.0)


# ── decide (순수) ────────────────────────────────────────────────
def test_decide_passes_when_covered_and_no_residual() -> None:
    result = decide({0: 0.99, 1: 1.0}, [])

    assert result.passed is True
    assert result.needs_review is False
    assert result.low_coverage_lines == ()


def test_decide_fails_on_low_coverage() -> None:
    # line1이 임계 미만 → 5.1 실패
    result = decide({0: 0.99, 1: 0.80}, [])

    assert result.passed is False
    assert result.low_coverage_lines == (1,)


def test_decide_fails_on_residual_pii() -> None:
    residual = [Span(0, 13, PiiLabel.RRN)]
    result = decide({0: 1.0}, residual)

    assert result.passed is False
    assert result.residual_pii == (residual[0],)


def test_decide_threshold_boundary_inclusive() -> None:
    # 정확히 임계값이면 통과(미만일 때만 실패)
    result = decide({0: COVERAGE_THRESHOLD}, [])

    assert result.passed is True


# ── verify_redacted_page (측정·엔진 주입) ────────────────────────
class _FakeEngine:
    """재OCR 결과를 고정 텍스트로 돌려주는 OCR 엔진 대역."""

    def __init__(self, text: str) -> None:
        self._text = text

    def recognize(self, images: list[object]) -> list[list[OcrLine]]:
        return [[_line(self._text)]]


def test_verify_passes_when_clean() -> None:
    page = OcrPage(0, 100, 50, (_line("홍길동"),))

    result = verify_redacted_page(
        image="masked",
        page=page,
        line_indices={0},
        engine=_FakeEngine("내용 없음"),  # 재OCR에 PII 없음
        measure=lambda _img, _pg, _idx: {0: 1.0},  # 커버리지 충분
    )

    assert result.passed is True


def test_verify_fails_when_reocr_finds_residual_rrn() -> None:
    page = OcrPage(0, 100, 50, (_line("홍길동"),))

    # 블럭이 빗나가 재OCR에 주민번호가 다시 읽힘 → 5.2 실패
    result = verify_redacted_page(
        image="masked",
        page=page,
        line_indices={0},
        engine=_FakeEngine("901011-1234567"),
        measure=lambda _img, _pg, _idx: {0: 1.0},
    )

    assert result.passed is False
    assert result.needs_review is True
    assert len(result.residual_pii) == 1


def test_verify_fails_when_coverage_low() -> None:
    page = OcrPage(0, 100, 50, (_line("홍길동"),))

    result = verify_redacted_page(
        image="masked",
        page=page,
        line_indices={0},
        engine=_FakeEngine("내용 없음"),
        measure=lambda _img, _pg, _idx: {0: 0.5},  # 커버리지 미달
    )

    assert result.passed is False
    assert result.low_coverage_lines == (0,)
