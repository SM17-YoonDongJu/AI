"""OCR 오케스트레이션 단위 테스트 (이슈 #16).

surya·pymupdf·boto3는 배포 전용이라 로컬에 없다. 따라서 엔진·다운로드·렌더 경계를
**페이크로 주입**해 ``OcrProcessor``의 순수 오케스트레이션(라인 구조 보존·페이지 크기
매핑·평균 신뢰도·계약 위반 검출)과 content_type 라우팅 가드를 검증한다.
"""

import pytest

from core.exceptions import OcrError
from ocr_worker.ocr import (
    OcrLine,
    OcrPage,
    OcrProcessor,
    OcrResult,
    render_to_images,
)


# ── 페이크 ───────────────────────────────────────────────────────
class _FakeImage:
    """PIL.Image 대역 — 렌더 결과 이미지의 크기만 흉내낸다."""

    def __init__(self, width: int, height: int) -> None:
        self.width = width
        self.height = height


def _line(text: str, confidence: float) -> OcrLine:
    return OcrLine(
        text=text,
        bbox=(0.0, 0.0, 10.0, 5.0),
        polygon=((0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)),
        confidence=confidence,
    )


class _FakeEngine:
    """주어진 페이지별 라인을 그대로 돌려주는 OCR 엔진 대역."""

    def __init__(self, pages: list[list[OcrLine]]) -> None:
        self._pages = pages
        self.received: list[object] | None = None

    def recognize(self, images: list[object]) -> list[list[OcrLine]]:
        self.received = images
        return self._pages


def _make_processor(
    *, images: list[_FakeImage], pages: list[list[OcrLine]]
) -> tuple[OcrProcessor, _FakeEngine]:
    engine = _FakeEngine(pages)

    async def fake_fetch(_s3_key: str) -> bytes:
        return b"rawbytes"

    def fake_render(_data: bytes, _content_type: str) -> list[_FakeImage]:
        return images

    return OcrProcessor(engine=engine, fetch=fake_fetch, render=fake_render), engine


# ── OcrResult 접근자(순수) ───────────────────────────────────────
def test_first_page_text_joins_lines() -> None:
    result = OcrResult(
        pages=(OcrPage(0, 800, 600, (_line("보험증권", 1.0), _line("증권번호 123", 0.9))),)
    )

    assert result.first_page_text == "보험증권\n증권번호 123"


def test_full_text_spans_all_pages() -> None:
    result = OcrResult(
        pages=(
            OcrPage(0, 800, 600, (_line("first", 1.0),)),
            OcrPage(1, 800, 600, (_line("second", 1.0),)),
        )
    )

    assert result.full_text == "first\nsecond"


def test_mean_confidence_averages_all_lines() -> None:
    result = OcrResult(
        pages=(
            OcrPage(0, 800, 600, (_line("a", 1.0), _line("b", 0.8))),
            OcrPage(1, 800, 600, (_line("c", 0.6),)),
        )
    )

    assert result.mean_confidence == pytest.approx((1.0 + 0.8 + 0.6) / 3)


def test_mean_confidence_empty_is_zero() -> None:
    assert OcrResult(pages=()).mean_confidence == 0.0
    assert OcrResult(pages=()).first_page_text == ""


# ── OcrProcessor 오케스트레이션 ──────────────────────────────────
async def test_process_builds_pages_with_image_sizes() -> None:
    images = [_FakeImage(800, 600), _FakeImage(1024, 768)]
    pages = [[_line("p0", 1.0)], [_line("p1a", 0.9), _line("p1b", 0.7)]]
    processor, engine = _make_processor(images=images, pages=pages)

    result = await processor.process("uploads/x.pdf", "application/pdf")

    assert len(result.pages) == 2
    assert (result.pages[0].width, result.pages[0].height) == (800, 600)
    assert (result.pages[1].width, result.pages[1].height) == (1024, 768)
    assert result.pages[1].index == 1
    assert result.pages[0].lines[0].text == "p0"
    assert engine.received is images  # 렌더 이미지를 그대로 엔진에 전달


async def test_process_preserves_line_structure() -> None:
    images = [_FakeImage(800, 600)]
    pages = [[_line("증권번호 202301-042", 0.99)]]
    processor, _ = _make_processor(images=images, pages=pages)

    result = await processor.process("uploads/x.png", "image/png")

    line = result.pages[0].lines[0]
    assert line.bbox == (0.0, 0.0, 10.0, 5.0)
    assert line.polygon == ((0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0))
    assert line.confidence == 0.99


async def test_process_raises_on_page_count_mismatch() -> None:
    # 엔진이 이미지 2장에 대해 1페이지만 반환 → 계약 위반.
    images = [_FakeImage(800, 600), _FakeImage(800, 600)]
    pages = [[_line("only-one", 1.0)]]
    processor, _ = _make_processor(images=images, pages=pages)

    with pytest.raises(OcrError, match="페이지 수 불일치"):
        await processor.process("uploads/x.pdf", "application/pdf")


# ── content_type 라우팅 가드 ─────────────────────────────────────
def test_render_rejects_unsupported_content_type() -> None:
    # 가드는 PIL/pymupdf import 이전에 동작 → 로컬에서 검증 가능.
    with pytest.raises(OcrError, match="지원하지 않는 content_type"):
        render_to_images(b"data", "application/zip")
