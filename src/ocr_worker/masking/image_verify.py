"""이미지 마스킹 검증 (이슈 #18, 노션 "이미지 마스킹 상세" 5장).

OCR·PII 검출은 불완전하다 — 잘못 마스킹된 사본을 만들고 **원본까지 지우면** 복구가
영영 불가하다. 그래서 비식별 사본은 **두 검사를 통과해야** 신뢰하고, 원본 삭제도 이
검증을 게이트로 둔다(통과 전 삭제 금지).

- **5.1 bbox 커버리지**: PII로 판정된 라인 bbox가 사본에서 실제로 가려졌는가
  (검정 픽셀 비율 ≥ 임계). 가장자리가 새는 부분 마스킹(B안)·좌표 오차를 잡는다.
- **5.2 재OCR 잔류 검사**: 사본을 다시 OCR해 PII 패턴이 읽히는가
  (``find_residual_pii`` 재사용). 블럭이 빗나간 false negative를 잡는다.

맹점(둘 다 못 잡음): OCR이 애초에 못 읽는 PII(서명·인감·손글씨) — 라인이 없어
가려지지도, 재OCR로 읽히지도 않는다. 이는 레이아웃/서명 탐지나 사람 검수로만
커버되며, 본 모듈 범위 밖이다(호출자가 별도 정책으로 다룬다).

순수 판정(``decide``)과 무거운 측정(PIL/numpy·재OCR)을 분리해, 측정·엔진을 주입하면
이미지 라이브러리·GPU 없이 검증 흐름을 테스트한다.
"""

from collections.abc import Callable
from dataclasses import dataclass

from ocr_worker.masking.spans import Span
from ocr_worker.masking.verify import find_residual_pii
from ocr_worker.ocr import OcrEngine, OcrPage, PageImage

# 5.1: PII bbox 영역이 이 비율 이상 검정이어야 통과(노션 표 기준 95%).
COVERAGE_THRESHOLD = 0.95
# 픽셀이 "검정"으로 인정되는 채널 상한(JPEG 압축 노이즈 허용).
_BLACK_TOLERANCE = 10

type CoverageFn = Callable[[PageImage, OcrPage, set[int]], dict[int, float]]


@dataclass(frozen=True, slots=True)
class VerificationResult:
    """이미지 마스킹 검증 결과.

    Attributes:
        passed: 두 검사를 모두 통과했는가(원본 삭제 게이트).
        low_coverage_lines: 커버리지 미달 라인 인덱스(5.1 실패).
        residual_pii: 재OCR에서 잔류한 PII 스팬(5.2 실패).
    """

    passed: bool
    low_coverage_lines: tuple[int, ...]
    residual_pii: tuple[Span, ...]

    @property
    def needs_review(self) -> bool:
        """통과하지 못함 → 재처리 또는 수동검수 대상(원본 삭제 금지)."""
        return not self.passed


def decide(
    coverage: dict[int, float],
    residual: list[Span],
    threshold: float = COVERAGE_THRESHOLD,
) -> VerificationResult:
    """커버리지·잔류 결과로 통과 여부를 판정한다(순수).

    Args:
        coverage: 라인 인덱스 → 검정 픽셀 비율(0~1).
        residual: 재OCR 잔류 PII 스팬.
        threshold: 커버리지 통과 임계.

    Returns:
        ``VerificationResult``. 미달 라인이 없고 잔류 PII도 없을 때만 ``passed``.
    """
    low = tuple(sorted(index for index, ratio in coverage.items() if ratio < threshold))
    passed = not low and not residual
    return VerificationResult(passed=passed, low_coverage_lines=low, residual_pii=tuple(residual))


def measure_coverage(image: PageImage, page: OcrPage, line_indices: set[int]) -> dict[int, float]:
    """각 PII 라인 bbox에서 검정 픽셀 비율을 잰다(5.1, numpy).

    이미지가 작거나 bbox가 화면 밖이면 0.0(미커버)으로 본다.
    """
    import numpy as np

    array = np.asarray(image.convert("RGB"))
    height, width = int(array.shape[0]), int(array.shape[1])
    ratios: dict[int, float] = {}
    for index in line_indices:
        x0, y0, x1, y1 = (int(value) for value in page.lines[index].bbox)
        x0, y0 = max(0, x0), max(0, y0)
        x1, y1 = min(width, x1), min(height, y1)
        if x1 <= x0 or y1 <= y0:
            ratios[index] = 0.0
            continue
        region = array[y0:y1, x0:x1]
        black = (region <= _BLACK_TOLERANCE).all(axis=-1)
        ratios[index] = float(black.mean())
    return ratios


def reocr_residual_pii(image: PageImage, engine: OcrEngine) -> list[Span]:
    """마스킹 사본을 재OCR해 잔류 PII를 스캔한다(5.2)."""
    pages = engine.recognize([image])
    text = "\n".join(line.text for line in pages[0]) if pages else ""
    return find_residual_pii(text)


def verify_redacted_page(
    image: PageImage,
    page: OcrPage,
    line_indices: set[int],
    *,
    engine: OcrEngine,
    measure: CoverageFn | None = None,
    threshold: float = COVERAGE_THRESHOLD,
) -> VerificationResult:
    """비식별 사본 1장을 5.1+5.2로 검증한다.

    Args:
        image: 마스킹된 페이지 이미지.
        page: 해당 페이지 OCR 결과(원본 라인 bbox).
        line_indices: 가렸어야 할 PII 라인 인덱스.
        engine: 재OCR용 OCR 엔진(주입 — 테스트는 페이크).
        measure: 커버리지 측정 함수(주입). 기본은 ``measure_coverage``.
        threshold: 커버리지 통과 임계.

    Returns:
        ``VerificationResult``. ``passed``가 아니면 원본을 삭제하지 말고
        재처리/수동검수로 보낸다.
    """
    measure_fn = measure or measure_coverage
    coverage = measure_fn(image, page, line_indices)
    residual = reocr_residual_pii(image, engine)
    return decide(coverage, residual, threshold)
