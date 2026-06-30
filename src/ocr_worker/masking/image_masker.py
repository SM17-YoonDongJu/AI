"""이미지 마스킹 트랙 (이슈 #18, 노션 B안) — PII 라인 검은블럭 비식별 사본.

손해사정사 비식별 열람용으로, 원본 페이지 이미지에서 **PII가 포함된 OCR 라인**을
줄 단위 검은블럭으로 가린 사본을 만든다(노션 A안: 줄 단위 추천 — 라벨까지 가려도 무방).

핵심 설계:
- **검출 1회 공유**: 텍스트 마스킹과 같은 ``Masker.detect``로 스팬을 얻어, 텍스트
  치환과 이미지 좌표 마스킹이 한 번의 탐지를 공유한다.
- **좌표만 사용**: 라인의 ``bbox``/``polygon``(PII 아님)으로 가릴 영역을 정한다. 텍스트
  자체는 노출하지 않는다.
- **순수/렌더 분리**: "어느 라인이 PII인가"(``pii_line_indices``)는 순수 함수라 PIL 없이
  테스트하고, PIL 렌더(``redact_page_image``)는 lazy import로 격리한다(배포 의존).
- **소비자 프로파일**: ``labels``로 가릴 PII 분류를 제한할 수 있다(예: 고유식별정보만
  가리고 성명은 유지). ``None``이면 모든 PII.
"""

from collections.abc import Callable

from ocr_worker.masking.masker import Masker
from ocr_worker.masking.spans import PiiLabel, Span
from ocr_worker.ocr import OcrPage, OcrResult, PageImage

# OcrPage.text가 "\n".join이므로 오프셋 계산도 동일 구분자를 쓴다.
_LINE_SEPARATOR = "\n"
# 검은블럭 채움색(불투명 검정).
_REDACT_FILL = (0, 0, 0)

type DetectFn = Callable[[str], list[Span]]
type RedactFn = Callable[[PageImage, OcrPage, set[int]], PageImage]


def _line_char_ranges(page: OcrPage) -> list[tuple[int, int]]:
    """각 라인이 ``page.text``에서 차지하는 ``[start, end)`` 문자 범위를 만든다.

    ``OcrPage.text``(``"\\n".join``)와 정확히 같은 오프셋 체계를 재현해, 텍스트에서
    검출한 스팬을 라인으로 되짚을 수 있게 한다.
    """
    ranges: list[tuple[int, int]] = []
    cursor = 0
    for line in page.lines:
        start = cursor
        end = start + len(line.text)
        ranges.append((start, end))
        cursor = end + len(_LINE_SEPARATOR)
    return ranges


def pii_line_indices(
    page: OcrPage, spans: list[Span], labels: set[PiiLabel] | None = None
) -> set[int]:
    """PII 스팬이 걸친 라인 인덱스 집합을 반환한다(줄 단위 마스킹 대상).

    스팬은 ``page.text`` 기준 문자 오프셋이다. 스팬이 라인 경계를 넘으면 닿는 라인을
    모두 포함한다(안전 우선 — 부분 노출 방지).

    Args:
        page: OCR 페이지(라인 텍스트·좌표 보유).
        spans: ``page.text``에서 검출된 PII 스팬.
        labels: 가릴 PII 분류 집합. ``None``이면 모든 PII를 가린다.

    Returns:
        검은블럭으로 가릴 라인 인덱스 집합(가릴 것이 없으면 빈 집합).
    """
    selected = [span for span in spans if labels is None or span.label in labels]
    if not selected:
        return set()
    ranges = _line_char_ranges(page)
    indices: set[int] = set()
    for span in selected:
        for index, (start, end) in enumerate(ranges):
            if span.start < end and span.end > start:  # 범위 겹침
                indices.add(index)
    return indices


def redact_page_image(image: PageImage, page: OcrPage, line_indices: set[int]) -> PageImage:
    """지정 라인을 검은블럭으로 가린 페이지 이미지 **사본**을 반환한다(PIL, lazy).

    polygon이 있으면 polygon을, 없으면 ``bbox`` 사각형을 채운다. 원본 이미지는
    변형하지 않는다(사본에 그린다).
    """
    from PIL import ImageDraw

    redacted = image.convert("RGB").copy()
    draw = ImageDraw.Draw(redacted)
    for index in line_indices:
        line = page.lines[index]
        if line.polygon:
            draw.polygon([tuple(point) for point in line.polygon], fill=_REDACT_FILL)
        else:
            draw.rectangle(list(line.bbox), fill=_REDACT_FILL)
    return redacted


def image_to_png_bytes(image: PageImage) -> bytes:
    """PIL 이미지를 PNG 바이트로 인코딩한다(비식별 사본 S3 업로드용, lazy)."""
    import io

    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


class ImageMasker:
    """OCR 결과의 PII 라인을 가린 비식별 페이지 이미지를 생성한다.

    검출은 텍스트 마스킹과 같은 ``Masker``를 공유한다(검출 1회). PIL 렌더는 주입
    가능(``redactor``)이라 이미지 라이브러리 없이 선택 로직을 테스트한다.
    """

    def __init__(
        self,
        *,
        detect: DetectFn | None = None,
        redactor: RedactFn | None = None,
        labels: set[PiiLabel] | None = None,
    ) -> None:
        """이미지 마스커를 구성한다.

        Args:
            detect: 텍스트 → 스팬 검출 함수. 기본은 ``Masker().detect``(정규식+USE_NER).
            redactor: 라인 검은블럭 렌더 함수. 기본은 ``redact_page_image``.
            labels: 가릴 PII 분류. ``None``이면 모든 PII(소비자 프로파일 확장점).
        """
        self._detect: DetectFn = detect or Masker().detect
        self._redactor: RedactFn = redactor or redact_page_image
        self._labels = labels

    def redact_pages(self, images: list[PageImage], result: OcrResult) -> list[PageImage]:
        """페이지별로 PII 라인을 가린 이미지 목록을 반환한다.

        PII가 없는 페이지는 원본 이미지를 그대로 돌려준다(불필요한 사본 생성 회피).

        Args:
            images: 원본 페이지 이미지(``OcrProcessor``가 렌더한 것과 동일 순서·길이).
            result: 같은 문서의 OCR 결과(라인 좌표 보유).

        Returns:
            페이지별 비식별 이미지 목록.

        Raises:
            ValueError: 이미지 수와 OCR 페이지 수가 다를 때(계약 위반).
        """
        if len(images) != len(result.pages):
            raise ValueError(
                f"이미지/페이지 수 불일치: images={len(images)} pages={len(result.pages)}"
            )
        redacted: list[PageImage] = []
        for image, page in zip(images, result.pages, strict=True):
            spans = self._detect(page.text)
            indices = pii_line_indices(page, spans, self._labels)
            redacted.append(self._redactor(image, page, indices) if indices else image)
        return redacted
