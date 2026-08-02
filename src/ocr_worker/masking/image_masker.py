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
# OCR bbox는 글자에 빡빡하게 잡혀 가장자리 획이 샐 수 있다 → 여백을 줘 확실히 덮는다
# (노션 "이미지 마스킹 상세" 5.1: bbox 여백 = 원본보다 일정 픽셀 확장).
BBOX_MARGIN_PX = 3

type DetectFn = Callable[[str], list[Span]]
type RedactFn = Callable[[PageImage, OcrPage, set[int]], PageImage]


def reading_order(page: OcrPage) -> list[int]:
    """bbox(y0, x0) 기준으로 정렬한 라인 인덱스 순서를 반환한다.

    surya 검출 순서가 표의 시각적 순서(위→아래, 좌→우)와 다를 수 있어, 앵커 정규식의
    lookahead가 "물리적으로 가까운" 라벨-값 쌍을 못 붙잡는 경우가 있다(예: 라벨과 값이
    검출 순서상 멀리 떨어진 라인으로 나옴). 좌표로 재정렬해 이 문제를 완화한다.
    """
    return sorted(
        range(len(page.lines)), key=lambda i: (page.lines[i].bbox[1], page.lines[i].bbox[0])
    )


def reading_order_text(page: OcrPage, order: list[int]) -> str:
    """``order`` 순서로 라인 텍스트를 이어붙인다(``_line_char_ranges``와 동일 구분자)."""
    return _LINE_SEPARATOR.join(page.lines[i].text for i in order)


def _line_char_ranges(page: OcrPage, order: list[int]) -> dict[int, tuple[int, int]]:
    """``reading_order_text(page, order)`` 기준으로, 원래 라인 인덱스별 ``[start, end)``.

    ``order``로 재배열해 조인한 텍스트에서 오프셋을 계산하되, 결과 딕셔너리의 키는
    ``page.lines``의 **원래** 인덱스다 — 검출한 스팬을 원래 라인으로 되짚을 수 있게 한다.
    """
    ranges: dict[int, tuple[int, int]] = {}
    cursor = 0
    for i in order:
        start = cursor
        end = start + len(page.lines[i].text)
        ranges[i] = (start, end)
        cursor = end + len(_LINE_SEPARATOR)
    return ranges


def pii_line_indices(
    page: OcrPage, spans: list[Span], order: list[int], labels: set[PiiLabel] | None = None
) -> set[int]:
    """PII 스팬이 걸친 라인 인덱스 집합을 반환한다(줄 단위 마스킹 대상).

    스팬은 ``reading_order_text(page, order)`` 기준 문자 오프셋이다. 스팬이 라인 경계를
    넘으면 닿는 라인을 모두 포함한다(안전 우선 — 부분 노출 방지).

    Args:
        page: OCR 페이지(라인 텍스트·좌표 보유).
        spans: ``reading_order_text(page, order)``에서 검출된 PII 스팬.
        order: ``reading_order(page)``가 반환한 정렬 순서(스팬 오프셋 계산과 일치해야 함).
        labels: 가릴 PII 분류 집합. ``None``이면 모든 PII를 가린다.

    Returns:
        검은블럭으로 가릴 라인 인덱스 집합(원래 ``page.lines`` 인덱스, 가릴 것이 없으면 빈 집합).
    """
    selected = [span for span in spans if labels is None or span.label in labels]
    if not selected:
        return set()
    ranges = _line_char_ranges(page, order)
    indices: set[int] = set()
    for span in selected:
        for index, (start, end) in ranges.items():
            if span.start < end and span.end > start:  # 범위 겹침
                indices.add(index)
    return indices


def redact_page_image(
    image: PageImage, page: OcrPage, line_indices: set[int], *, margin_px: int = BBOX_MARGIN_PX
) -> PageImage:
    """지정 라인을 검은블럭으로 가린 페이지 이미지 **사본**을 반환한다(PIL, lazy).

    A안(줄 전체 검게): 라인의 축정렬 ``bbox``를 ``margin_px``만큼 확장해 채운다 —
    축정렬 사각형이라 기울어진 글자도 빠짐없이 덮고(과마스킹 무방), 5.1 커버리지
    검사에서 bbox 영역이 100%에 가깝게 가려진다. 원본은 변형하지 않는다(사본에 그린다).
    """
    from PIL import ImageDraw

    redacted = image.convert("RGB").copy()
    draw = ImageDraw.Draw(redacted)
    width, height = redacted.size
    for index in line_indices:
        x0, y0, x1, y1 = page.lines[index].bbox
        rect = (
            max(0, int(x0) - margin_px),
            max(0, int(y0) - margin_px),
            min(width, int(x1) + margin_px),
            min(height, int(y1) + margin_px),
        )
        draw.rectangle(rect, fill=_REDACT_FILL)
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
            order = reading_order(page)
            spans = self._detect(reading_order_text(page, order))
            indices = pii_line_indices(page, spans, order, self._labels)
            redacted.append(self._redactor(image, page, indices) if indices else image)
        return redacted
