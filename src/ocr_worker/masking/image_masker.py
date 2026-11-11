"""이미지 마스킹 트랙 (이슈 #18, 노션 B안) — PII 라인 검은블럭 비식별 사본.

손해사정사 비식별 열람용으로, 원본 페이지 이미지에서 **PII가 포함된 OCR 라인**을
줄 단위 검은블럭으로 가린 사본을 만든다(노션 A안: 줄 단위 추천 — 라벨까지 가려도 무방).

핵심 설계:
- **검출 1회 공유**: 텍스트 마스킹과 같은 ``Masker.detect``로 스팬을 얻어, 텍스트
  치환과 이미지 좌표 마스킹이 한 번의 탐지를 공유한다.
- **좌표만 사용**: 라인의 ``bbox``/``polygon``(PII 아님)으로 가릴 영역을 정한다. 텍스트
  자체는 노출하지 않는다.
- **순수/렌더 분리**: "어느 라인이 PII인가"(``pii_line_indices``)와 "그 라인 어느 구간이
  PII인가"(``line_local_spans``)는 순수 함수라 PIL 없이 테스트하고, PIL 렌더
  (``redact_page_image``)는 lazy import로 격리한다(배포 의존).
- **소비자 프로파일**: ``labels``로 가릴 PII 분류를 제한할 수 있다(예: 고유식별정보만
  가리고 성명은 유지). ``None``이면 모든 PII.
- **VLM 라인 재판독 보강(선택)**: OCR 엔진이 텍스트를 완전히 다른 문자로 오독하면(예: surya
  시절 이름 → "828") 라인 기반 검출로는 그 라인을 PII로 못 잡는다. ``reread``를 주입하면 저신뢰도
  문서에서 OCR 엔진 라인 bbox crop을 VLM이 다시 읽고(``vlm_client.reread_lines``), 텍스트만
  바꾼 같은 페이지에 **같은 페이지 단위 검출**을 한 번 더 돌려 나온 PII 라인을 검은블록에
  **추가**한다(대체 아님). 픽셀 좌표가 아니라 **라인 인덱스로 합치므로** 5.1 커버리지 검증이
  그 라인까지 잰다. 재판독이 실패한 라인은 OCR 엔진 텍스트 그대로라, 실패해도 **OCR 엔진 단독 판정**
  보다 후퇴하지 않는다. (예전 Qwen grounding — VLM이 0~1000 좌표로 PII 위치를 짚던 방식 — 의
  대체다. PaddleOCR-VL은 그런 지시를 따르지 않는다.)
- **알려진 한계(예전 grounding 대비 축소)**: 재판독은 OCR 엔진이 **라인을 만든 곳만** 다시 읽는다.
  예전 grounding은 이미지 전체에서 PII 위치를 짚어, OCR 엔진이 라인 자체를 검출하지 못한 영역
  (흐린 글씨·도장·손글씨 옆 이름 등)도 픽셀 bbox로 가릴 수 있었다. 지금은 (a) OCR 엔진 라인이 없는
  영역과 (b) 단계 예산(``vlm_reread_budget_seconds``)으로 건너뛴 라인의 PII를 재판독이 메우지
  못한다. 그래서 "후퇴 없음"은 OCR 엔진 단독과 비교할 때만 맞고, grounding 대비로는 이 두 경우에
  커버리지가 줄었다. (a)는 5.1·5.2 검증으로도 못 잡는 기존 맹점(``image_verify`` 모듈 설명)과
  같은 자리다. 대안 후보: PaddleOCR-VL ``Spotting:``(좌표 출력)의 형식을 확인한 뒤 평가.
"""

import dataclasses
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

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
# (페이지 이미지들, 같은 문서의 OCR 페이지들) → 페이지별·라인별 재판독 텍스트(실패 라인은 None).
type RereadFn = Callable[
    [Sequence[PageImage], Sequence[OcrPage]], Awaitable[list[list[str | None]]]
]
type RedactFn = Callable[[PageImage, OcrPage, set[int]], PageImage]


@dataclass(frozen=True, slots=True)
class RedactedPage:
    """비식별 처리한 페이지 1장 + 무엇을 가렸는지에 대한 근거(순수 데이터).

    ``line_indices``는 검증(``image_verify.verify_redacted_page``)이 "가렸어야 할
    영역"으로 그대로 받는 값이다. 검증 측이 같은 판정(검출→라인 매핑)을 다시 하지
    않도록(검출 1회 원칙) 렌더 시점의 결과를 그대로 실어 보낸다 — 재검출은 비용도
    비용이지만, 두 번의 판정이 갈리면 "가린 것"과 "검사하는 것"이 어긋난다.

    Attributes:
        image: 검은블럭이 적용된 사본. 가릴 것이 없던 페이지는 **원본 이미지 그대로**다.
        line_indices: 검은블럭 대상으로 판정된 PII 라인 인덱스(원래 ``page.lines`` 기준).
            VLM 라인 재판독으로 찾은 라인도 여기 합쳐진다(5.1이 그 라인도 잰다).
        reread_indices: ``line_indices`` 중 OCR 엔진 텍스트로는 못 찾고 **재판독 텍스트로만**
            PII로 판정된 라인(부분집합) — 재판독이 실제로 무엇을 메웠는지 보는 관측용.
    """

    image: PageImage
    line_indices: set[int]
    reread_indices: frozenset[int] = field(default_factory=frozenset)

    @property
    def redacted(self) -> bool:
        """실제로 가린 영역이 있는가(``False``면 ``image``는 원본 그대로)."""
        return bool(self.line_indices)


def reading_order(page: OcrPage) -> list[int]:
    """bbox(y0, x0) 기준으로 정렬한 라인 인덱스 순서를 반환한다.

    OCR 엔진 검출 순서가 표의 시각적 순서(위→아래, 좌→우)와 다를 수 있어, 앵커 정규식의
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


def line_local_spans(page: OcrPage, spans: list[Span], order: list[int]) -> dict[int, list[Span]]:
    """PII 스팬을 겹치는 라인마다 그 라인의 로컬 오프셋으로 잘라 반환한다.

    ``pii_line_indices``가 "어느 라인이 PII인가"만 답한다면, 이 함수는 "그 라인의
    어느 구간이 PII인가"까지 답한다 — 라인을 다시 통째로 재검출(``mask(line.text)``)하지
    않고 페이지 컨텍스트로 이미 검출한 스팬을 그대로 재사용해 라인 단위로 치환할 수
    있게 한다(라벨과 값이 다른 줄로 쪼개진 경우, 값만 있는 라인은 컨텍스트 없이 재검출하면
    못 잡을 수 있다 — 실측 확인).

    Args:
        page: OCR 페이지(라인 텍스트·좌표 보유).
        spans: ``reading_order_text(page, order)``에서 검출된 PII 스팬.
        order: ``reading_order(page)``가 반환한 정렬 순서(스팬 오프셋 계산과 일치해야 함).

    Returns:
        원래 라인 인덱스 → 그 라인 로컬 좌표로 클리핑된 스팬 목록. 겹치는 스팬이
        없는 라인은 키 자체가 없다(``apply_mask``에 그대로 넘길 수 있는 형태).
    """
    ranges = _line_char_ranges(page, order)
    result: dict[int, list[Span]] = {}
    for span in spans:
        for index, (start, end) in ranges.items():
            if span.start < end and span.end > start:  # 범위 겹침
                local_start = max(span.start, start) - start
                local_end = min(span.end, end) - start
                result.setdefault(index, []).append(
                    Span(local_start, local_end, span.label, span.source, span.score)
                )
    return result


def redact_page_image(
    image: PageImage,
    page: OcrPage,
    line_indices: set[int],
    *,
    margin_px: int = BBOX_MARGIN_PX,
) -> PageImage:
    """지정 라인을 검은블럭으로 가린 페이지 이미지 **사본**을 반환한다(PIL, lazy).

    A안(줄 전체 검게): 라인의 축정렬 ``bbox``를 ``margin_px``만큼 확장해 채운다 —
    축정렬 사각형이라 기울어진 글자도 빠짐없이 덮고(과마스킹 무방), 5.1 커버리지
    검사에서 bbox 영역이 100%에 가깝게 가려진다. 원본은 변형하지 않는다(사본에 그린다).
    VLM 재판독으로 찾은 라인도 같은 OCR 엔진 bbox로 가린다(별도 픽셀 좌표 경로 없음).
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
        reread: RereadFn | None = None,
        reread_confidence_threshold: float = 0.0,
    ) -> None:
        """이미지 마스커를 구성한다.

        Args:
            detect: 텍스트 → 스팬 검출 함수. 기본은 ``Masker().detect``(정규식+USE_NER).
            redactor: 라인 검은블럭 렌더 함수. 기본은 ``redact_page_image``.
            labels: 가릴 PII 분류. ``None``이면 모든 PII(소비자 프로파일 확장점). 재판독으로
                찾은 라인에도 같은 필터가 걸린다.
            reread: (이미지들, OCR 페이지들) → 라인 재판독 텍스트 함수(``vlm_client.reread_lines``).
                ``None``이면 재판독을 아예 시도하지 않는다(비용·지연시간 없음, OCR 엔진 검출만).
            reread_confidence_threshold: ``result.mean_confidence``가 이 값 미만일 때만
                재판독을 시도한다(고신뢰 문서는 OCR 엔진 자체 검출로 충분해 비용 절약). 기본 0.0은
                "항상 시도 안 함"과 같다 — 실제로 켜려면 호출측이 임계값을 넘겨야 한다.
        """
        self._detect: DetectFn = detect or Masker().detect
        self._redactor: RedactFn = redactor or redact_page_image
        self._labels = labels
        self._reread = reread
        self._reread_confidence_threshold = reread_confidence_threshold

    async def reread_pages(
        self, images: Sequence[PageImage], result: OcrResult
    ) -> list[list[str | None]] | None:
        """저신뢰도 문서면 라인 재판독 텍스트를 받아 온다(아니면 ``None`` — 호출하지 않는다).

        OCR 신뢰도가 임계값 이상이거나 ``reread``가 주입 안 됐으면 아무것도 안 한다(고신뢰
        문서는 라인 기반 검출로 충분하다고 본다 — 쪽 전사 게이트와 같은 임계값). VLM I/O라
        async로 두고, 결과는 ``redact_pages``의 ``reread_texts``에 그대로 넘긴다 — 검출(CPU)은
        렌더와 함께 스레드에서 돈다(§7).

        Args:
            images: 원본 페이지 이미지.
            result: 같은 문서의 OCR 결과(게이팅·라인 bbox).

        Returns:
            페이지별·라인별 재판독 텍스트(실패 라인 ``None``). 게이트를 못 넘으면 ``None``.

        Raises:
            ValueError: 이미지 수와 OCR 페이지 수가 다를 때(계약 위반).
        """
        if self._reread is None or result.mean_confidence >= self._reread_confidence_threshold:
            return None
        if len(images) != len(result.pages):
            raise ValueError(
                f"이미지/페이지 수 불일치: images={len(images)} pages={len(result.pages)}"
            )
        return await self._reread(images, result.pages)

    def redact_pages(
        self,
        images: Sequence[PageImage],
        result: OcrResult,
        reread_texts: Sequence[Sequence[str | None]] | None = None,
    ) -> list[RedactedPage]:
        """페이지별로 PII 라인을 가린 결과를 반환한다.

        가릴 게 없는 페이지는 원본 이미지를 그대로 돌려준다(불필요한 사본 생성 회피) —
        이 경우 ``RedactedPage.redacted``가 ``False``다.

        ``reread_texts``가 있으면 OCR 엔진 판정에 재판독 판정을 **합집합으로** 더한다
        (``_reread_pii_lines``). 재판독은 대체가 아니라 보강이다 — OCR 엔진이 이미 잡은 라인은
        재판독 결과와 무관하게 그대로 가린다.

        Args:
            images: 원본 페이지 이미지(``OcrProcessor``가 렌더한 것과 동일 순서·길이).
            result: 같은 문서의 OCR 결과(라인 좌표 보유).
            reread_texts: ``reread_pages``가 돌려준 페이지별·라인별 재판독 텍스트.
                ``None``이면 OCR 엔진 텍스트 기반 검출만 쓴다.

        Returns:
            페이지별 ``RedactedPage``(이미지 + 가린 근거). 검증(5.1 커버리지)이
            ``line_indices``를 필요로 해서 이미지만 돌려주지 않는다.

        Raises:
            ValueError: 이미지 수와 OCR 페이지 수(또는 재판독 페이지·라인 수)가 다를 때(계약 위반).
        """
        if len(images) != len(result.pages):
            raise ValueError(
                f"이미지/페이지 수 불일치: images={len(images)} pages={len(result.pages)}"
            )
        rereads: Sequence[Sequence[str | None] | None] = (
            reread_texts if reread_texts is not None else [None] * len(images)
        )
        if len(rereads) != len(images):
            raise ValueError(
                f"이미지/reread_texts 수 불일치: images={len(images)} reread_texts={len(rereads)}"
            )
        redacted: list[RedactedPage] = []
        for image, page, texts in zip(images, result.pages, rereads, strict=True):
            order = reading_order(page)
            spans = self._detect(reading_order_text(page, order))
            engine_indices = pii_line_indices(page, spans, order, self._labels)
            reread_only = (
                self._reread_pii_lines(page, texts, order) - engine_indices
                if texts is not None
                else set()
            )
            indices = engine_indices | reread_only
            rendered = self._redactor(image, page, indices) if indices else image
            redacted.append(
                RedactedPage(
                    image=rendered, line_indices=indices, reread_indices=frozenset(reread_only)
                )
            )
        return redacted

    def _reread_pii_lines(
        self, page: OcrPage, texts: Sequence[str | None], order: list[int]
    ) -> set[int]:
        """재판독 텍스트로 바꾼 페이지에서 PII 라인을 찾는다(OCR 엔진 판정과 **같은** 로직).

        라인·bbox는 그대로 두고 텍스트만 바꾼다 — 재판독에 실패한 라인(``None``)은 OCR 엔진
        텍스트를 유지한다. bbox가 같으니 리딩오더(``order``)도 같고, 페이지 단위로 검출하므로
        라벨("환자 성명")과 값(오독됐던 이름)이 다른 라인으로 쪼개진 경우도 페이지 컨텍스트로
        잡는다(라인 하나만 재검출하면 놓친다 — ``line_local_spans`` 참고).

        Raises:
            ValueError: 재판독 텍스트 수가 페이지 라인 수와 다를 때(계약 위반 — 값은 싣지 않는다).
        """
        if len(texts) != len(page.lines):
            raise ValueError(
                f"라인/재판독 텍스트 수 불일치: page={page.index} "
                f"lines={len(page.lines)} texts={len(texts)}"
            )
        if all(text is None for text in texts):
            return set()  # 전부 실패 — 보탤 근거가 없다(OCR 엔진 판정만 남는다)
        reread_page = dataclasses.replace(
            page,
            lines=tuple(
                line if text is None else dataclasses.replace(line, text=text)
                for line, text in zip(page.lines, texts, strict=True)
            ),
        )
        spans = self._detect(reading_order_text(reread_page, order))
        return pii_line_indices(reread_page, spans, order, self._labels)
