"""S3 원본 → surya-ocr 텍스트/좌표 추출 (이슈 #16).

파이프라인: S3 GetObject(원본) → PDF/이미지를 페이지 이미지로 렌더 → **surya-ocr**
(전용 OCR 엔진, PyTorch, GPU)로 페이지별 라인을 추출한다. 출력은 이어붙인 한 덩어리가
아니라 **라인 단위 ``OcrLine``(text·bbox·polygon·confidence)** 구조로 보존한다 —
분류·추출(#17)은 텍스트를, 마스킹(#18)은 텍스트 치환과 **이미지 마스킹 좌표**를
같은 구조에서 가져간다(검출 1회, 2트랙).

설계 메모:
- **무거운 의존은 격리**: surya·pymupdf·PIL은 배포(ocr extra)에만 설치되므로 전부
  lazy import한다. 엔진·다운로드·렌더를 주입 가능한 경계로 두어 순수 오케스트레이션을
  GPU 없이 테스트한다(``OcrProcessor`` 생성자 인자).
- **블로킹 격리**(CODE_CONVENTIONS §7): 렌더·OCR 추론은 동기·CPU/GPU 바운드이므로
  ``asyncio.to_thread``로 워커 이벤트 루프를 막지 않는다.
- **서빙 버전**: surya 0.17.x의 in-process API(``FoundationPredictor`` 주입)를 쓴다.
  0.18+는 별도 추론 서버가 필요하므로 in-process 세대로 고정한다(pyproject 핀).
"""

import asyncio
import io
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from core.exceptions import OcrError
from core.logging import get_logger
from ocr_worker.storage import get_object

logger = get_logger(__name__)

# 외부 라이브러리(PIL.Image) 객체 — lazy 의존이라 경계에서 Any로 둔다(§3 최후수단).
type PageImage = Any

# PDF 렌더 해상도. 증권·진단서는 표·작은 글씨가 많아 200dpi 기준(필요 시 상향).
PDF_RENDER_DPI = 200
_PDF_CONTENT_TYPE = "application/pdf"
_IMAGE_CONTENT_PREFIX = "image/"


@dataclass(frozen=True, slots=True)
class OcrLine:
    """OCR이 검출한 텍스트 라인 1건(순수 데이터).

    Attributes:
        text: 라인 텍스트(평문 — 마스킹 이전).
        bbox: 축 정렬 박스 ``(x0, y0, x1, y1)``.
        polygon: 4꼭짓점 ``((x, y), …)``(시계방향). 회전/기울기 보존.
        confidence: 라인 신뢰도 0~1.
    """

    text: str
    bbox: tuple[float, float, float, float]
    polygon: tuple[tuple[float, float], ...]
    confidence: float


@dataclass(frozen=True, slots=True)
class OcrPage:
    """페이지 1장의 OCR 결과. ``width``·``height``는 이미지 마스킹 렌더 범위에 쓴다."""

    index: int
    width: int
    height: int
    lines: tuple[OcrLine, ...]

    @property
    def text(self) -> str:
        """라인을 줄바꿈으로 이어붙인 페이지 텍스트."""
        return "\n".join(line.text for line in self.lines)


@dataclass(frozen=True, slots=True)
class OcrResult:
    """문서 전체 OCR 결과(페이지 묶음) + 다운스트림 편의 접근자."""

    pages: tuple[OcrPage, ...]

    @property
    def first_page_text(self) -> str:
        """첫 페이지 텍스트(분류·추출은 첫 페이지 기준 — #17)."""
        return self.pages[0].text if self.pages else ""

    @property
    def full_text(self) -> str:
        """전체 페이지 텍스트(마스킹 대상)."""
        return "\n".join(page.text for page in self.pages)

    @property
    def mean_confidence(self) -> float:
        """전체 라인 평균 신뢰도(0~1). 라인이 없으면 0.0 — QA 게이팅용."""
        confidences = [line.confidence for page in self.pages for line in page.lines]
        return sum(confidences) / len(confidences) if confidences else 0.0


@runtime_checkable
class OcrEngine(Protocol):
    """OCR 엔진 경계 — 페이지 이미지 목록 → 페이지별 라인 목록.

    구현체는 surya를 in-process 로드한다. 테스트는 페이크로 대체한다.
    """

    def recognize(self, images: list[PageImage]) -> list[list[OcrLine]]: ...


class SuryaEngine:
    """surya-ocr(0.17.x) in-process 엔진. 모델은 첫 호출에서 lazy 로드해 재사용한다."""

    def __init__(self) -> None:
        # surya 예측기(외부 객체) — lazy 로드 전 None. 경계라 Any로 둔다.
        self._recognition: Any = None
        self._detection: Any = None

    def _ensure_loaded(self) -> None:
        """surya 예측기를 lazy 로드한다(torch/surya는 이때 import — GPU 자동 사용)."""
        if self._recognition is not None:
            return
        from surya.detection import DetectionPredictor
        from surya.foundation import FoundationPredictor
        from surya.recognition import RecognitionPredictor

        self._recognition = RecognitionPredictor(FoundationPredictor())
        self._detection = DetectionPredictor()

    def recognize(self, images: list[PageImage]) -> list[list[OcrLine]]:
        """페이지 이미지들을 OCR해 페이지별 ``OcrLine`` 목록을 반환한다.

        Raises:
            OcrError: surya 추론 실패.
        """
        self._ensure_loaded()
        try:
            predictions = self._recognition(images, det_predictor=self._detection)
        except Exception as exc:  # 엔진 내부 예외를 도메인 예외로 변환(컨텍스트 체이닝)
            raise OcrError("surya OCR 추론 실패") from exc
        return [[_to_ocr_line(line) for line in page.text_lines] for page in predictions]


def _to_ocr_line(line: Any) -> OcrLine:
    """surya ``TextLine`` → 내부 ``OcrLine``(좌표를 불변 튜플로 정규화)."""
    bbox = tuple(float(value) for value in line.bbox)
    polygon = tuple((float(point[0]), float(point[1])) for point in line.polygon)
    return OcrLine(
        text=(line.text or "").strip(),
        bbox=bbox,  # type: ignore[arg-type]  # surya bbox는 길이 4 보장
        polygon=polygon,
        confidence=float(line.confidence) if line.confidence is not None else 0.0,
    )


def render_to_images(data: bytes, content_type: str) -> list[PageImage]:
    """원본 바이트를 페이지 이미지(RGB) 목록으로 렌더한다.

    PDF는 ``pymupdf``로 페이지별 래스터화하고, 이미지(JPEG/PNG/TIFF)는 단일 페이지로
    연다. 둘 다 PIL ``Image``로 통일해 surya 입력에 맞춘다(lazy import).

    Args:
        data: 원본 파일 바이트.
        content_type: ``application/pdf`` 또는 ``image/*``.

    Returns:
        페이지 이미지 목록(최소 1장).

    Raises:
        OcrError: 지원하지 않는 content_type 또는 렌더 실패.
    """
    if content_type == _PDF_CONTENT_TYPE:
        return _render_pdf(data)
    if content_type.startswith(_IMAGE_CONTENT_PREFIX):
        return _render_image(data)
    raise OcrError(f"지원하지 않는 content_type: {content_type}")


def _render_pdf(data: bytes) -> list[PageImage]:
    """PDF 바이트를 페이지별 PIL 이미지로 렌더한다(pymupdf).

    페이지 물리 크기가 큰 스캔본(예: A3 이상)은 ``PDF_RENDER_DPI``로 그대로
    렌더하면 PIL의 압축폭탄 방지 한도(``Image.MAX_IMAGE_PIXELS``)를 넘어
    디코드가 거부된다. 한도를 해제하는 대신 페이지별로 한도 안에 들어오도록
    DPI를 낮춰 렌더한다.
    """
    import fitz  # lazy: pymupdf
    from PIL import Image

    default_zoom = PDF_RENDER_DPI / 72
    images: list[PageImage] = []
    try:
        with fitz.open(stream=data, filetype="pdf") as document:
            for page in document:
                zoom = _safe_zoom(page.rect.width, page.rect.height, default_zoom)
                matrix = fitz.Matrix(zoom, zoom)
                pixmap = page.get_pixmap(matrix=matrix)
                images.append(Image.open(io.BytesIO(pixmap.tobytes("png"))).convert("RGB"))
    except Exception as exc:  # 렌더 실패를 도메인 예외로 변환(컨텍스트 체이닝)
        raise OcrError("PDF 렌더 실패") from exc
    if not images:
        raise OcrError("PDF에 페이지가 없습니다.")
    return images


def _safe_zoom(width_pt: float, height_pt: float, zoom: float) -> float:
    """``zoom``으로 렌더 시 픽셀 수가 PIL 압축폭탄 한도를 넘으면 한도 안으로 줄인다."""
    from PIL import Image

    max_pixels = Image.MAX_IMAGE_PIXELS
    if not max_pixels:
        return zoom
    pixels = (width_pt * zoom) * (height_pt * zoom)
    if pixels <= max_pixels:
        return zoom
    safe_zoom = zoom * math.sqrt(max_pixels / pixels)
    logger.warning(
        "pdf_page_downscaled",
        requested_dpi=round(zoom * 72),
        safe_dpi=round(safe_zoom * 72),
        width_pt=round(width_pt),
        height_pt=round(height_pt),
    )
    return safe_zoom


def _render_image(data: bytes) -> list[PageImage]:
    """이미지 바이트를 단일 PIL 이미지로 연다(JPEG/PNG/TIFF)."""
    from PIL import Image

    try:
        return [Image.open(io.BytesIO(data)).convert("RGB")]
    except Exception as exc:  # 디코드 실패를 도메인 예외로 변환(컨텍스트 체이닝)
        raise OcrError("이미지 디코드 실패") from exc


class OcrProcessor:
    """S3 다운로드 → 렌더 → OCR 엔진을 잇는 오케스트레이터.

    경계(다운로드·렌더·엔진)를 주입 가능하게 두어, GPU·네트워크 없이 페이크로
    오케스트레이션을 테스트한다. 블로킹 작업(렌더·추론)은 스레드로 격리한다.
    """

    def __init__(
        self,
        *,
        engine: OcrEngine | None = None,
        fetch: Callable[[str], Awaitable[bytes]] | None = None,
        render: Callable[[bytes, str], list[PageImage]] | None = None,
    ) -> None:
        """프로세서를 구성한다.

        Args:
            engine: OCR 엔진. 기본은 ``SuryaEngine``(첫 사용 시 모델 lazy 로드).
            fetch: S3 다운로드 함수. 기본은 ``storage.get_object``.
            render: 바이트→이미지 렌더 함수. 기본은 ``render_to_images``.
        """
        self._engine: OcrEngine = engine or SuryaEngine()
        self._fetch = fetch or get_object
        self._render = render or render_to_images

    @property
    def engine(self) -> OcrEngine:
        """이 프로세서가 쓰는 OCR 엔진.

        이미지 마스킹 검증(#18 5.2 재OCR 잔류 검사)이 **같은 엔진 인스턴스**를 재사용해
        surya 모델을 두 번 로드하지 않게 하려고 노출한다(모델 로드는 수 GB·수십 초).
        """
        return self._engine

    async def process(self, s3_key: str, content_type: str) -> OcrResult:
        """원본을 OCR해 라인 구조를 보존한 ``OcrResult``를 반환한다.

        Args:
            s3_key: 원본 파일 S3 키.
            content_type: ``OcrJob.content_type``(application/pdf | image/*).

        Returns:
            페이지별 라인을 담은 ``OcrResult``.

        Raises:
            OcrError: 다운로드·렌더·OCR 중 실패.
        """
        result, _images = await self.process_with_images(s3_key, content_type)
        return result

    async def process_with_images(
        self, s3_key: str, content_type: str
    ) -> tuple[OcrResult, list[PageImage]]:
        """OCR 결과와 함께 **렌더한 페이지 이미지**를 반환한다(#15 파이프라인용).

        이미지 마스킹 트랙(#18)은 같은 문서를 다시 내려받아 재렌더하지 않고 이 이미지를
        재사용해 검은블럭 사본을 만든다(S3 GetObject·PDF 래스터화 중복 회피). ``process``는
        이 메서드에 위임하고 이미지를 버린다(분류·추출·텍스트 마스킹만 필요한 경로).

        Args:
            s3_key: 원본 파일 S3 키.
            content_type: ``OcrJob.content_type``(application/pdf | image/*).

        Returns:
            ``(OcrResult, 페이지 이미지 목록)`` — 순서·길이는 ``result.pages``와 일치한다.

        Raises:
            OcrError: 다운로드·렌더·OCR 중 실패.
        """
        data = await self._fetch(s3_key)
        images = await asyncio.to_thread(self._render, data, content_type)
        lines_per_page = await asyncio.to_thread(self._engine.recognize, images)
        result = _build_result(images, lines_per_page)
        logger.info(
            "ocr_done",
            pages=len(result.pages),
            lines=sum(len(page.lines) for page in result.pages),
            mean_confidence=round(result.mean_confidence, 3),
        )
        return result, images


def _build_result(images: list[PageImage], lines_per_page: list[list[OcrLine]]) -> OcrResult:
    """이미지 크기와 페이지별 라인을 묶어 ``OcrResult``를 만든다.

    Raises:
        OcrError: 엔진이 페이지 수와 다른 길이의 결과를 반환한 경우(계약 위반).
    """
    if len(images) != len(lines_per_page):
        raise OcrError(
            f"OCR 결과 페이지 수 불일치: images={len(images)} lines={len(lines_per_page)}"
        )
    pages = tuple(
        OcrPage(
            index=index,
            width=int(image.width),
            height=int(image.height),
            lines=tuple(lines),
        )
        for index, (image, lines) in enumerate(zip(images, lines_per_page, strict=True))
    )
    return OcrResult(pages=pages)


# ── 모듈 전역 기본 프로세서 + 편의 함수 ──────────────────────────
_default_processor: OcrProcessor | None = None


def get_processor() -> OcrProcessor:
    """프로세스 1회 생성되는 기본 ``OcrProcessor``를 반환한다(surya 모델 재사용)."""
    global _default_processor
    if _default_processor is None:
        _default_processor = OcrProcessor()
    return _default_processor


async def run_ocr(s3_key: str, content_type: str) -> OcrResult:
    """기본 프로세서로 OCR을 수행한다(파이프라인 #15 진입점).

    Args:
        s3_key: 원본 파일 S3 키.
        content_type: ``OcrJob.content_type``.

    Returns:
        페이지별 라인을 담은 ``OcrResult``.
    """
    return await get_processor().process(s3_key, content_type)
