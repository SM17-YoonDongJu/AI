"""S3 원본 → OCR 엔진(PaddleOCR PP-OCRv5) 텍스트/좌표 추출 (이슈 #16).

파이프라인: S3 GetObject(원본) → PDF/이미지를 페이지 이미지로 렌더 → **OCR 엔진**
(PaddleOCR PP-OCRv5 검출 + 한국어 인식, CPU in-process)으로 페이지별 라인을 추출한다. 출력은
이어붙인 한 덩어리가 아니라 **라인 단위 ``OcrLine``(text·bbox·polygon·confidence)** 구조로
보존한다 — 분류·추출(#17)은 텍스트를, 마스킹(#18)은 텍스트 치환과 **이미지 마스킹 좌표**를
같은 구조에서 가져간다(검출 1회, 2트랙). 엔진 경계는 ``OcrEngine`` Protocol 하나다.

설계 메모:
- **무거운 의존은 격리**: paddleocr·paddle·pymupdf·PIL은 배포(ocr extra)에만 설치되므로 전부
  lazy import한다. 엔진·다운로드·렌더를 주입 가능한 경계로 두어 순수 오케스트레이션을
  모델 없이 테스트한다(``OcrProcessor`` 생성자 인자, ``PaddleOcrEngine``의 ``factory``).
- **블로킹 격리**(CODE_CONVENTIONS §7): 렌더·OCR 추론은 동기·CPU 바운드이므로
  ``asyncio.to_thread``로 워커 이벤트 루프를 막지 않는다.
- **엔진 이력**: 2026-11 이전엔 surya-ocr(PyTorch, GPU)였다. 신뢰도 임계값(0.90) 같은 실측
  수치는 surya 시절 것이라 PP-OCRv5에선 PoC로 다시 맞춘다(``ocr_low_confidence_threshold``).
"""

import asyncio
import io
import math
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from core.config import Settings, get_settings
from core.exceptions import OcrError, UnreadableFileError
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

    구현체는 ``PaddleOcrEngine``(PaddleOCR in-process)이다. 테스트는 페이크로 대체한다.
    """

    def recognize(self, images: list[PageImage]) -> list[list[OcrLine]]: ...


# 쪽 방향 분류·왜곡 보정은 쪽 이미지를 돌리거나 펴서 인식하므로 결과 좌표가 **원본 쪽 이미지와
# 어긋난다** — 이미지 마스킹 검은블록·5.1 커버리지 검증·VLM 라인 재판독 crop이 모두 원본 좌표를
# 쓰므로 그 검은블록이 엉뚱한 곳에 그려진다. PaddleOCR 3.x 기본값이 둘 다 True라 여기서 항상
# 끈다(설정으로 열지 않는다). 줄 방향 분류(``use_textline_orientation``)는 줄 crop만 돌려 인식하고
# 좌표는 원본 그대로라 설정으로 둔다(실제 모델로 확인: 뒤집힌 줄의 상자도 원본 위치에 나온다).
_FIXED_PADDLE_OPTIONS: dict[str, object] = {
    "use_doc_orientation_classify": False,
    "use_doc_unwarping": False,
    "device": "cpu",
}


class PaddleOcrEngine:
    """PaddleOCR(PP-OCRv5) in-process 엔진(CPU). 모델은 첫 호출에서 lazy 로드해 재사용한다.

    결과 매핑(PaddleOCR 3.7 ``predict`` → ``OcrLine``, 실제 모델 출력으로 형식을 확인했다):

    - ``rec_texts[i]`` → ``text``(앞뒤 공백 제거). 빈 문자열이 돼도 줄은 유지한다 — surya 때와
      같은 의미다(검출된 줄은 좌표·신뢰도 근거로 남고, PII가 아니니 가려지지 않을 뿐이다).
    - ``rec_polys[i]``(int16 (4,2)) → ``polygon``, 그 min/max → ``bbox``. ``rec_boxes``와 같은
      값이지만(실측 확인) 다각형 하나에서 둘 다 만들어 어긋날 여지를 없앤다.
    - ``rec_scores[i]`` → ``confidence``(줄 인식 확률). surya 신뢰도와 분포가 다르다 — 임계값은
      ``ocr_low_confidence_threshold``로 재보정한다.

    **스레드 안전**: 같은 인스턴스를 문서 OCR과 재OCR 검증(5.2)이 함께 쓰고
    (``OcrProcessor.engine``), 둘 다 ``asyncio.to_thread``에서 부른다. paddle predictor는 동시 호출
    안전을 보장하지 않으므로
    로드와 ``predict``를 락 하나로 직렬화한다(``NerDetector``와 같은 방식). 워커는 문서를 1건씩
    처리하므로 직렬화 비용은 없다.
    """

    def __init__(
        self,
        *,
        det_model: str,
        rec_model: str,
        textline_orientation: bool,
        cpu_threads: int,
        enable_mkldnn: bool = True,
        det_limit_side_len: int = 0,
        factory: Callable[..., Any] | None = None,
    ) -> None:
        """엔진을 구성한다(모델은 아직 로드하지 않는다).

        Args:
            det_model: 검출 모델 이름(PaddleX 공식 모델명, 예: ``PP-OCRv5_server_det``).
            rec_model: 인식 모델 이름(예: ``korean_PP-OCRv5_mobile_rec``).
            textline_orientation: 줄 방향 분류(뒤집힌 줄 보정) 사용 여부.
            cpu_threads: paddle 추론 스레드 수.
            enable_mkldnn: x86 oneDNN 가속 사용 여부(``ocr_enable_mkldnn`` 설정 주석 참고).
            det_limit_side_len: 검출 입력의 긴 변 상한(픽셀). 큰 사진의 검출 메모리를 묶는다
                (``ocr_det_limit_side_len`` 설정 주석 참고). 0이면 PaddleOCR 기본값(긴 변
                4000까지 그대로).
            factory: ``PaddleOCR`` 생성 함수(주입 — 테스트는 가짜 predict 객체를 돌려준다).
                ``None``이면 ``paddleocr.PaddleOCR``.
        """
        det_limit: dict[str, object] = (
            {"text_det_limit_type": "max", "text_det_limit_side_len": det_limit_side_len}
            if det_limit_side_len > 0
            else {}
        )
        self._options: dict[str, object] = {
            "text_detection_model_name": det_model,
            "text_recognition_model_name": rec_model,
            "use_textline_orientation": textline_orientation,
            "cpu_threads": cpu_threads,
            "enable_mkldnn": enable_mkldnn,
            **det_limit,
            **_FIXED_PADDLE_OPTIONS,
        }
        self._factory: Callable[..., Any] = factory or _create_paddle_ocr
        # PaddleOCR 파이프라인(외부 객체) — lazy 로드 전 None. 경계라 Any로 둔다(§3).
        self._ocr: Any = None
        self._lock = threading.Lock()

    @classmethod
    def from_settings(cls, settings: Settings) -> "PaddleOcrEngine":
        """설정(``ocr_*``)으로 엔진을 만든다."""
        return cls(
            det_model=settings.ocr_det_model,
            rec_model=settings.ocr_rec_model,
            textline_orientation=settings.ocr_textline_orientation,
            cpu_threads=settings.ocr_cpu_threads,
            enable_mkldnn=settings.ocr_enable_mkldnn,
            det_limit_side_len=settings.ocr_det_limit_side_len,
        )

    def load(self) -> None:
        """모델을 지금 로드한다(이미지 빌드 시 모델 프리페치용 — 평소엔 첫 ``recognize``가 한다).

        Raises:
            OcrError: 모델 로드 실패(모델 파일 없음 + 내려받을 수 없음 등).
        """
        with self._lock:
            self._ensure_loaded()

    def recognize(self, images: list[PageImage]) -> list[list[OcrLine]]:
        """페이지 이미지들을 한 쪽씩 OCR해 페이지별 ``OcrLine`` 목록을 반환한다.

        Raises:
            OcrError: 모델 로드·추론 실패, 또는 결과 형식 이상(메시지엔 개수만 싣는다).
        """
        with self._lock:
            ocr = self._ensure_loaded()
            return [_recognize_page(ocr, image) for image in images]

    def _ensure_loaded(self) -> Any:
        """PaddleOCR 파이프라인을 1회 만든다 — 반드시 ``self._lock`` 안에서 부른다."""
        if self._ocr is None:
            try:
                self._ocr = self._factory(**self._options)
            except Exception as exc:  # 외부 라이브러리 예외를 도메인 예외로 변환(컨텍스트 체이닝)
                raise OcrError("PaddleOCR 모델 로드 실패") from exc
        return self._ocr


def _create_paddle_ocr(**options: object) -> Any:
    """``paddleocr.PaddleOCR``를 만든다(paddleocr·paddle은 이때 lazy import)."""
    from paddleocr import PaddleOCR

    return PaddleOCR(**options)


def _recognize_page(ocr: Any, image: PageImage) -> list[OcrLine]:
    """한 쪽을 ``predict``하고 라인으로 바꾼다(호출측이 락을 잡고 있다)."""
    try:
        results = ocr.predict(_to_bgr_array(image))
    except Exception as exc:  # 엔진 내부 예외를 도메인 예외로 변환(컨텍스트 체이닝)
        raise OcrError("PaddleOCR 추론 실패") from exc
    if len(results) != 1:
        raise OcrError(f"PaddleOCR 결과 수 이상: 입력 1쪽 → 결과 {len(results)}개")
    return _page_lines(results[0])


def _to_bgr_array(image: PageImage) -> Any:
    """PIL 이미지 → BGR ``uint8`` 배열.

    PaddleOCR 파이프라인은 ndarray 입력을 **BGR**로 보고 그대로 쓴다(paddlex 3.7
    ``pipelines/ocr/pipeline.py``의 ``ReadImage(format="BGR")`` — 배열은 변환 없이 통과). 각 모델
    전처리가 다시 BGR→RGB로 바꾸므로, RGB를 그대로 넘기면 채널이 뒤바뀐 채 추론한다.
    """
    import numpy as np

    rgb = np.asarray(image.convert("RGB"))
    return np.ascontiguousarray(rgb[:, :, ::-1])


def _page_lines(result: Any) -> list[OcrLine]:
    """PaddleOCR 결과 1쪽 → ``OcrLine`` 목록(외부 미지정 데이터 경계)."""
    try:
        texts = result["rec_texts"]
        scores = result["rec_scores"]
        polys = result["rec_polys"]
    except (KeyError, TypeError) as exc:
        raise OcrError("PaddleOCR 결과 형식 이상") from exc
    if not len(texts) == len(scores) == len(polys):
        raise OcrError(
            "PaddleOCR 결과 길이 불일치: "
            f"texts={len(texts)} scores={len(scores)} polys={len(polys)}"
        )
    return [
        _to_ocr_line(text, score, poly)
        for text, score, poly in zip(texts, scores, polys, strict=True)
    ]


def _to_ocr_line(text: str | None, score: float, poly: Any) -> OcrLine:
    """한 줄 → ``OcrLine``(좌표를 불변 float 튜플로, bbox는 다각형의 min/max)."""
    polygon = tuple((float(point[0]), float(point[1])) for point in poly)
    if len(polygon) < 3:
        raise OcrError(f"PaddleOCR 다각형 이상: 꼭짓점 {len(polygon)}개")
    xs = [x for x, _ in polygon]
    ys = [y for _, y in polygon]
    return OcrLine(
        text=(text or "").strip(),
        bbox=(min(xs), min(ys), max(xs), max(ys)),
        polygon=polygon,
        confidence=float(score),
    )


def render_to_images(data: bytes, content_type: str) -> list[PageImage]:
    """원본 바이트를 페이지 이미지(RGB) 목록으로 렌더한다.

    PDF는 ``pymupdf``로 페이지별 래스터화하고, 이미지(JPEG/PNG/TIFF)는 단일 페이지로
    연다. 둘 다 PIL ``Image``로 통일해 OCR 엔진 입력에 맞춘다(lazy import).

    Args:
        data: 원본 파일 바이트.
        content_type: ``application/pdf`` 또는 ``image/*``.

    Returns:
        페이지 이미지 목록(최소 1장).

    Raises:
        UnreadableFileError: PDF 렌더·이미지 디코드 실패(결정적 — 재전달 무의미).
        OcrError: 지원하지 않는 content_type.
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
        # 손상·암호화 PDF 등 — 같은 바이트를 다시 받아도 결과가 같다(재전달 무의미).
        raise UnreadableFileError("PDF 렌더 실패") from exc
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
        # 깨진 바이트·미지원 포맷 등 — 재전달해도 같은 결론이라 첫 시도에서 종결한다.
        raise UnreadableFileError("이미지 디코드 실패") from exc


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
            engine: OCR 엔진. 기본은 설정으로 만든 ``PaddleOcrEngine``(첫 사용 시 모델 lazy 로드).
            fetch: S3 다운로드 함수. 기본은 ``storage.get_object``.
            render: 바이트→이미지 렌더 함수. 기본은 ``render_to_images``.
        """
        self._engine: OcrEngine = engine or PaddleOcrEngine.from_settings(get_settings())
        self._fetch = fetch or get_object
        self._render = render or render_to_images

    @property
    def engine(self) -> OcrEngine:
        """이 프로세서가 쓰는 OCR 엔진.

        이미지 마스킹 검증(#18 5.2 재OCR 잔류 검사)이 **같은 엔진 인스턴스**를 재사용해
        OCR 모델을 두 번 로드하지 않게 하려고 노출한다(모델 로드는 수백 MB·수십 초). 엔진이
        ``predict``를 락으로 직렬화하므로 공유해도 안전하다.
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
    """프로세스 1회 생성되는 기본 ``OcrProcessor``를 반환한다(OCR 모델 재사용)."""
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
