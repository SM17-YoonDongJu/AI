"""OCR 오케스트레이션·엔진 어댑터 단위 테스트 (이슈 #16).

paddleocr·paddle·pymupdf·boto3는 배포 전용이라 CI에 없다. 따라서 엔진·다운로드·렌더 경계를
**페이크로 주입**해 ``OcrProcessor``의 순수 오케스트레이션(라인 구조 보존·페이지 크기
매핑·평균 신뢰도·계약 위반 검출)과 content_type 라우팅 가드를 검증한다.

``PaddleOcrEngine``은 ``PaddleOCR`` 생성 함수(``factory``)를 주입해 가짜 ``predict`` 결과로
본다. 결과 형식(``rec_texts``·``rec_scores``·``rec_polys`` int16 (4,2)·``rec_boxes``)은 실제
PaddleOCR 3.7.0 + paddlepaddle 3.3.1로 한국어 합성 이미지를 돌려 확인한 모양을 그대로 흉내낸다
(``_workspace/20``). 지키는 계약: 쪽 방향 분류·왜곡 보정은 늘 끔(켜면 좌표가 원본 쪽과 어긋나
검은블록이 엉뚱한 곳에 그려진다), 입력은 BGR, 좌표·신뢰도 매핑, 모델 1회 로드, ``predict`` 직렬화.
"""

import threading
import time
from typing import Any

import numpy as np
import pytest
from PIL import Image
from structlog.testing import capture_logs

from core.config import Settings
from core.exceptions import OcrError
from ocr_worker import ocr as ocr_module
from ocr_worker.ocr import (
    OcrLine,
    OcrPage,
    OcrProcessor,
    OcrResult,
    PaddleOcrEngine,
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


async def test_process_emits_stage_timing_for_fetch_render_and_engine() -> None:
    # Arrange: 2쪽 문서
    images = [_FakeImage(800, 600), _FakeImage(800, 600)]
    pages = [[_line("p0", 1.0)], [_line("p1a", 0.9), _line("p1b", 0.7)]]
    processor, _ = _make_processor(images=images, pages=pages)

    # Act
    with capture_logs() as logs:
        await processor.process("uploads/x.pdf", "application/pdf")

    # Assert: 문서 처리 시간 분해의 앞 세 단계 — 크기·쪽·라인 수만 싣는다(원문 없음)
    stages = {e["stage"]: e for e in logs if e["event"] == "ocr.stage.finished"}
    assert list(stages) == ["s3_fetch", "render", "ocr_engine"]
    assert stages["s3_fetch"]["bytes"] == len(b"rawbytes")
    assert stages["render"]["pages"] == 2
    assert (stages["ocr_engine"]["pages"], stages["ocr_engine"]["lines"]) == (2, 3)
    assert all(isinstance(e["duration_ms"], int) for e in stages.values())


# ── content_type 라우팅 가드 ─────────────────────────────────────
def test_render_rejects_unsupported_content_type() -> None:
    # 가드는 PIL/pymupdf import 이전에 동작 → 로컬에서 검증 가능.
    with pytest.raises(OcrError, match="지원하지 않는 content_type"):
        render_to_images(b"data", "application/zip")


# ── PaddleOcrEngine(가짜 predict) ────────────────────────────────
def _poly(*points: tuple[int, int]) -> Any:
    """PaddleOCR ``rec_polys`` 원소 모양(int16 (4,2) ndarray)."""
    return np.array(points, dtype=np.int16)


def _paddle_result(texts: list[str], scores: list[float], polys: list[Any]) -> dict[str, Any]:
    """PaddleOCR 3.7 ``predict``가 쪽마다 돌려주는 결과(필요한 키만, 실측 모양)."""
    boxes = np.array(
        [[p[:, 0].min(), p[:, 1].min(), p[:, 0].max(), p[:, 1].max()] for p in polys],
        dtype=np.int16,
    )
    return {"rec_texts": texts, "rec_scores": scores, "rec_polys": polys, "rec_boxes": boxes}


class _FakePaddleOcr:
    """``PaddleOCR`` 대역 — 쪽마다 정해진 결과를 돌려주고 받은 입력을 기록한다."""

    def __init__(self, results: list[list[dict[str, Any]]] | None = None) -> None:
        self._results = list(results or [])
        self.inputs: list[Any] = []

    def predict(self, array: Any) -> list[dict[str, Any]]:
        self.inputs.append(array)
        return self._results.pop(0) if self._results else [_paddle_result([], [], [])]


class _RecordingFactory:
    """``PaddleOCR`` 생성 함수 대역 — 생성 인자와 호출 횟수를 기록한다."""

    def __init__(self, ocr: Any) -> None:
        self._ocr = ocr
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **options: Any) -> Any:
        self.calls.append(options)
        return self._ocr


def _engine(ocr: Any, **overrides: Any) -> tuple[PaddleOcrEngine, _RecordingFactory]:
    factory = _RecordingFactory(ocr)
    kwargs: dict[str, Any] = {
        "det_model": "PP-OCRv5_server_det",
        "rec_model": "korean_PP-OCRv5_mobile_rec",
        "textline_orientation": True,
        "cpu_threads": 2,
    }
    kwargs.update(overrides)
    return PaddleOcrEngine(**kwargs, factory=factory), factory


def _page_image(width: int = 40, height: int = 20) -> Image.Image:
    return Image.new("RGB", (width, height), "white")


def test_paddle_engine_always_disables_doc_orientation_and_unwarping() -> None:
    # Arrange: PaddleOCR 3.x 기본값은 둘 다 True다 — 켜지면 쪽을 돌리거나 펴서 인식해 좌표가 원본
    # 쪽과 어긋나고, 이미지 마스킹 검은블록·5.1 커버리지·라인 재판독 crop이 엉뚱한 곳을 본다.
    engine, factory = _engine(_FakePaddleOcr(), textline_orientation=False, cpu_threads=3)

    # Act
    engine.recognize([_page_image()])

    # Assert
    assert factory.calls == [
        {
            "text_detection_model_name": "PP-OCRv5_server_det",
            "text_recognition_model_name": "korean_PP-OCRv5_mobile_rec",
            "use_textline_orientation": False,
            "cpu_threads": 3,
            "enable_mkldnn": True,
            "use_doc_orientation_classify": False,
            "use_doc_unwarping": False,
            "device": "cpu",
        }
    ]


def test_paddle_engine_from_settings_uses_ocr_settings() -> None:
    # Arrange: PoC 손잡이(env)가 엔진 생성 인자로 그대로 들어가야 한다.
    settings = Settings(
        _env_file=None,
        ocr_det_model="PP-OCRv5_mobile_det",
        ocr_rec_model="korean_PP-OCRv5_mobile_rec",
        ocr_textline_orientation=False,
        ocr_cpu_threads=4,
        ocr_enable_mkldnn=False,
        ocr_det_limit_side_len=1800,
    )
    factory = _RecordingFactory(_FakePaddleOcr())

    # Act
    engine = PaddleOcrEngine.from_settings(settings)
    engine._factory = factory  # 생성 함수만 가짜로(설정 매핑을 본다)
    engine.load()

    # Assert
    options = factory.calls[0]
    assert options["text_detection_model_name"] == "PP-OCRv5_mobile_det"
    assert options["text_recognition_model_name"] == "korean_PP-OCRv5_mobile_rec"
    assert options["use_textline_orientation"] is False
    assert options["cpu_threads"] == 4
    assert options["enable_mkldnn"] is False
    assert options["text_det_limit_type"] == "max"
    assert options["text_det_limit_side_len"] == 1800
    # M1: oneDNN 캐시 기본값 2(PaddleOCR 기본 10보다 작게 — 문서별 최고 RSS 5% 감소)
    assert options["mkldnn_cache_capacity"] == 2


def test_paddle_engine_caps_detection_input_long_side() -> None:
    # Arrange: 큰 폰 사진은 기본값(긴 변 4000까지 그대로)으로 검출하면 OOM이 난다
    # — 긴 변 상한(max)을 건다.
    engine, factory = _engine(_FakePaddleOcr(), det_limit_side_len=2400)

    # Act
    engine.recognize([_page_image()])

    # Assert
    options = factory.calls[0]
    assert options["text_det_limit_type"] == "max"
    assert options["text_det_limit_side_len"] == 2400


def test_paddle_engine_leaves_paddle_default_when_detection_limit_is_zero() -> None:
    # Arrange: 0은 "상한 없음" — PaddleOCR 기본값을 쓴다(키를 넘기지 않는다).
    engine, factory = _engine(_FakePaddleOcr(), det_limit_side_len=0)

    # Act
    engine.recognize([_page_image()])

    # Assert
    options = factory.calls[0]
    assert "text_det_limit_type" not in options
    assert "text_det_limit_side_len" not in options


def test_ocr_processor_default_engine_is_paddle_built_from_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 기본 프로세서(엔진 미주입)가 설정으로 PaddleOcrEngine을 만든다.
    factory = _RecordingFactory(_FakePaddleOcr())
    monkeypatch.setattr(ocr_module, "_create_paddle_ocr", factory)
    monkeypatch.setattr(
        ocr_module, "get_settings", lambda: Settings(_env_file=None, ocr_cpu_threads=5)
    )

    # Act
    processor = OcrProcessor()
    assert isinstance(processor.engine, PaddleOcrEngine)
    processor.engine.load()

    # Assert
    assert factory.calls[0]["cpu_threads"] == 5
    assert factory.calls[0]["use_doc_unwarping"] is False


def test_paddle_engine_maps_result_to_ocr_lines() -> None:
    # Arrange: 실측과 같은 모양. 두 번째 줄은 기울어진 사각형이라 bbox가 다각형의 min/max여야 한다
    # (꼭짓점 하나를 bbox로 쓰거나 x·y를 바꾸면 검은블록 좌표가 틀린다).
    skewed = _poly((36, 121), (309, 124), (309, 171), (35, 167))
    result = _paddle_result(
        ["  보험금 청구서 ", "환자 성명 홍길동"],
        [0.9685, 0.9731],
        [_poly((36, 26), (263, 26), (263, 75), (36, 75)), skewed],
    )
    engine, _ = _engine(_FakePaddleOcr([[result]]))

    # Act
    pages = engine.recognize([_page_image()])

    # Assert
    first, second = pages[0]
    assert first == OcrLine(
        text="보험금 청구서",
        bbox=(36.0, 26.0, 263.0, 75.0),
        polygon=((36.0, 26.0), (263.0, 26.0), (263.0, 75.0), (36.0, 75.0)),
        confidence=0.9685,
    )
    assert second.text == "환자 성명 홍길동"
    assert second.bbox == (35.0, 121.0, 309.0, 171.0)
    assert second.polygon == ((36.0, 121.0), (309.0, 124.0), (309.0, 171.0), (35.0, 167.0))
    assert second.confidence == pytest.approx(0.9731)
    assert all(isinstance(value, float) for value in second.bbox)


def test_paddle_engine_feeds_contiguous_bgr_uint8_array() -> None:
    # Arrange: PaddleOCR 파이프라인은 ndarray를 BGR로 본다(paddlex ReadImage(format="BGR")).
    image = Image.new("RGB", (3, 2), (255, 0, 0))  # 빨강(RGB)
    fake = _FakePaddleOcr()
    engine, _ = _engine(fake)

    # Act
    engine.recognize([image])

    # Assert
    array = fake.inputs[0]
    assert array.shape == (2, 3, 3)
    assert array.dtype == np.uint8
    assert array.flags["C_CONTIGUOUS"]
    assert array[0, 0].tolist() == [0, 0, 255]  # B, G, R


def test_paddle_engine_converts_non_rgb_images_before_bgr() -> None:
    # Arrange: 흑백(L)·RGBA 원본도 3채널 BGR로 넘어가야 한다.
    fake = _FakePaddleOcr()
    engine, _ = _engine(fake)

    # Act
    engine.recognize([Image.new("L", (2, 2), 128), Image.new("RGBA", (2, 2), (0, 255, 0, 10))])

    # Assert
    assert [array.shape for array in fake.inputs] == [(2, 2, 3), (2, 2, 3)]
    assert fake.inputs[1][0, 0].tolist() == [0, 255, 0]


def test_paddle_engine_keeps_lines_whose_text_is_empty() -> None:
    # Arrange: 검출됐지만 글자가 비어 있는 줄도 줄로 남긴다(surya 때와 같은 의미 — 좌표·신뢰도
    # 근거로 남는다).
    result = _paddle_result(
        ["   ", "x"],
        [0.1, 0.9],
        [_poly((0, 0), (5, 0), (5, 5), (0, 5)), _poly((0, 9), (5, 9), (5, 14), (0, 14))],
    )
    engine, _ = _engine(_FakePaddleOcr([[result]]))

    # Act
    lines = engine.recognize([_page_image()])[0]

    # Assert
    assert [line.text for line in lines] == ["", "x"]
    assert lines[0].confidence == pytest.approx(0.1)


def test_paddle_engine_blank_page_has_no_lines() -> None:
    # Arrange: 실측 — 빈 쪽은 rec_texts·rec_scores·rec_polys가 빈 리스트, rec_boxes는 shape (0,).
    blank = {
        "rec_texts": [],
        "rec_scores": [],
        "rec_polys": [],
        "rec_boxes": np.zeros((0,), np.int16),
    }
    engine, _ = _engine(_FakePaddleOcr([[blank]]))

    # Act / Assert
    assert engine.recognize([_page_image()]) == [[]]


def test_paddle_engine_predicts_one_page_at_a_time_in_order() -> None:
    # Arrange
    first = _paddle_result(["1쪽"], [0.9], [_poly((0, 0), (5, 0), (5, 5), (0, 5))])
    second = _paddle_result(["2쪽"], [0.8], [_poly((0, 0), (5, 0), (5, 5), (0, 5))])
    fake = _FakePaddleOcr([[first], [second]])
    engine, _ = _engine(fake)

    # Act
    pages = engine.recognize([_page_image(10, 10), _page_image(20, 10)])

    # Assert
    assert [[line.text for line in page] for page in pages] == [["1쪽"], ["2쪽"]]
    assert [array.shape for array in fake.inputs] == [(10, 10, 3), (10, 20, 3)]


def test_paddle_engine_loads_model_once() -> None:
    # Arrange
    engine, factory = _engine(_FakePaddleOcr())

    # Act
    engine.recognize([_page_image()])
    engine.recognize([_page_image()])

    # Assert
    assert len(factory.calls) == 1


class _OverlapProbe:
    """동시에 ``predict`` 안에 있는 호출 수의 최댓값을 잰다(직렬화 확인용)."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self.in_flight = 0
        self.peak = 0

    def predict(self, array: Any) -> list[dict[str, Any]]:
        with self._guard:
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
        time.sleep(0.05)  # 추론 중 — 락이 없으면 다른 스레드가 여기로 들어온다
        with self._guard:
            self.in_flight -= 1
        return [_paddle_result([], [], [])]


def test_paddle_engine_serializes_predict_across_threads() -> None:
    # Arrange: 문서 OCR과 재OCR 검증(5.2)이 같은 인스턴스를 to_thread로 부른다 — paddle predictor는
    # 동시 호출 안전을 보장하지 않으므로 predict가 겹치면 안 된다.
    probe = _OverlapProbe()
    engine, factory = _engine(probe)
    start = threading.Barrier(3)

    def worker() -> None:
        start.wait()
        engine.recognize([_page_image()])

    threads = [threading.Thread(target=worker) for _ in range(3)]

    # Act
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    # Assert
    assert probe.peak == 1
    assert len(factory.calls) == 1  # 동시 첫 호출에도 모델은 한 번만 만든다


def test_paddle_engine_wraps_predict_failure_without_content() -> None:
    # Arrange
    class Exploding:
        def predict(self, array: Any) -> list[dict[str, Any]]:
            raise RuntimeError("홍길동 901010-1234567")

    engine, _ = _engine(Exploding())

    # Act / Assert: 도메인 예외로 바뀌고, 메시지엔 원문이 없다(원인은 체인에만).
    with pytest.raises(OcrError, match="PaddleOCR 추론 실패") as exc_info:
        engine.recognize([_page_image()])
    assert "홍길동" not in str(exc_info.value)


def test_paddle_engine_wraps_model_load_failure() -> None:
    # Arrange: 모델 파일이 없고 받을 수도 없는 경우 등.
    def broken_factory(**_options: Any) -> Any:
        raise RuntimeError("No available model hosting platforms detected.")

    engine = PaddleOcrEngine(
        det_model="x",
        rec_model="y",
        textline_orientation=True,
        cpu_threads=1,
        factory=broken_factory,
    )

    # Act / Assert
    with pytest.raises(OcrError, match="모델 로드 실패"):
        engine.recognize([_page_image()])


def test_paddle_engine_rejects_mismatched_result_lengths() -> None:
    # Arrange
    broken = {
        "rec_texts": ["a", "b"],
        "rec_scores": [0.9],
        "rec_polys": [_poly((0, 0), (1, 0), (1, 1), (0, 1))] * 2,
    }
    engine, _ = _engine(_FakePaddleOcr([[broken]]))

    # Act / Assert
    with pytest.raises(OcrError, match="길이 불일치"):
        engine.recognize([_page_image()])


def test_paddle_engine_rejects_unexpected_result_count() -> None:
    # Arrange: 쪽 하나를 넣었는데 결과가 두 개 — 쪽 순서가 어긋난다.
    empty = _paddle_result([], [], [])
    engine, _ = _engine(_FakePaddleOcr([[empty, empty]]))

    # Act / Assert
    with pytest.raises(OcrError, match="결과 수 이상"):
        engine.recognize([_page_image()])


def test_paddle_engine_rejects_malformed_result() -> None:
    # Arrange
    engine, _ = _engine(_FakePaddleOcr([[{"unexpected": "shape"}]]))

    # Act / Assert
    with pytest.raises(OcrError, match="결과 형식 이상"):
        engine.recognize([_page_image()])


def test_paddle_engine_passes_mkldnn_cache_capacity_only_when_set() -> None:
    # Arrange: M1 손잡이 — 설정하면 생성 인자로 넘기고, 비우면 PaddleOCR 기본값(10)에 맡긴다
    engine, factory = _engine(_FakePaddleOcr(), mkldnn_cache_capacity=2)
    default_engine, default_factory = _engine(_FakePaddleOcr())

    # Act
    engine.recognize([_page_image()])
    default_engine.recognize([_page_image()])

    # Assert
    assert factory.calls[0]["mkldnn_cache_capacity"] == 2
    assert "mkldnn_cache_capacity" not in default_factory.calls[0]


def test_paddle_engine_from_settings_can_override_det_model() -> None:
    # Arrange: 재OCR 검증 전용 엔진(O3b)은 검출 모델만 바꾸고 나머지 설정은 메인과 같다
    settings = Settings(_env_file=None, ocr_det_model="PP-OCRv5_server_det", ocr_cpu_threads=3)
    factory = _RecordingFactory(_FakePaddleOcr())

    # Act
    engine = PaddleOcrEngine.from_settings(settings, det_model="PP-OCRv5_mobile_det")
    engine._factory = factory
    engine.load()

    # Assert
    options = factory.calls[0]
    assert options["text_detection_model_name"] == "PP-OCRv5_mobile_det"
    assert options["cpu_threads"] == 3
