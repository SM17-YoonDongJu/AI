"""vlm_client.py VLM(vision) 클라이언트 테스트.

httpx.MockTransport로 ollama 네이티브 /api/generate 응답을 가짜로 주입해(실제
GPU/ollama 없이) transcribe_table의 파싱·에러 변환을 확인한다. test_ai_client.py와
동일한 패턴(공유 클라이언트를 MockTransport로 교체).
"""

import json

import httpx
import pytest
from PIL import Image

from ocr_worker import vlm_client
from ocr_worker.masking.spans import PiiLabel


def _install_mock(handler) -> None:  # type: ignore[no-untyped-def]
    vlm_client._vlm_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://test"
    )


@pytest.fixture(autouse=True)
async def _reset_client():  # type: ignore[no-untyped-def]
    yield
    await vlm_client.close_client()


def _image() -> Image.Image:
    return Image.new("RGB", (10, 10))


async def test_transcribe_table_returns_response_text() -> None:
    # Arrange
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/api/generate")
        return httpx.Response(200, json={"response": "| 항목 | 금액 |\n|---|---|"})

    _install_mock(handler)

    # Act
    result = await vlm_client.transcribe_table(_image())

    # Assert
    assert result == "| 항목 | 금액 |\n|---|---|"


async def test_transcribe_table_sends_base64_png_image() -> None:
    # Arrange
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        captured["images"] = body["images"]
        captured["model"] = body["model"]
        return httpx.Response(200, json={"response": "ok"})

    _install_mock(handler)

    # Act
    await vlm_client.transcribe_table(_image())

    # Assert: 이미지가 base64 PNG 1장으로 인코딩되어 전송됐다.
    images = captured["images"]
    assert isinstance(images, list)
    assert len(images) == 1
    assert isinstance(images[0], str) and len(images[0]) > 0


async def test_transcribe_table_uses_vlm_model_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    # 실측 확인: 표 전사용 모델이 grounding(좌표)엔 부정확해서 별도 설정으로 분리했다
    # — transcribe_table은 vlm_model만 써야 한다(vlm_grounding_model과 섞이면 안 됨).
    monkeypatch.setattr(vlm_client.settings, "vlm_model", "table-model")
    monkeypatch.setattr(vlm_client.settings, "vlm_grounding_model", "grounding-model")
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["model"] = json.loads(request.content)["model"]
        return httpx.Response(200, json={"response": "ok"})

    _install_mock(handler)

    await vlm_client.transcribe_table(_image())

    assert captured["model"] == "table-model"


async def test_transcribe_table_sets_zero_temperature() -> None:
    # Arrange: 환각(창작) 경향을 낮추기 위해 temperature=0.0을 보내는지 확인.
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        captured["options"] = body.get("options")
        return httpx.Response(200, json={"response": "ok"})

    _install_mock(handler)

    # Act
    await vlm_client.transcribe_table(_image())

    # Assert
    assert captured["options"] == {"temperature": 0.0, "num_ctx": vlm_client.settings.vlm_num_ctx}


async def test_transcribe_table_uses_vlm_num_ctx_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    # 실측 확인: num_ctx를 안 보내면 페이지 이미지 토큰만으로 컨텍스트 대부분이 차
    # 표 전사 응답이 문장 중간에서 잘렸다 — 반드시 명시해서 보내야 한다.
    monkeypatch.setattr(vlm_client.settings, "vlm_num_ctx", 8192)
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["options"] = json.loads(request.content)["options"]
        return httpx.Response(200, json={"response": "ok"})

    _install_mock(handler)

    await vlm_client.transcribe_table(_image())

    assert captured["options"]["num_ctx"] == 8192


async def test_transcribe_table_wraps_http_error() -> None:
    # Arrange: 5xx 응답
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError):
        await vlm_client.transcribe_table(_image())


async def test_transcribe_table_wraps_malformed_response() -> None:
    # Arrange: "response" 키 누락
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError):
        await vlm_client.transcribe_table(_image())


async def test_transcribe_table_wraps_non_json_response() -> None:
    # Arrange — 코드리뷰 지적: resp.json()이 던지는 JSONDecodeError(ValueError 하위)가
    # 그대로 전파되면 VlmClientError로 안 잡혀 pipeline._extract_pages의 surya 폴백을
    # 못 타고 작업 자체가 실패한다(예: ollama 앞단 프록시가 에러 페이지를 200으로 반환).
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>error</html>")

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError):
        await vlm_client.transcribe_table(_image())


async def test_transcribe_table_wraps_connect_error() -> None:
    # Arrange: 타임아웃/연결 실패
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("연결 거부", request=request)

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError):
        await vlm_client.transcribe_table(_image())


# ── ground_pii (이미지 마스킹 보조 grounding) ────────────────────
# _image()는 10x10 픽셀이라 정규화(0~1000) 좌표는 /100으로 픽셀 변환된다.


async def test_ground_pii_parses_response_to_pixel_boxes() -> None:
    # Arrange
    body = json.dumps([{"label": "이름", "bbox": [100, 200, 300, 400]}])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"response": body})

    _install_mock(handler)

    # Act
    boxes = await vlm_client.ground_pii(_image())

    # Assert: 10x10 이미지 기준 정규화 [100,200,300,400] → 픽셀 [1,2,3,4]
    assert boxes == [(PiiLabel.NAME, (1, 2, 3, 4))]


async def test_ground_pii_uses_grounding_model_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    # 실측 확인(GPU E2E): vlm_model(표 전사용)을 grounding에도 쓰면 좌표가 실제 텍스트
    # 위치를 벗어났다 — ground_pii는 반드시 vlm_grounding_model만 써야 한다.
    monkeypatch.setattr(vlm_client.settings, "vlm_model", "table-model")
    monkeypatch.setattr(vlm_client.settings, "vlm_grounding_model", "grounding-model")
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["model"] = json.loads(request.content)["model"]
        return httpx.Response(200, json={"response": "[]"})

    _install_mock(handler)

    await vlm_client.ground_pii(_image())

    assert captured["model"] == "grounding-model"


async def test_ground_pii_uses_vlm_num_ctx_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    # transcribe_table과 동일한 num_ctx 잘림 위험 방지 — grounding 호출에도 명시해야 한다.
    monkeypatch.setattr(vlm_client.settings, "vlm_num_ctx", 8192)
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["options"] = json.loads(request.content)["options"]
        return httpx.Response(200, json={"response": "[]"})

    _install_mock(handler)

    await vlm_client.ground_pii(_image())

    assert captured["options"]["num_ctx"] == 8192


async def test_ground_pii_strips_markdown_code_fence() -> None:
    # Arrange: 모델이 ```json ... ``` 로 감싸 응답하는 경우
    body = "```json\n" + json.dumps([{"label": "주민등록번호", "bbox": [0, 0, 500, 500]}]) + "\n```"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"response": body})

    _install_mock(handler)

    # Act
    boxes = await vlm_client.ground_pii(_image())

    # Assert
    assert boxes == [(PiiLabel.RRN, (0, 0, 5, 5))]


async def test_ground_pii_returns_empty_list_when_no_pii() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"response": "[]"})

    _install_mock(handler)

    boxes = await vlm_client.ground_pii(_image())

    assert boxes == []


async def test_ground_pii_returns_empty_list_on_http_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    _install_mock(handler)

    boxes = await vlm_client.ground_pii(_image())

    assert boxes == []


async def test_ground_pii_returns_empty_list_on_malformed_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    _install_mock(handler)

    boxes = await vlm_client.ground_pii(_image())

    assert boxes == []


async def test_ground_pii_returns_empty_list_on_non_json_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"response": "이 이미지엔 개인정보가 없습니다."})

    _install_mock(handler)

    boxes = await vlm_client.ground_pii(_image())

    assert boxes == []


async def test_ground_pii_skips_items_with_unknown_label() -> None:
    # Arrange: 알려진 PII 분류와 전혀 매칭 안 되는 라벨 — 그 항목만 무시
    body = json.dumps(
        [
            {"label": "알수없는분류", "bbox": [0, 0, 100, 100]},
            {"label": "이름", "bbox": [100, 200, 300, 400]},
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"response": body})

    _install_mock(handler)

    boxes = await vlm_client.ground_pii(_image())

    assert boxes == [(PiiLabel.NAME, (1, 2, 3, 4))]


async def test_ground_pii_matches_document_style_labels() -> None:
    # Arrange — 실측 확인(E2E): 프롬프트가 "이름"만 쓰라고 지시해도 모델이 문서 자체의
    # 필드 캡션("환자 성명", "연락처")을 그대로 라벨로 쓰는 사례가 있었다. 엄격한
    # PiiLabel(raw_label) 매칭이던 시절엔 이런 라벨이 전부 조용히 드롭돼 이름이 하나도
    # 안 가려지는 실제 회귀가 있었다 — 부분 문자열 매칭으로 잡아야 한다.
    body = json.dumps(
        [
            {"label": "환자 성명", "bbox": [100, 200, 300, 400]},
            {"label": "연락처", "bbox": [500, 200, 700, 400]},
            {"label": "환자등록번호", "bbox": [100, 500, 300, 700]},
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"response": body})

    _install_mock(handler)

    boxes = await vlm_client.ground_pii(_image())

    assert boxes == [
        (PiiLabel.NAME, (1, 2, 3, 4)),
        (PiiLabel.PHONE, (5, 2, 7, 4)),
        (PiiLabel.PATIENT_ID, (1, 5, 3, 7)),
    ]


async def test_ground_pii_skips_items_with_invalid_bbox() -> None:
    # Arrange: 좌표 역전(x1>x2)·범위 밖(>1000)·길이 이상 항목은 무시하고 유효한 것만 채택
    body = json.dumps(
        [
            {"label": "이름", "bbox": [300, 200, 100, 400]},  # x1 > x2
            {"label": "전화번호", "bbox": [0, 0, 1500, 100]},  # 범위 밖
            {"label": "주소", "bbox": [0, 0, 100]},  # 길이 3
            {"label": "계좌번호", "bbox": [100, 200, 300, 400]},  # 유효
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"response": body})

    _install_mock(handler)

    boxes = await vlm_client.ground_pii(_image())

    assert boxes == [(PiiLabel.ACCOUNT, (1, 2, 3, 4))]
