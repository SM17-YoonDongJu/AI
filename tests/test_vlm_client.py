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
    assert captured["options"] == {"temperature": 0.0}


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


async def test_transcribe_table_wraps_connect_error() -> None:
    # Arrange: 타임아웃/연결 실패
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("연결 거부", request=request)

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError):
        await vlm_client.transcribe_table(_image())
