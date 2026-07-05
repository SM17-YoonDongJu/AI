"""ai_client.py 추론 클라이언트 테스트.

httpx.MockTransport로 OpenAI 호환 응답을 가짜로 주입해(실제 LLM 서버 없이) chat/embed의
파싱·차원 검증·에러 변환을 확인한다.
"""

import httpx
import pytest

from core import ai_client
from core.config import settings


def _install_mock(handler) -> None:  # type: ignore[no-untyped-def]
    """챗·임베딩 공유 클라이언트를 모두 MockTransport 기반으로 교체한다."""
    ai_client._chat_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://test/v1",
    )
    ai_client._embed_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://test/v1",
    )


@pytest.fixture(autouse=True)
async def _reset_client():  # type: ignore[no-untyped-def]
    # Arrange: 각 테스트 후 공유 클라이언트 정리
    yield
    await ai_client.close_client()


async def test_chat_returns_message_content() -> None:
    # Arrange
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/chat/completions")
        return httpx.Response(200, json={"choices": [{"message": {"content": "안녕하세요"}}]})

    _install_mock(handler)

    # Act
    reply = await ai_client.chat([{"role": "user", "content": "hi"}])

    # Assert
    assert reply == "안녕하세요"


async def test_embed_returns_vector_of_contract_dimension() -> None:
    # Arrange
    vector = [0.0] * settings.embedding_dim

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/embeddings")
        return httpx.Response(200, json={"data": [{"embedding": vector}]})

    _install_mock(handler)

    # Act
    result = await ai_client.embed("문서")

    # Assert
    assert len(result) == settings.embedding_dim


async def test_embed_raises_on_dimension_mismatch() -> None:
    # Arrange: 잘못된 차원(3) 반환
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": [{"embedding": [0.1, 0.2, 0.3]}]})

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(ai_client.EmbeddingDimensionError):
        await ai_client.embed("문서")


async def test_chat_wraps_http_error() -> None:
    # Arrange: 5xx 응답
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(ai_client.AiClientError):
        await ai_client.chat([{"role": "user", "content": "hi"}])
