"""ai_client.py 추론 클라이언트 테스트.

실제 네트워크 호출 없이 검증한다. OpenAI 호환·Voyage 경로는 httpx.MockTransport로 응답을 가짜로
주입하고, Claude 경로는 실제 anthropic SDK 클라이언트에 httpx2.MockTransport(SDK의 HTTP 계층)를
꽂아 SDK가 실제로 내보내는 요청(헤더·본문)과 스트리밍 응답 처리를 함께 확인한다.
"""

import json
from collections.abc import Callable
from typing import Any

import anthropic
import httpx
import httpx2
import pytest

from core import ai_client
from core.config import settings

_CHAT_MESSAGES = [
    {"role": "system", "content": "너는 보험 리포트 작성자다."},
    {"role": "user", "content": "질문 본문"},
]


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


@pytest.fixture(autouse=True)
def _local_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 개발자 셸의 LLM_PROVIDER 등이 새어 들지 않게 기본(로컬) provider로 고정한다.
    monkeypatch.setattr(settings, "llm_provider", "openai_compat")
    monkeypatch.setattr(settings, "embedding_provider", "openai_compat")


class _RecordingLogger:
    """structlog 로거 대역 — (레벨, 이벤트, 필드)를 기록한다."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def info(self, event: str, **fields: Any) -> None:
        self.records.append(("info", event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        self.records.append(("warning", event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.records.append(("error", event, fields))


@pytest.fixture
def log(monkeypatch: pytest.MonkeyPatch) -> _RecordingLogger:
    recorder = _RecordingLogger()
    monkeypatch.setattr(ai_client, "logger", recorder)
    return recorder


# ── OpenAI 호환(기본 경로) — 기존 동작 회귀 ───────────────────────────────────


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


async def test_openai_compat_chat_passes_opts_and_model_through() -> None:
    # Arrange: 로컬 경로는 temperature 등 추가 옵션을 페이로드에 그대로 싣는다(기존 동작).
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    _install_mock(handler)

    # Act
    await ai_client.chat(_CHAT_MESSAGES, model="qwen-test", temperature=0.1)

    # Assert
    assert seen == [
        {"model": "qwen-test", "messages": _CHAT_MESSAGES, "stream": False, "temperature": 0.1}
    ]


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


def _json_reply(content: str):  # type: ignore[no-untyped-def]
    """chat 완성 응답에 임의 content를 실어 주는 MockTransport 핸들러."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

    return handler


async def test_chat_json_parses_fenced_block() -> None:
    _install_mock(_json_reply('```json\n{"a": 1}\n```'))
    assert await ai_client.chat_json([{"role": "user", "content": "hi"}]) == {"a": 1}


async def test_chat_json_parses_plain_json() -> None:
    _install_mock(_json_reply('{"b": 2}'))
    assert await ai_client.chat_json([{"role": "user", "content": "hi"}]) == {"b": 2}


async def test_chat_json_recovers_from_surrounding_noise() -> None:
    _install_mock(_json_reply('앞말 {"c": 3} 뒷말'))
    assert await ai_client.chat_json([{"role": "user", "content": "hi"}]) == {"c": 3}


async def test_chat_json_returns_empty_on_unparseable() -> None:
    _install_mock(_json_reply("이건 JSON이 아니에요"))
    assert await ai_client.chat_json([{"role": "user", "content": "hi"}]) == {}


# ── 인증키 분리 — 챗 키가 임베딩 요청에 실리면 안 된다 ────────────────────────


def test_embed_request_carries_only_embedding_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 운영처럼 챗 키(Claude)와 임베딩 키가 둘 다 설정된 상태
    monkeypatch.setattr(settings, "ai_api_key", "sk-ant-chat-secret")
    monkeypatch.setattr(settings, "embedding_api_key", "voyage-embed-secret")

    # Act: 실제 공유 클라이언트가 만드는 요청(기본 헤더 병합 포함)을 전송 없이 조립
    request = ai_client._get_embed_client().build_request("POST", "/embeddings", json={})

    # Assert
    assert request.headers["Authorization"] == "Bearer voyage-embed-secret"
    assert "sk-ant-chat-secret" not in str(request.headers.raw)


def test_embed_request_has_no_auth_when_embedding_key_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 기존 버그 경로 — 임베딩 키는 기본값인데 챗 키만 있음
    monkeypatch.setattr(settings, "ai_api_key", "sk-ant-chat-secret")
    monkeypatch.setattr(settings, "embedding_api_key", "not-needed")

    # Act
    request = ai_client._get_embed_client().build_request("POST", "/embeddings", json={})

    # Assert: 챗 키로 대체하지 않고 인증 헤더 자체를 붙이지 않는다
    assert "Authorization" not in request.headers


def test_chat_request_carries_only_chat_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setattr(settings, "ai_api_key", "chat-key")
    monkeypatch.setattr(settings, "embedding_api_key", "embed-key")

    # Act
    request = ai_client._get_chat_client().build_request("POST", "/chat/completions", json={})

    # Assert
    assert request.headers["Authorization"] == "Bearer chat-key"


# ── 임베딩 — voyage 페이로드·정렬·배치 ─────────────────────────────────────────


def _vector(seed: float) -> list[float]:
    """계약 차원의 식별 가능한 벡터(첫 원소로 구분)."""
    return [seed] + [0.0] * (settings.embedding_dim - 1)


def _embedding_reply(seen: list[dict[str, Any]]):  # type: ignore[no-untyped-def]
    """요청 본문을 기록하고 입력마다 벡터(첫 원소=입력 위치)를 index 역순으로 돌려준다."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body)
        inputs = body["input"] if isinstance(body["input"], list) else [body["input"]]
        data = [{"embedding": _vector(float(i)), "index": i} for i in range(len(inputs))]
        return httpx.Response(200, json={"object": "list", "data": data[::-1]})

    return handler


@pytest.fixture
def voyage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "embedding_provider", "voyage")
    monkeypatch.setattr(settings, "embedding_model", "voyage-4-large")


@pytest.mark.usefixtures("voyage")
async def test_voyage_embed_batch_sends_input_type_and_output_dimension() -> None:
    # Arrange
    seen: list[dict[str, Any]] = []
    _install_mock(_embedding_reply(seen))

    # Act
    await ai_client.embed_batch(["조항 A", "조항 B"])

    # Assert: 문서 적재 기본 input_type=document, 계약 차원을 명시
    assert seen == [
        {
            "model": "voyage-4-large",
            "input": ["조항 A", "조항 B"],
            "input_type": "document",
            "output_dimension": settings.embedding_dim,
        }
    ]


@pytest.mark.usefixtures("voyage")
async def test_voyage_embed_defaults_to_query_input_type() -> None:
    # Arrange
    seen: list[dict[str, Any]] = []
    _install_mock(_embedding_reply(seen))

    # Act
    await ai_client.embed("보장 범위가 어떻게 되나요")

    # Assert: 단건도 voyage는 list input
    assert seen[0]["input"] == ["보장 범위가 어떻게 되나요"]
    assert seen[0]["input_type"] == "query"


@pytest.mark.usefixtures("voyage")
async def test_embed_batch_orders_vectors_by_response_index() -> None:
    # Arrange: 핸들러는 data를 index 역순으로 돌려준다
    _install_mock(_embedding_reply([]))

    # Act
    vectors = await ai_client.embed_batch(["a", "b", "c"])

    # Assert: 입력 순서(index 0, 1, 2)로 복원
    assert [vector[0] for vector in vectors] == [0.0, 1.0, 2.0]


@pytest.mark.usefixtures("voyage")
async def test_embed_batch_splits_into_requests_of_32() -> None:
    # Arrange
    seen: list[dict[str, Any]] = []
    _install_mock(_embedding_reply(seen))
    texts = [f"청크 {i}" for i in range(33)]

    # Act
    vectors = await ai_client.embed_batch(texts)

    # Assert: 32 + 1 두 번 순차 호출, 결과는 입력 개수·순서대로
    assert [body["input"] for body in seen] == [texts[:32], texts[32:]]
    assert len(vectors) == 33
    assert vectors[31][0] == 31.0
    assert vectors[32][0] == 0.0


async def test_embed_batch_empty_input_makes_no_request() -> None:
    # Arrange
    seen: list[dict[str, Any]] = []
    _install_mock(_embedding_reply(seen))

    # Act
    vectors = await ai_client.embed_batch([])

    # Assert
    assert vectors == []
    assert seen == []


@pytest.mark.usefixtures("voyage")
async def test_voyage_embed_batch_checks_every_vector_dimension() -> None:
    # Arrange: 두 번째 벡터만 차원이 틀림
    def handler(request: httpx.Request) -> httpx.Response:
        data = [
            {"embedding": _vector(0.0), "index": 0},
            {"embedding": [0.1, 0.2], "index": 1},
        ]
        return httpx.Response(200, json={"data": data})

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(ai_client.EmbeddingDimensionError):
        await ai_client.embed_batch(["a", "b"])


@pytest.mark.usefixtures("voyage")
async def test_embed_batch_rejects_vector_count_mismatch() -> None:
    # Arrange: 2건 요청에 1건만 응답
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": [{"embedding": _vector(0.0), "index": 0}]})

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(ai_client.AiClientError, match="개수 불일치"):
        await ai_client.embed_batch(["a", "b"])


async def test_openai_compat_embed_payload_is_unchanged() -> None:
    # Arrange: 로컬 경로는 model·input만(단건은 기존처럼 문자열). input_type은 무시.
    seen: list[dict[str, Any]] = []
    _install_mock(_embedding_reply(seen))

    # Act
    await ai_client.embed("쿼리", input_type="document")
    await ai_client.embed_batch(["a", "b"])

    # Assert
    assert seen == [
        {"model": settings.embedding_model, "input": "쿼리"},
        {"model": settings.embedding_model, "input": ["a", "b"]},
    ]


# ── Claude(anthropic) 경로 ────────────────────────────────────────────────────


@pytest.fixture
def claude(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "llm_provider", "anthropic")
    monkeypatch.setattr(settings, "ai_api_key", "sk-ant-test")
    monkeypatch.setattr(settings, "llm_model", "claude-opus-5-5")
    monkeypatch.setattr(settings, "llm_effort", "high")
    monkeypatch.setattr(settings, "llm_max_tokens", 64000)


def _install_anthropic(handler: Callable[[httpx2.Request], httpx2.Response]) -> None:
    """Claude 공유 클라이언트를 httpx2.MockTransport를 꽂은 실제 SDK 클라이언트로 교체한다."""
    ai_client._anthropic_client = anthropic.AsyncAnthropic(
        api_key="sk-ant-test",
        base_url="https://api.anthropic.com",
        max_retries=0,
        http_client=anthropic.DefaultAsyncHttpxClient(transport=httpx2.MockTransport(handler)),
    )


def _sse(
    *,
    model: str = "claude-opus-5-5",
    blocks: list[dict[str, Any]],
    stop_reason: str = "end_turn",
    stop_details: dict[str, Any] | None = None,
) -> bytes:
    """Messages 스트리밍(SSE) 응답 본문. text 블록은 text_delta 1개로, 그 외는 시작 블록 그대로."""
    message = {
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 12, "output_tokens": 1},
    }
    events: list[dict[str, Any]] = [{"type": "message_start", "message": message}]
    for index, block in enumerate(blocks):
        if block["type"] == "text":
            start = {"type": "text", "text": ""}
            delta = {"type": "text_delta", "text": block["text"]}
            events.append({"type": "content_block_start", "index": index, "content_block": start})
            events.append({"type": "content_block_delta", "index": index, "delta": delta})
        else:
            events.append({"type": "content_block_start", "index": index, "content_block": block})
        events.append({"type": "content_block_stop", "index": index})
    final = {"stop_reason": stop_reason, "stop_sequence": None, "stop_details": stop_details}
    events.append({"type": "message_delta", "delta": final, "usage": {"output_tokens": 7}})
    events.append({"type": "message_stop"})
    return "".join(
        f"event: {event['type']}\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
        for event in events
    ).encode()


def _stream_reply(
    body: bytes, seen: list[httpx2.Request] | None = None
) -> Callable[[httpx2.Request], httpx2.Response]:
    """요청을 기록하고 고정 SSE 본문으로 응답하는 핸들러."""

    def handler(request: httpx2.Request) -> httpx2.Response:
        if seen is not None:
            seen.append(request)
        return httpx2.Response(200, headers={"content-type": "text/event-stream"}, content=body)

    return handler


_THINKING_BLOCK = {"type": "thinking", "thinking": "", "signature": "sig"}


@pytest.mark.usefixtures("claude")
async def test_anthropic_chat_request_shape() -> None:
    # Arrange
    seen: list[httpx2.Request] = []
    _install_anthropic(_stream_reply(_sse(blocks=[{"type": "text", "text": "답"}]), seen))

    # Act
    await ai_client.chat(_CHAT_MESSAGES)

    # Assert: SDK가 실제로 내보낸 요청
    request = seen[0]
    body = json.loads(request.content)
    assert request.url.path == "/v1/messages"
    assert "server-side-fallback-2026-07-01" in request.headers["anthropic-beta"]
    assert body["model"] == "claude-opus-5-5"
    assert body["system"] == "너는 보험 리포트 작성자다."
    assert body["messages"] == [{"role": "user", "content": "질문 본문"}]
    assert body["fallbacks"] == "default"
    assert body["output_config"] == {"effort": "high"}
    assert body["max_tokens"] == 64000
    assert body["stream"] is True
    assert "thinking" not in body
    assert {"temperature", "top_p", "top_k"}.isdisjoint(body)


@pytest.mark.usefixtures("claude")
async def test_anthropic_chat_model_opt_overrides_default() -> None:
    # Arrange
    seen: list[httpx2.Request] = []
    _install_anthropic(_stream_reply(_sse(blocks=[{"type": "text", "text": "답"}]), seen))

    # Act
    await ai_client.chat(_CHAT_MESSAGES, model="claude-sonnet-5-5")

    # Assert
    assert json.loads(seen[0].content)["model"] == "claude-sonnet-5-5"


@pytest.mark.usefixtures("claude")
async def test_anthropic_chat_returns_only_text_blocks() -> None:
    # Arrange: thinking(빈 문자열) + text 2개
    blocks = [_THINKING_BLOCK, {"type": "text", "text": "첫째 "}, {"type": "text", "text": "둘째"}]
    _install_anthropic(_stream_reply(_sse(blocks=blocks)))

    # Act
    reply = await ai_client.chat(_CHAT_MESSAGES)

    # Assert
    assert reply == "첫째 둘째"


@pytest.mark.usefixtures("claude")
async def test_anthropic_chat_joins_text_across_fallback_and_logs_models_only(
    log: _RecordingLogger,
) -> None:
    # Arrange: 스트림 중간 거절 → 폴백 모델이 이어 씀(경계에 fallback 블록)
    fallback_block = {
        "type": "fallback",
        "from": {"model": "claude-opus-5-5"},
        "to": {"model": "claude-opus-5"},
        "trigger": {"type": "refusal", "category": "cyber"},
    }
    blocks = [
        {"type": "text", "text": "앞부분 "},
        fallback_block,
        {"type": "text", "text": "뒷부분"},
    ]
    _install_anthropic(_stream_reply(_sse(blocks=blocks)))

    # Act
    reply = await ai_client.chat(_CHAT_MESSAGES)

    # Assert: 텍스트는 이어 붙이고, 로그엔 모델명만(원문 없음)
    assert reply == "앞부분 뒷부분"
    assert log.records == [
        (
            "info",
            "ai_client.chat.fallback_served",
            {"requested_model": "claude-opus-5-5", "served_model": "claude-opus-5"},
        )
    ]


@pytest.mark.usefixtures("claude")
async def test_anthropic_refusal_raises_without_content(log: _RecordingLogger) -> None:
    # Arrange: 출력 전 거절(폴백 모델까지 거절)
    details = {"type": "refusal", "category": "bio", "explanation": "설명 원문"}
    body = _sse(blocks=[], stop_reason="refusal", stop_details=details)
    _install_anthropic(_stream_reply(body))

    # Act
    with pytest.raises(ai_client.LlmRefusalError) as caught:
        await ai_client.chat(_CHAT_MESSAGES)

    # Assert: 예외·로그에 프롬프트·설명 원문이 없고 카테고리만 남는다
    assert "질문 본문" not in str(caught.value)
    assert log.records == [
        ("warning", "ai_client.chat.refused", {"model": "claude-opus-5-5", "category": "bio"})
    ]


@pytest.mark.usefixtures("claude")
@pytest.mark.parametrize("stop_reason", ["max_tokens", "model_context_window_exceeded"])
async def test_anthropic_truncated_reply_is_not_returned_as_result(
    stop_reason: str, log: _RecordingLogger
) -> None:
    # Arrange: 출력 상한·컨텍스트 한도에서 잘린 응답
    body = _sse(blocks=[{"type": "text", "text": "잘린 응답 조각"}], stop_reason=stop_reason)
    _install_anthropic(_stream_reply(body))

    # Act
    with pytest.raises(ai_client.AiClientError) as caught:
        await ai_client.chat(_CHAT_MESSAGES)

    # Assert: 거절과 구분되고, 잘린 텍스트는 예외·로그 어디에도 없다
    assert not isinstance(caught.value, ai_client.LlmRefusalError)
    assert "잘린 응답 조각" not in str(caught.value)
    assert "잘린 응답 조각" not in repr(log.records)
    assert log.records[0][2]["stop_reason"] == stop_reason


@pytest.mark.usefixtures("claude")
async def test_anthropic_stream_without_stop_reason_is_not_returned_as_result() -> None:
    # Arrange: message_delta(stop_reason) 전에 정상 종료된 스트림
    full = _sse(blocks=[{"type": "text", "text": "중간까지만 온 응답"}])
    body = full[: full.index(b"event: message_delta")]
    _install_anthropic(_stream_reply(body))

    # Act / Assert: 부분 텍스트를 완성 응답으로 돌려주지 않는다
    with pytest.raises(ai_client.AiClientError, match="stop_reason=None"):
        await ai_client.chat(_CHAT_MESSAGES)


@pytest.mark.usefixtures("claude")
async def test_anthropic_chat_rejects_extra_opts_before_calling() -> None:
    # Arrange
    seen: list[httpx2.Request] = []
    _install_anthropic(_stream_reply(_sse(blocks=[{"type": "text", "text": "답"}]), seen))

    # Act / Assert: 샘플링 파라미터는 Opus 5.5에서 400 — 조용히 버리지 않는다
    with pytest.raises(TypeError, match="temperature"):
        await ai_client.chat(_CHAT_MESSAGES, temperature=0.2)
    assert seen == []


@pytest.mark.usefixtures("claude")
async def test_anthropic_status_error_is_wrapped(log: _RecordingLogger) -> None:
    # Arrange: 429 rate limit(재시도 0회 클라이언트)
    def handler(request: httpx2.Request) -> httpx2.Response:
        error = {"type": "error", "error": {"type": "rate_limit_error", "message": "slow down"}}
        return httpx2.Response(429, json=error)

    _install_anthropic(handler)

    # Act
    with pytest.raises(ai_client.AiClientError) as caught:
        await ai_client.chat(_CHAT_MESSAGES)

    # Assert: SDK 예외는 원인으로 보존, 로그엔 분류값만
    assert isinstance(caught.value.__cause__, anthropic.RateLimitError)
    level, event, fields = log.records[0]
    assert (level, event) == ("warning", "ai_client.chat.api_error")
    assert fields["error_type"] == "RateLimitError"
    assert fields["status_code"] == 429
    assert "slow down" not in repr(log.records)  # API 오류 메시지 원문은 남기지 않는다


@pytest.mark.usefixtures("claude")
async def test_anthropic_connection_error_is_wrapped() -> None:
    # Arrange
    def handler(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("connection refused")

    _install_anthropic(handler)

    # Act / Assert
    with pytest.raises(ai_client.AiClientError):
        await ai_client.chat(_CHAT_MESSAGES)


class _DroppedStream(httpx2.AsyncByteStream):
    """앞부분 SSE를 흘린 뒤 연결이 끊기는 응답 본문(스트림 도중 끊김 재현)."""

    def __init__(self, head: bytes) -> None:
        self._head = head

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        yield self._head
        raise httpx2.RemoteProtocolError("peer closed connection")


@pytest.mark.usefixtures("claude")
async def test_anthropic_mid_stream_disconnect_is_wrapped() -> None:
    # Arrange: SDK는 스트림 도중 끊김을 자체 예외로 감싸지 않고 httpx2 예외로 올린다
    head = _sse(blocks=[{"type": "text", "text": "부분"}]).split(b"event: message_delta")[0]

    def handler(request: httpx2.Request) -> httpx2.Response:
        headers = {"content-type": "text/event-stream"}
        return httpx2.Response(200, headers=headers, stream=_DroppedStream(head))

    _install_anthropic(handler)

    # Act
    with pytest.raises(ai_client.AiClientError) as caught:
        await ai_client.chat(_CHAT_MESSAGES)

    # Assert
    assert isinstance(caught.value.__cause__, httpx2.RemoteProtocolError)


@pytest.mark.usefixtures("claude")
async def test_anthropic_requires_real_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 키가 로컬 자리표시값 그대로
    monkeypatch.setattr(settings, "ai_api_key", "not-needed")

    # Act / Assert
    with pytest.raises(ai_client.AiClientError, match="AI_API_KEY"):
        await ai_client.chat(_CHAT_MESSAGES)


@pytest.mark.usefixtures("claude")
async def test_anthropic_requires_model(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setattr(settings, "llm_model", "")

    # Act / Assert
    with pytest.raises(ai_client.AiClientError, match="LLM_MODEL"):
        await ai_client.chat(_CHAT_MESSAGES)


@pytest.mark.usefixtures("claude")
async def test_anthropic_client_uses_chat_key_and_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setattr(settings, "llm_timeout_seconds", 123.0)

    # Act
    client = ai_client._get_anthropic_client()

    # Assert
    assert client.api_key == "sk-ant-test"
    assert client.timeout == 123.0


@pytest.mark.usefixtures("claude")
async def test_close_client_closes_anthropic_client() -> None:
    # Arrange
    client = ai_client._get_anthropic_client()

    # Act
    await ai_client.close_client()

    # Assert
    assert client.is_closed()
    assert ai_client._anthropic_client is None


@pytest.mark.usefixtures("claude")
async def test_anthropic_chat_json_parses_claude_reply() -> None:
    # Arrange: 호출부 대부분이 쓰는 chat_json 경로
    body = _sse(
        blocks=[_THINKING_BLOCK, {"type": "text", "text": '```json\n{"failures": []}\n```'}]
    )
    _install_anthropic(_stream_reply(body))

    # Act
    verdict = await ai_client.chat_json(_CHAT_MESSAGES)

    # Assert
    assert verdict == {"failures": []}


def test_to_anthropic_messages_merges_system_and_keeps_turn_order() -> None:
    # Arrange
    messages = [
        {"role": "system", "content": "규칙 1"},
        {"role": "user", "content": "질문"},
        {"role": "assistant", "content": "답변"},
        {"role": "system", "content": "규칙 2"},
        {"role": "user", "content": "추가 질문"},
    ]

    # Act
    system, turns = ai_client._to_anthropic_messages(messages)

    # Assert
    assert system == "규칙 1\n\n규칙 2"
    assert turns == [
        {"role": "user", "content": "질문"},
        {"role": "assistant", "content": "답변"},
        {"role": "user", "content": "추가 질문"},
    ]


def test_to_anthropic_messages_rejects_unknown_role() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="tool"):
        ai_client._to_anthropic_messages([{"role": "tool", "content": "x"}])
