"""추론 클라이언트 — 챗(OpenAI 호환 | Claude)·임베딩(OpenAI 호환 | Voyage).

provider는 env(`LLM_PROVIDER`·`EMBEDDING_PROVIDER`)로 고르고, 기본값은 기존 로컬 경로(Ollama 등
OpenAI 호환 httpx)다 — env를 바꾸지 않으면 코드 머지만으로 런타임 동작이 바뀌지 않는다.
base_url·모델명·인증키는 config에서 주입한다(하드코딩 금지). 챗과 임베딩은 서로 다른 노드·
업체일 수 있어 엔드포인트와 **인증키를 분리**한다 — 챗 키(운영에선 Claude API 키)가 임베딩
업체로 새면 안 된다. 모든 호출은 async-first다(httpx.AsyncClient·anthropic.AsyncAnthropic).

`anthropic` 패키지는 report extra에만 있다(OCR·corpus 이미지엔 미설치). Claude 경로를 처음 쓸 때
지연 import하고, 타입은 `TYPE_CHECKING`으로만 가져온다.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal

import httpx

from core.config import settings
from core.logging import cap_sdk_body_loggers, get_logger

if TYPE_CHECKING:
    from anthropic import AsyncAnthropic
    from anthropic.types.beta import BetaMessage, BetaMessageParam

type EmbedInputType = Literal["query", "document"]

logger = get_logger(__name__)

# 인증키 자리표시값. 키가 이 값이거나 비어 있으면 인증 헤더를 붙이지 않는다(무인증 로컬 서버).
_NO_API_KEY = "not-needed"
# 임베딩 요청 1회당 최대 입력 수. Voyage 요청당 토큰 한도(voyage-4-large 120K)에 여유를 둔다.
_EMBED_BATCH_SIZE = 32
# Claude 서버 측 거절 폴백 beta. fallbacks="default"(거절 카테고리별 권장 모델로 같은 호출 안에서
# 재실행) 전용 헤더다 — 배열 형식(fallbacks=[...])은 다른 헤더라 섞어 보내면 400이다.
_SERVER_SIDE_FALLBACK_BETA = "server-side-fallback-2026-07-01"
# 완결된 응답으로 인정하는 stop_reason. 그 밖의 값 — 상한에서 잘림(max_tokens·
# model_context_window_exceeded), 스트림이 stop_reason 없이 끝남(None) 등 — 은 부분 텍스트를
# 정상 결과로 돌려주지 않는다.
_COMPLETE_STOP_REASONS = frozenset({"end_turn", "stop_sequence"})

_chat_client: httpx.AsyncClient | None = None
_embed_client: httpx.AsyncClient | None = None
_anthropic_client: AsyncAnthropic | None = None


class AiClientError(RuntimeError):
    """추론 호출 실패의 기반 예외."""


class EmbeddingDimensionError(AiClientError):
    """임베딩 차원이 계약값(embedding_dim)과 다를 때 발생."""


class LlmRefusalError(AiClientError):
    """LLM이 응답을 거절했을 때 발생(stop_reason=refusal — 서버 측 폴백 모델까지 거절)."""


def _bearer_headers(api_key: str) -> dict[str, str]:
    """Bearer 인증 헤더. 키가 비었거나 `not-needed`(로컬)면 헤더를 붙이지 않는다."""
    if api_key and api_key != _NO_API_KEY:
        return {"Authorization": f"Bearer {api_key}"}
    return {}


def _get_chat_client() -> httpx.AsyncClient:
    """OpenAI 호환 챗용 공유 AsyncClient(지연 생성). 챗 키(`ai_api_key`)만 싣는다."""
    global _chat_client
    if _chat_client is None:
        _chat_client = httpx.AsyncClient(
            base_url=settings.ai_base_url,
            timeout=settings.ai_timeout_seconds,
            headers=_bearer_headers(settings.ai_api_key),
        )
    return _chat_client


def _get_embed_client() -> httpx.AsyncClient:
    """임베딩용 공유 AsyncClient(지연 생성). 챗과 다른 엔드포인트일 수 있다.

    임베딩 전용 키(`embedding_api_key`)만 싣는다 — 챗 키까지 실으면 운영 Claude 키가 임베딩
    업체로 전송된다.
    """
    global _embed_client
    if _embed_client is None:
        _embed_client = httpx.AsyncClient(
            base_url=settings.embedding_base_url,
            timeout=settings.ai_timeout_seconds,
            headers=_bearer_headers(settings.embedding_api_key),
        )
    return _embed_client


def _get_anthropic_client() -> AsyncAnthropic:
    """Claude 호출용 공유 AsyncAnthropic(지연 생성).

    Raises:
        AiClientError: 챗 키(`ai_api_key`)가 비었거나 `not-needed`인 경우.
    """
    global _anthropic_client
    if _anthropic_client is None:
        if not settings.ai_api_key or settings.ai_api_key == _NO_API_KEY:
            logger.error("ai_client.chat.misconfigured", provider="anthropic", missing="AI_API_KEY")
            raise AiClientError("llm_provider=anthropic에는 AI_API_KEY(Claude API 키)가 필요하다")
        import anthropic  # report extra 전용 의존성 — Claude 경로를 쓸 때만 불러온다

        # SDK가 import 시 ANTHROPIC_LOG로 로거를 DEBUG로 되돌릴 수 있다 — 본문 로깅을 다시 막는다.
        cap_sdk_body_loggers()

        # 키를 명시하면 SDK가 env(ANTHROPIC_*)·디스크 프로필에서 다른 자격증명을 찾지 않는다.
        _anthropic_client = anthropic.AsyncAnthropic(
            api_key=settings.ai_api_key, timeout=settings.llm_timeout_seconds
        )
    return _anthropic_client


async def close_client() -> None:
    """공유 클라이언트(httpx 챗·임베딩, Claude SDK)를 모두 종료한다(앱 종료 시 1회)."""
    global _chat_client, _embed_client, _anthropic_client
    http_clients = (_chat_client, _embed_client)
    anthropic_client = _anthropic_client
    _chat_client = _embed_client = _anthropic_client = None
    for client in http_clients:
        if client is not None:
            await client.aclose()
    if anthropic_client is not None:
        await anthropic_client.close()


def _to_anthropic_messages(
    messages: list[dict[str, str]],
) -> tuple[str, list[BetaMessageParam]]:
    """OpenAI 형식 메시지를 Claude 형식(top-level system + user/assistant 턴)으로 바꾼다.

    Claude는 messages 첫 항목에 system role을 받지 않는다. 그래서 system 메시지는 순서대로
    합쳐 top-level `system`으로 올리고 나머지 턴은 순서를 그대로 둔다.

    Args:
        messages: OpenAI 형식 메시지 목록(`{"role", "content"}`).

    Returns:
        (합친 system 프롬프트 — 없으면 빈 문자열, user/assistant 턴 목록).

    Raises:
        ValueError: system·user·assistant 외의 role이 있는 경우.
    """
    system_parts: list[str] = []
    turns: list[BetaMessageParam] = []
    for message in messages:
        role, content = message["role"], message["content"]
        # BetaMessageParam은 role별 TypedDict union이라 role을 리터럴로 고정해 만든다.
        if role == "system":
            system_parts.append(content)
        elif role == "user":
            turns.append({"role": "user", "content": content})
        elif role == "assistant":
            turns.append({"role": "assistant", "content": content})
        else:
            raise ValueError(f"anthropic chat이 지원하지 않는 role: {role!r}")
    return "\n\n".join(part for part in system_parts if part), turns


def _final_text(message: BetaMessage, *, requested_model: str) -> str:
    """Claude 최종 메시지에서 응답 텍스트만 꺼낸다. 거절·잘림은 예외로 올린다.

    Args:
        message: 스트림이 끝난 뒤의 최종 메시지.
        requested_model: 요청한 모델 ID(폴백 발생 여부 판단용).

    Returns:
        text 블록을 순서대로 이은 응답 텍스트.

    Raises:
        LlmRefusalError: stop_reason이 refusal인 경우.
        AiClientError: 응답이 완결되지 않은 경우(max_tokens·컨텍스트 초과로 잘림,
            stop_reason 없이 끝난 스트림 등).
    """
    if message.stop_reason == "refusal":
        # 폴백 모델까지 거절했다. stop_details는 정보용(없을 수 있음)이라 분기엔 쓰지 않고
        # 카테고리만 남긴다 — explanation·부분 출력은 원문 조각일 수 있어 남기지 않는다.
        category = message.stop_details.category if message.stop_details else None
        logger.warning("ai_client.chat.refused", model=message.model, category=category)
        raise LlmRefusalError("chat 응답이 거절됨(stop_reason=refusal)")
    if message.stop_reason not in _COMPLETE_STOP_REASONS:
        logger.warning(
            "ai_client.chat.truncated",
            model=message.model,
            stop_reason=message.stop_reason,
            output_tokens=message.usage.output_tokens,
        )
        raise AiClientError(f"chat 응답이 완결되지 않음(stop_reason={message.stop_reason})")
    if message.model != requested_model:
        # 서버 측 폴백 모델이 이어받아 응답했다. 원문 없이 모델명만 남긴다.
        logger.info(
            "ai_client.chat.fallback_served",
            requested_model=requested_model,
            served_model=message.model,
        )
    # thinking(기본 display=omitted라 빈 문자열)·fallback 경계 블록은 응답 본문이 아니다. 스트림
    # 중간 거절 뒤 폴백 모델이 이어 쓴 경우엔 경계 앞뒤 text가 한 답이라 순서대로 잇는다.
    return "".join(block.text for block in message.content if block.type == "text")


async def _openai_compat_chat(messages: list[dict[str, str]], opts: dict[str, Any]) -> str:
    """OpenAI 호환 `/chat/completions`를 비스트리밍으로 1회 호출한다."""
    model = opts.pop("model", None) or settings.llm_model
    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "stream": False,
        **opts,
    }
    client = _get_chat_client()
    try:
        resp = await client.post("/chat/completions", json=payload)
        resp.raise_for_status()
        data = resp.json()
        content: str = data["choices"][0]["message"]["content"]
    except httpx.HTTPError as exc:
        raise AiClientError("chat 호출 실패") from exc
    except (KeyError, IndexError, TypeError) as exc:
        raise AiClientError("chat 응답 형식이 올바르지 않음") from exc
    return content


async def _anthropic_chat(messages: list[dict[str, str]], opts: dict[str, Any]) -> str:
    """Claude Messages API(공식 SDK)를 스트리밍으로 1회 호출해 최종 응답 텍스트를 돌려준다."""
    model = opts.pop("model", None) or settings.llm_model
    if opts:
        # Opus 5.5는 temperature·top_p·top_k를 400으로 거절한다. 조용히 버리면 호출자가 기대한
        # 동작과 달라지므로 호출 시점에 드러낸다(값은 빼고 옵션 이름만).
        raise TypeError(f"anthropic chat은 model 외 옵션을 받지 않는다: {', '.join(sorted(opts))}")
    if not model:
        logger.error("ai_client.chat.misconfigured", provider="anthropic", missing="LLM_MODEL")
        raise AiClientError("llm_provider=anthropic에는 LLM_MODEL이 필요하다")
    system, turns = _to_anthropic_messages(messages)
    client = _get_anthropic_client()

    import anthropic  # _get_anthropic_client가 이미 불러온 모듈(SDK 예외·omit 참조)
    import httpx2  # anthropic의 HTTP 계층 — 스트림 도중 끊김은 SDK가 감싸지 않고 그대로 올린다

    try:
        # 긴 입력·출력(thinking 포함)은 비스트리밍이면 HTTP 타임아웃에 걸려 스트림으로 받고 최종
        # 메시지만 쓴다. thinking은 보내지 않는다 — Opus 5.5는 항상 adaptive이고 disabled는 400.
        async with client.beta.messages.stream(
            model=model,
            max_tokens=settings.llm_max_tokens,
            system=system or anthropic.omit,
            messages=turns,
            output_config={"effort": settings.llm_effort},
            betas=[_SERVER_SIDE_FALLBACK_BETA],
            fallbacks="default",
        ) as stream:
            message = await stream.get_final_message()
    except (anthropic.APIError, httpx2.HTTPError) as exc:
        # 예외 메시지엔 응답 본문이 섞일 수 있어 남기지 않는다(§9) — 분류값만.
        logger.warning(
            "ai_client.chat.api_error",
            provider="anthropic",
            error_type=type(exc).__name__,
            status_code=getattr(exc, "status_code", None),
            request_id=getattr(exc, "request_id", None),
        )
        raise AiClientError("chat 호출 실패") from exc
    return _final_text(message, requested_model=model)


async def chat(messages: list[dict[str, str]], **opts: Any) -> str:
    """채팅 완성을 1회 호출해 완성된 응답 텍스트를 돌려준다.

    provider(`llm_provider`)에 따라 OpenAI 호환 `/chat/completions`(httpx) 또는 Claude Messages
    API(공식 anthropic SDK)를 부른다. Claude 경로는 내부적으로 스트리밍으로 받지만 호출자에겐
    완성 응답을 1회만 돌려준다(비스트리밍 계약 유지).

    Args:
        messages: OpenAI 형식 메시지 목록(`{"role", "content"}`). Claude 경로는 system 메시지를
            top-level system으로 합치고 user/assistant만 대화 턴으로 보낸다.
        **opts: `model`로 기본 모델을 덮어쓸 수 있다. OpenAI 호환 경로는 나머지 파라미터
            (temperature 등)를 페이로드에 그대로 싣고, Claude 경로는 `model` 외 옵션을 거절한다.

    Returns:
        모델이 생성한 응답 텍스트.

    Raises:
        AiClientError: HTTP·SDK 호출 실패, 응답 형식 이상, 출력 상한에서 잘린 응답, Claude 경로
            필수 설정(AI_API_KEY·LLM_MODEL) 누락.
        LlmRefusalError: Claude가 응답을 거절한 경우(서버 측 폴백 모델까지 거절).
        TypeError: Claude 경로에 `model` 외 옵션을 넘긴 경우.
        ValueError: Claude 경로에 system·user·assistant 외 role이 있는 경우.
    """
    if settings.llm_provider == "anthropic":
        return await _anthropic_chat(messages, opts)
    return await _openai_compat_chat(messages, opts)


async def chat_json(messages: list[dict[str, str]], **opts: Any) -> Any:
    """chat 결과를 JSON으로 파싱한다(코드펜스 제거 + 중괄호 폴백).

    리포트·가드레일이 공유하는 JSON 모드 헬퍼. LLM이 ```json 펜스나 잡음을 섞어도
    관용적으로 파싱하고, 파싱 불가 시에만 `{}`를 반환한다. `chat()`의 HTTP/형식 오류
    (`AiClientError`)는 삼키지 않고 그대로 전파한다(호출자가 판단).

    Args:
        messages: OpenAI 형식 메시지 목록.
        **opts: `chat`에 전달할 추가 파라미터(model 등).

    Returns:
        파싱된 JSON 값(보통 dict). 파싱 실패 시 `{}`.

    Raises:
        AiClientError: `chat()` 호출 자체가 실패한 경우.
    """
    raw = await chat(messages, **opts)
    s = raw.strip()
    if s.startswith("```"):
        s = s.split("```", 2)[1] if s.count("```") >= 2 else s.strip("`")
        s = s[4:].strip() if s.lower().startswith("json") else s.strip()
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        start, end = s.find("{"), s.rfind("}")
        if start != -1 and end != -1:
            try:
                return json.loads(s[start : end + 1])
            except (json.JSONDecodeError, ValueError):
                return {}
        return {}


def _embed_payload(texts: list[str], input_type: EmbedInputType) -> dict[str, Any]:
    """provider별 `/embeddings` 요청 본문을 만든다."""
    if settings.embedding_provider == "voyage":
        return {
            "model": settings.embedding_model,
            "input": texts,
            "input_type": input_type,
            # 모델 기본 차원에 기대지 않고 계약 차원(코퍼스 벡터 컬럼)을 명시한다.
            "output_dimension": settings.embedding_dim,
        }
    # OpenAI 호환은 기존처럼 model·input만 보낸다(input_type 미지원 — 쿼리 instruction 프리픽스는
    # 호출자 소관). 단건은 기존과 같은 문자열 input으로 보내 기본 경로 요청을 바꾸지 않는다.
    return {"model": settings.embedding_model, "input": texts[0] if len(texts) == 1 else texts}


def _parse_embeddings(data: Any) -> list[list[float]]:
    """OpenAI 형식 응답(`{"data": [{"embedding", "index"}, ...]}`)을 입력 순서의 벡터로 편다.

    응답 항목 순서는 보장되지 않아 `index`로 정렬한다. `index`가 없는 서버는 응답 순서를 따른다.
    """
    items = data["data"]
    if all("index" in item for item in items):
        items = sorted(items, key=lambda item: item["index"])
    return [item["embedding"] for item in items]


async def _embed_request(texts: list[str], input_type: EmbedInputType) -> list[list[float]]:
    """`POST /embeddings` 1회. 응답을 입력 순서로 맞추고 개수·차원을 검사한다."""
    client = _get_embed_client()
    try:
        resp = await client.post("/embeddings", json=_embed_payload(texts, input_type))
        resp.raise_for_status()
        vectors = _parse_embeddings(resp.json())
    except httpx.HTTPError as exc:
        raise AiClientError("embed 호출 실패") from exc
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise AiClientError("embed 응답 형식이 올바르지 않음") from exc

    if len(vectors) != len(texts):
        raise AiClientError(f"embed 응답 개수 불일치: {len(vectors)} != {len(texts)}")
    for vector in vectors:
        if len(vector) != settings.embedding_dim:
            raise EmbeddingDimensionError(
                f"임베딩 차원 불일치: {len(vector)} != {settings.embedding_dim}"
            )
    return vectors


async def embed(text: str, *, input_type: EmbedInputType = "query") -> list[float]:
    """텍스트 1건을 임베딩한다. 차원은 embedding_dim(1024) 고정.

    Args:
        text: 임베딩할 입력 텍스트.
        input_type: 검색 쿼리면 `"query"`(기본), 적재할 문서면 `"document"`. voyage provider만
            요청에 싣고(업체가 검색용 프롬프트를 앞에 붙인다) openai_compat은 무시한다.

    Returns:
        길이 embedding_dim의 임베딩 벡터.

    Raises:
        AiClientError: HTTP 오류 또는 응답 형식이 예상과 다른 경우.
        EmbeddingDimensionError: 반환 벡터 길이가 embedding_dim과 다른 경우.
    """
    vectors = await _embed_request([text], input_type)
    return vectors[0]


async def embed_batch(
    texts: list[str], *, input_type: EmbedInputType = "document"
) -> list[list[float]]:
    """여러 텍스트를 임베딩한다. 요청당 최대 32개씩 나눠 순차 호출한다.

    Args:
        texts: 임베딩할 입력 텍스트 목록. 비어 있으면 호출 없이 빈 목록을 돌려준다.
        input_type: 적재할 문서면 `"document"`(기본), 쿼리 묶음이면 `"query"`. voyage만 사용.

    Returns:
        입력과 같은 순서의 임베딩 벡터 목록(각 길이 embedding_dim).

    Raises:
        AiClientError: HTTP 오류, 응답 형식 이상, 응답 벡터 개수가 입력과 다른 경우.
        EmbeddingDimensionError: 반환 벡터 길이가 embedding_dim과 다른 경우.
    """
    vectors: list[list[float]] = []
    for start in range(0, len(texts), _EMBED_BATCH_SIZE):
        batch = texts[start : start + _EMBED_BATCH_SIZE]
        vectors.extend(await _embed_request(batch, input_type))
    return vectors
