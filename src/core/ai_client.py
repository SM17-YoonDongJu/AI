"""OpenAI 호환 추론 클라이언트.

base_url·모델명·인증키는 config에서 주입(하드코딩 금지). 챗과 임베딩은 서로 다른 노드에
있을 수 있어(EXAONE vs qwen3:embedding) 엔드포인트를 분리한다. 모든 호출은 async-first이며
블로킹을 유발하지 않는다(httpx.AsyncClient).
"""

from typing import Any

import httpx

from core.config import settings

_chat_client: httpx.AsyncClient | None = None
_embed_client: httpx.AsyncClient | None = None


class AiClientError(RuntimeError):
    """추론 호출 실패의 기반 예외."""


class EmbeddingDimensionError(AiClientError):
    """임베딩 차원이 계약값(embedding_dim)과 다를 때 발생."""


def _auth_headers() -> dict[str, str]:
    """OpenAI 호환 인증 헤더. 로컬(`not-needed`)이면 헤더를 붙이지 않는다."""
    key = settings.ai_api_key
    if key and key != "not-needed":
        return {"Authorization": f"Bearer {key}"}
    return {}


def _get_chat_client() -> httpx.AsyncClient:
    """챗 추론용 공유 AsyncClient(지연 생성)."""
    global _chat_client
    if _chat_client is None:
        _chat_client = httpx.AsyncClient(
            base_url=settings.ai_base_url,
            timeout=settings.ai_timeout_seconds,
            headers=_auth_headers(),
        )
    return _chat_client


def _get_embed_client() -> httpx.AsyncClient:
    """임베딩용 공유 AsyncClient(지연 생성). 챗과 다른 엔드포인트일 수 있다."""
    global _embed_client
    if _embed_client is None:
        _embed_client = httpx.AsyncClient(
            base_url=settings.embedding_base_url,
            timeout=settings.ai_timeout_seconds,
            headers=_auth_headers(),
        )
    return _embed_client


async def close_client() -> None:
    """공유 AsyncClient들을 종료한다(앱 종료 시 1회)."""
    global _chat_client, _embed_client
    for client in (_chat_client, _embed_client):
        if client is not None:
            await client.aclose()
    _chat_client = None
    _embed_client = None


async def chat(messages: list[dict[str, str]], **opts: Any) -> str:
    """채팅 완성을 1회 호출(비스트리밍).

    Args:
        messages: OpenAI 형식 메시지 목록(`{"role", "content"}`).
        **opts: 추가 추론 파라미터(temperature 등). `model`로 기본 모델을 덮어쓸 수 있다.

    Returns:
        모델이 생성한 응답 텍스트.

    Raises:
        AiClientError: HTTP 오류 또는 응답 형식이 예상과 다른 경우.
    """
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
        return data["choices"][0]["message"]["content"]
    except httpx.HTTPError as exc:
        raise AiClientError("chat 호출 실패") from exc
    except (KeyError, IndexError, TypeError) as exc:
        raise AiClientError("chat 응답 형식이 올바르지 않음") from exc


async def embed(text: str) -> list[float]:
    """텍스트 임베딩. 차원은 embedding_dim(1024) 고정.

    Args:
        text: 임베딩할 입력 텍스트.

    Returns:
        길이 embedding_dim의 임베딩 벡터.

    Raises:
        AiClientError: HTTP 오류 또는 응답 형식이 예상과 다른 경우.
        EmbeddingDimensionError: 반환 벡터 길이가 embedding_dim과 다른 경우.
    """
    payload: dict[str, Any] = {"model": settings.embedding_model, "input": text}
    client = _get_embed_client()
    try:
        resp = await client.post("/embeddings", json=payload)
        resp.raise_for_status()
        data = resp.json()
        vector: list[float] = data["data"][0]["embedding"]
    except httpx.HTTPError as exc:
        raise AiClientError("embed 호출 실패") from exc
    except (KeyError, IndexError, TypeError) as exc:
        raise AiClientError("embed 응답 형식이 올바르지 않음") from exc

    if len(vector) != settings.embedding_dim:
        raise EmbeddingDimensionError(
            f"임베딩 차원 불일치: {len(vector)} != {settings.embedding_dim}"
        )
    return vector
