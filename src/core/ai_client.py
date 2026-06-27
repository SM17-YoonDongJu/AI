"""OpenAI 호환 추론 클라이언트.

base_url·모델명은 config에서 주입(하드코딩 금지). 모든 호출은 async-first이며 블로킹을
유발 x(httpx.AsyncClient)
"""

from typing import Any

import httpx

from core.config import settings

_client: httpx.AsyncClient | None = None


class AiClientError(RuntimeError):
    """추론 호출 실패의 기반 예외."""


class EmbeddingDimensionError(AiClientError):
    """임베딩 차원이 계약값(EMBEDDING_DIM)과 다를 때 발생."""


def _get_client() -> httpx.AsyncClient:
    """공유 AsyncClient를 지연 생성해 반환한다(커넥션 재사용)."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=settings.ollama_base_url,
            timeout=settings.ai_timeout_seconds,
        )
    return _client


async def close_client() -> None:
    """공유 AsyncClient를 종료한다(앱 종료 시 1회)."""
    global _client
    if _client is None:
        return
    await _client.aclose()
    _client = None


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
    model = opts.pop("model", None) or settings.chat_model
    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "stream": False,
        **opts,
    }
    client = _get_client()
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
    """텍스트 임베딩. 차원은 EMBEDDING_DIM(1024) 고정.

    Args:
        text: 임베딩할 입력 텍스트.

    Returns:
        길이 EMBEDDING_DIM의 임베딩 벡터.

    Raises:
        AiClientError: HTTP 오류 또는 응답 형식이 예상과 다른 경우.
        EmbeddingDimensionError: 반환 벡터 길이가 EMBEDDING_DIM과 다른 경우.
    """
    payload: dict[str, Any] = {"model": settings.embedding_model, "input": text}
    client = _get_client()
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
