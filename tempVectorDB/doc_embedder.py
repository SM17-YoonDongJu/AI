"""tempVectorDB 적재기 공용 문서 임베딩 — Voyage(기본) | Ollama.

load_cases.py·load_schedule.py가 같이 쓴다. 적재기는 독립 스크립트라 src(core.ai_client)를
import하지 않는다. 적재 모델은 쿼리 쪽(src/rag — EMBEDDING_PROVIDER·EMBEDDING_MODEL)과 같아야
벡터 공간이 맞는다. 다르면 오류 없이 검색 결과만 틀어진다.

env:
    EMBED_PROVIDER   voyage(기본, 쿼리 쪽 core.config 기본과 같다) | ollama
    EMBED_MODEL      기본 ollama=qwen3-embedding:0.6b, voyage=voyage-4-large
    OLLAMA_URL       ollama 엔드포인트(기본 http://localhost:11434) — POST /api/embed
    VOYAGE_API_KEY   voyage 인증키(필수, 시크릿 — 커밋·출력 금지)
    VOYAGE_BASE_URL  voyage 엔드포인트(기본 https://ai.mongodb.com/v1) — POST /embeddings

문서는 원문 그대로 임베딩한다. voyage는 input_type=document로 보낸다(쿼리 쪽은 query).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import httpx

# 테이블 embedding 컬럼이 halfvec(1024)다. voyage는 output_dimension으로 이 차원을 요청한다.
EMBED_DIM = 1024

_DEFAULT_MODELS = {"ollama": "qwen3-embedding:0.6b", "voyage": "voyage-4-large"}


@dataclass(frozen=True, slots=True)
class EmbedConfig:
    """env에서 읽은 문서 임베딩 설정."""

    provider: str  # ollama | voyage
    model: str
    ollama_url: str
    voyage_base_url: str
    voyage_api_key: str = field(repr=False)  # 시크릿 — repr·print에 나오지 않게 한다

    @classmethod
    def from_env(cls) -> EmbedConfig:
        """env를 읽어 설정을 만든다.

        Raises:
            ValueError: EMBED_PROVIDER가 ollama·voyage가 아니거나, voyage인데 키가 없는 경우.
        """
        provider = os.environ.get("EMBED_PROVIDER", "voyage").strip().lower()
        if provider not in _DEFAULT_MODELS:
            raise ValueError(f"EMBED_PROVIDER는 ollama 또는 voyage여야 한다: {provider!r}")
        config = cls(
            provider=provider,
            model=os.environ.get("EMBED_MODEL") or _DEFAULT_MODELS[provider],
            ollama_url=os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/"),
            voyage_base_url=os.environ.get("VOYAGE_BASE_URL", "https://ai.mongodb.com/v1").rstrip(
                "/"
            ),
            voyage_api_key=os.environ.get("VOYAGE_API_KEY", ""),
        )
        if provider == "voyage" and not config.voyage_api_key:
            raise ValueError("EMBED_PROVIDER=voyage에는 VOYAGE_API_KEY가 필요하다")
        return config

    def describe(self) -> str:
        """적재 로그용 한 줄 요약(키 제외)."""
        return f"{self.provider} / {self.model} / {EMBED_DIM}d"


async def embed_batch(
    client: httpx.AsyncClient, config: EmbedConfig, texts: list[str]
) -> list[str]:
    """문서 텍스트 묶음을 임베딩해 halfvec 리터럴('[v1,...]') 목록으로 돌려준다(입력 순서).

    리터럴은 INSERT에서 `$n::halfvec`로 캐스트한다. 적재기는 pgvector 코덱 없이 접속하므로
    문자열로 넘긴다.

    Raises:
        httpx.HTTPError: 임베딩 서버 호출이 실패한 경우.
        ValueError: 응답 벡터의 개수나 차원이 입력·EMBED_DIM과 다른 경우.
    """
    if config.provider == "voyage":
        vectors = await _voyage_embed(client, config, texts)
    else:
        vectors = await _ollama_embed(client, config, texts)
    if len(vectors) != len(texts):
        raise ValueError(f"임베딩 개수 불일치: {len(vectors)} != {len(texts)}")
    for vector in vectors:
        if len(vector) != EMBED_DIM:
            raise ValueError(f"임베딩 차원 불일치: {len(vector)} != {EMBED_DIM}")
    return ["[" + ",".join(map(str, vector)) + "]" for vector in vectors]


async def _ollama_embed(
    client: httpx.AsyncClient, config: EmbedConfig, texts: list[str]
) -> list[list[float]]:
    """Ollama 네이티브 /api/embed. 응답 embeddings는 입력 순서다."""
    resp = await client.post(
        f"{config.ollama_url}/api/embed", json={"model": config.model, "input": texts}
    )
    resp.raise_for_status()
    embeddings: list[list[float]] = resp.json()["embeddings"]
    return embeddings


async def _voyage_embed(
    client: httpx.AsyncClient, config: EmbedConfig, texts: list[str]
) -> list[list[float]]:
    """Voyage /embeddings(OpenAI 형식 응답). 항목 순서가 보장되지 않아 index로 정렬한다."""
    resp = await client.post(
        f"{config.voyage_base_url}/embeddings",
        headers={"Authorization": f"Bearer {config.voyage_api_key}"},
        json={
            "model": config.model,
            "input": texts,
            "input_type": "document",
            "output_dimension": EMBED_DIM,
        },
    )
    resp.raise_for_status()
    items = sorted(resp.json()["data"], key=lambda item: item["index"])
    return [item["embedding"] for item in items]
