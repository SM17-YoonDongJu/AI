"""ai_client (실험) — Ollama OpenAI 호환 LLM/임베딩 클라이언트.

계약(contracts.md):
  async def chat(messages: list[dict[str, str]], **opts) -> str
  async def embed(text: str) -> list[float]   # len == 1024

모델은 env로 주입(하드코딩 금지). 실험 기본 = qwen2.5:3b(chat) / qwen3-embedding:0.6b(embed).
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx

AI_BASE_URL = os.environ.get("AI_BASE_URL", "http://localhost:11434/v1").rstrip("/")
AI_API_KEY = os.environ.get("AI_API_KEY", "not-needed")
LLM_MODEL = os.environ.get("LLM_MODEL", "qwen2.5:3b")

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "qwen3-embedding:0.6b")
EMBEDDING_DIM = 1024


async def chat(messages: list[dict[str, str]], **opts: Any) -> str:
    """OpenAI 호환 /chat/completions 호출. 비스트리밍 완성 1회."""
    payload = {
        "model": opts.pop("model", LLM_MODEL),
        "messages": messages,
        "temperature": opts.pop("temperature", 0.2),
        "stream": False,
        **opts,
    }
    async with httpx.AsyncClient(timeout=180) as client:
        resp = await client.post(
            f"{AI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {AI_API_KEY}"},
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]


async def chat_json(messages: list[dict[str, str]], **opts: Any) -> Any:
    """chat 결과를 JSON으로 파싱(코드펜스 제거). 실패 시 {} 반환."""
    raw = await chat(messages, **opts)
    s = raw.strip()
    if s.startswith("```"):
        s = s.split("```", 2)[1] if s.count("```") >= 2 else s.strip("`")
        s = s[4:].strip() if s.lower().startswith("json") else s.strip()
    try:
        return json.loads(s)
    except Exception:
        start, end = s.find("{"), s.rfind("}")
        if start != -1 and end != -1:
            try:
                return json.loads(s[start : end + 1])
            except Exception:
                return {}
        return {}


async def embed(text: str) -> list[float]:
    """쿼리/텍스트 임베딩(1024d). Ollama /api/embed."""
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBEDDING_MODEL, "input": text},
        )
        resp.raise_for_status()
        vec = resp.json()["embeddings"][0]
    if len(vec) != EMBEDDING_DIM:
        raise RuntimeError(f"임베딩 차원 {len(vec)} != {EMBEDDING_DIM}")
    return vec
