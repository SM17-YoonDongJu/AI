"""rag·guardrail·ai_client 시그니처 mock.

실제 구현 교체 지점:
  rag.search()        ← src/rag (8-feature-rag)
  guardrail.*()       ← src/guardrail (9-feature-가드레일)
  ai_client.chat/embed ← src/core/ai_client.py (미구현)
"""

from __future__ import annotations

from typing import Any


# ── RAG (README: 입력=쿼리+namespace 힌트, 출력=ranked_chunks+citations) ──
def search(
    query: str,
    namespaces: list[tuple[str, int]],
    *,
    insurer: str | None = None,
) -> dict[str, Any]:
    """Hybrid 검색 mock. 실제: BM25+pgvector+RRF → 청크+인용."""
    return {
        "ranked_chunks": [
            {
                "chunk_id": "mock-1",
                "content": f"[MOCK 약관 청크] '{query}' 관련 보장 조항",
                "article_number": "제5조",
                "article_title": "보험금의 지급사유",
                "insurer": insurer or "메리츠화재",
            }
        ],
        "citations": ["[제5조 제2항]"],
    }


# ── 가드레일 3단 (README 기준) ──
def input_guard(text: str) -> dict[str, Any]:
    """입력: PII 마스킹 + 도메인 외 차단 mock."""
    return {"masked_text": text, "blocked": False, "reason": None}


def generation_guard(text: str) -> str:
    """생성: 단정 금액 → 참고 범위 치환, 인용 강제 mock."""
    return text


def output_guard(report: str, citations: list[str], *, run_judge: bool = True) -> dict[str, Any]:
    """출력: 고지문 삽입 + LLM Judge 인용검증 mock (리포트는 run_judge=True)."""
    disclaimer = "본 분석은 참고용이며 법적 효력이 없습니다. 정확한 보험금 지급 여부는 담당 손해사정사의 검토 후 확정됩니다.\n\n"
    return {"report": disclaimer + report, "judge_failures": []}


# ── ai_client (모델 미정 — env 주입) ──
def chat(messages: list[dict[str, str]], **kwargs: Any) -> str:
    """LLM 생성 mock."""
    return "[MOCK LLM 응답]"


def embed(texts: list[str]) -> list[list[float]]:
    """임베딩 mock (1024d). 실제: qwen3:embedding / BGE-M3."""
    return [[0.0] * 1024 for _ in texts]
