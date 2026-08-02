"""ollama VLM(vision) 클라이언트 — 표 구조 보존 보조 추출(하이브리드 경로).

core/ai_client.py는 OpenAI 호환 chat/embeddings 전용이라 이미지 페이로드를 지원하지
않는다. VLM은 다중 항목 표 문서(지급결과서·청구서·입원확인서·진료비영수증)에서
surya 라인 순서가 표 구조를 보존하지 못하는 문제를 보완하는 OCR 전용 보조 단계라
core가 아닌 여기 별도 모듈로 둔다.
"""

import base64
import io
from typing import Any

import httpx

from core.config import settings
from ocr_worker.ocr import PageImage

_vlm_client: httpx.AsyncClient | None = None

VLM_TABLE_PROMPT = (
    "이 이미지는 한국 보험/의료 문서입니다. 이미지 안의 모든 텍스트를 표 구조를 "
    "유지해 가능한 한 정확히 그대로 옮겨 적어주세요. 표는 마크다운 표로, "
    "항목-금액 쌍은 누락 없이 옮겨주세요. 추측하지 말고 보이는 대로만 적어주세요. "
    "특정 부분이 흐리거나 읽을 수 없으면 내용을 지어내지 말고 '[읽을 수 없음]'이라고만 "
    "적어주세요."
)


class VlmClientError(RuntimeError):
    """VLM 호출 실패의 기반 예외."""


def _get_vlm_client() -> httpx.AsyncClient:
    """VLM 추론용 공유 AsyncClient(지연 생성)."""
    global _vlm_client
    if _vlm_client is None:
        _vlm_client = httpx.AsyncClient(
            base_url=settings.vlm_base_url, timeout=settings.vlm_timeout_seconds
        )
    return _vlm_client


async def close_client() -> None:
    """공유 AsyncClient를 종료한다(앱 종료 시 1회, core/ai_client.close_client와 함께 호출)."""
    global _vlm_client
    if _vlm_client is not None:
        await _vlm_client.aclose()
    _vlm_client = None


async def transcribe_table(image: PageImage) -> str:
    """페이지 이미지 1장을 VLM으로 표 구조 보존 전사한다.

    Args:
        image: ``render_to_images``가 만든 PIL 페이지 이미지.

    Returns:
        마크다운 표를 포함한 전사 텍스트(원문 그대로 — PII 포함 가능, 호출측이 마스킹).

    Raises:
        VlmClientError: HTTP 오류·타임아웃·응답 형식 이상.
    """
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    client = _get_vlm_client()
    payload: dict[str, Any] = {
        "model": settings.vlm_model,
        "prompt": VLM_TABLE_PROMPT,
        "images": [b64],
        "stream": False,
        # temperature=0: 창작 경향을 낮춰 환각(이미지에 없는 내용 지어내기)을 줄인다.
        # 완전히 막진 못하므로 pipeline._looks_grounded()가 별도 안전장치로 검증한다.
        "options": {"temperature": 0.0},
    }
    try:
        resp = await client.post("/api/generate", json=payload)
        resp.raise_for_status()
        return resp.json()["response"]
    except httpx.HTTPError as exc:
        raise VlmClientError("VLM 표 전사 호출 실패") from exc
    except (KeyError, TypeError) as exc:
        raise VlmClientError("VLM 응답 형식이 올바르지 않음") from exc
