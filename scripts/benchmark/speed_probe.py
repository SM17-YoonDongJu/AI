"""생성 속도 프로브 — 정밀 tok/s 측정(파이프라인과 분리, 통제된 프롬프트).

chat()은 usage를 버리므로, 여기서는 엔드포인트를 직접 호출해 정확한 토큰 수를 얻는다.
  - 1차: OpenAI 호환 /chat/completions → usage.completion_tokens / wall-clock (이식성 O)
  - 2차(가능 시): Ollama native /api/chat → prefill·decode 분리(eval_duration 기반)

프롬프트는 우리 노드 특성을 흉내낸 3종 고정:
  - classify: 짧은 JSON 출력(진단 분류형)
  - extract : 긴 컨텍스트 입력(약관 청크형, prefill 부하)
  - compose : 긴 자연어 출력(리포트 본문형, decode 부하)
고정 프롬프트라 모델 간 공정 비교가 된다. warm-up 1회는 집계에서 제외(VRAM 로드 배제).
"""

from __future__ import annotations

import time
from typing import Any

import httpx

# prefill 부하용 긴 컨텍스트(약관 청크 흉내). 고정 문자열이라 모델마다 동일 prefill.
_LONG_CONTEXT = (
    "제3조(보장하는 손해) 회사는 피보험자가 보험기간 중 상해의 직접결과로써 장해분류표에서 "
    "정한 각 장해지급률에 해당하는 후유장해가 발생한 경우 보험가입금액에 장해지급률을 곱한 "
    "금액을 후유장해보험금으로 지급합니다. 다만 동일한 신체부위에 발생한 장해는 합산하지 아니하고 "
    "그중 높은 지급률을 적용하며, 서로 다른 신체부위의 장해지급률은 이를 합산합니다. "
) * 8

_PROMPTS: dict[str, list[dict[str, str]]] = {
    "classify": [
        {"role": "system", "content": "너는 보험 진단 분석가다. JSON만 출력한다."},
        {
            "role": "user",
            "content": "진단명: 전방십자인대 파열 S83.5. 키 diagnosis, accident_type, "
            "requires_disability_review(bool)로만 JSON 출력.",
        },
    ],
    "extract": [
        {
            "role": "system",
            "content": "너는 보험 약관 분석가다. 제공 원문에서만 근거를 찾아 JSON만 출력한다.",
        },
        {
            "role": "user",
            "content": f"[약관 원문]\n{_LONG_CONTEXT}\n\n"
            "키 applicable(list), analysis(str)로 JSON 출력.",
        },
    ],
    "compose": [
        {"role": "system", "content": "너는 보험 손해사정 리포트 작성자다. 금액은 범위로 쓴다."},
        {
            "role": "user",
            "content": "사고: 전방십자인대 파열. 적용특약: 상해후유장해, 수술비특약. "
            "사건요약·적용특약·분쟁포인트·추가확인필요 4개 절로 "
            "상세한 손해사정 리포트 본문을 작성하라.",
        },
    ],
}


def _native_url(base_url: str) -> str:
    """OpenAI 호환 base_url에서 Ollama native /api/chat URL을 유도한다(/v1 제거)."""
    root = base_url.rstrip("/")
    if root.endswith("/v1"):
        root = root[: -len("/v1")]
    return f"{root}/api/chat"


async def _probe_openai(
    client: httpx.AsyncClient, model: str, messages: list[dict[str, str]]
) -> dict[str, Any]:
    """OpenAI 호환 1회 호출 → usage + wall-clock."""
    t0 = time.perf_counter()
    resp = await client.post(
        "/chat/completions",
        json={"model": model, "messages": messages, "stream": False, "temperature": 0},
    )
    resp.raise_for_status()
    dt = time.perf_counter() - t0
    data = resp.json()
    usage = data.get("usage", {}) or {}
    completion = int(usage.get("completion_tokens", 0) or 0)
    prompt = int(usage.get("prompt_tokens", 0) or 0)
    return {
        "wall_s": dt,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "output_tok_s": (completion / dt) if dt else 0.0,  # prefill+decode 합산 실효 처리량
    }


async def _probe_native(
    base_url: str, model: str, messages: list[dict[str, str]]
) -> dict[str, Any] | None:
    """Ollama native 호출 → prefill·decode 분리(가능 시). 실패하면 None."""
    url = _native_url(base_url)
    payload = {"model": model, "messages": messages, "stream": False, "options": {"temperature": 0}}
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
    except (httpx.HTTPError, ValueError):
        return None
    # ns → s. eval=decode, prompt_eval=prefill.
    eval_n = int(data.get("eval_count", 0) or 0)
    eval_ns = int(data.get("eval_duration", 0) or 0)
    pre_n = int(data.get("prompt_eval_count", 0) or 0)
    pre_ns = int(data.get("prompt_eval_duration", 0) or 0)
    return {
        "decode_tok_s": (eval_n / (eval_ns / 1e9)) if eval_ns else 0.0,
        "prefill_tok_s": (pre_n / (pre_ns / 1e9)) if pre_ns else 0.0,
        "eval_count": eval_n,
        "prompt_eval_count": pre_n,
    }


async def probe(model: str, base_url: str, api_key: str, *, repeats: int = 3) -> dict[str, Any]:
    """한 모델의 속도 프로브. 프롬프트 3종 x repeats(첫 회 warm-up 제외).

    Returns:
        {"model", "openai": {프롬프트별 평균}, "native": {프롬프트별 평균 or None}}.
    """
    headers = {"Authorization": f"Bearer {api_key}"} if api_key and api_key != "not-needed" else {}
    result: dict[str, Any] = {"model": model, "openai": {}, "native": {}}
    async with httpx.AsyncClient(base_url=base_url, timeout=180.0, headers=headers) as client:
        for name, messages in _PROMPTS.items():
            samples: list[dict[str, Any]] = []
            native_samples: list[dict[str, Any]] = []
            for idx in range(repeats + 1):  # +1 = warm-up
                oa = await _probe_openai(client, model, messages)
                nat = await _probe_native(base_url, model, messages)
                if idx == 0:
                    continue  # warm-up 제외
                samples.append(oa)
                if nat:
                    native_samples.append(nat)
            result["openai"][name] = _avg_dicts(samples)
            result["native"][name] = _avg_dicts(native_samples) if native_samples else None
    return result


def _avg_dicts(dicts: list[dict[str, Any]]) -> dict[str, float]:
    """동일 키 숫자 dict들의 평균."""
    if not dicts:
        return {}
    keys = dicts[0].keys()
    return {k: sum(float(d[k]) for d in dicts) / len(dicts) for k in keys}
