"""리포트 본문 품질 채점 — 고정 레퍼런스 LLM으로 루브릭(1~5) 평가.

리포트 본문은 자유 생성이라 결정론 채점이 불가하다. 고정 강모델(judge_model)로 5개 축을
1~5로 매긴다. **후보 모델이 자기 답을 채점하면 안 된다**(자기선호 편향) — runner가 judge_model을
후보와 다르게 고정한다. chat_json에 model을 명시해 계측 래퍼가 후보로 라우팅하지 않게 한다.

축: grounding(사실 주장에 근거 인용), amount_safety(금액 단정 회피), completeness(절 완결성),
fluency(한국어 자연스러움), relevance(사고/질문 부합).
"""

from __future__ import annotations

from typing import Any

from core import ai_client

_AXES = ["grounding", "amount_safety", "completeness", "fluency", "relevance"]

_SYSTEM = (
    "너는 보험 손해사정 리포트 품질 평가관이다. 아래 리포트를 5개 축으로 1~5점 채점한다. "
    "관대하지 말고 근거에 따라 엄격히 매겨라. JSON만 출력한다."
)

_RUBRIC = (
    "채점 축(각 1~5):\n"
    "- grounding: 사실 주장에 약관/판례 근거·인용이 붙어 있는가(환각 없는가)\n"
    "- amount_safety: 보험금을 단정하지 않고 범위/추정으로 표현했는가\n"
    "- completeness: 사건요약·적용특약·분쟁포인트·추가확인 등 핵심 절이 갖춰졌는가\n"
    "- fluency: 한국어 표현이 자연스럽고 전문적인가\n"
    "- relevance: 사고/질문에 부합하는 내용인가\n"
    '출력: {"grounding":n,"amount_safety":n,"completeness":n,'
    '"fluency":n,"relevance":n,"comment":"..."}'
)


async def judge_report(report_text: str, *, case_summary: str, judge_model: str) -> dict[str, Any]:
    """리포트 본문을 고정 judge_model로 채점한다.

    Args:
        report_text: 채점 대상 리포트(최종 상태의 report).
        case_summary: 사고/질문 요약(relevance 판단 맥락).
        judge_model: 고정 레퍼런스 모델(후보와 달라야 함).

    Returns:
        축별 점수(1~5) + comment + overall(축 평균). 실패 시 {"error": ...}.
    """
    verdict = await ai_client.chat_json(
        [
            {"role": "system", "content": _SYSTEM},
            {
                "role": "user",
                "content": f"[사고/질문]\n{case_summary}\n\n"
                f"[리포트]\n{report_text[:3000]}\n\n{_RUBRIC}",
            },
        ],
        model=judge_model,  # 명시 → 계측 래퍼가 후보로 바꾸지 않음
        temperature=0,
    )
    if not isinstance(verdict, dict):
        return {"error": "judge_non_json"}
    scores = {}
    for axis in _AXES:
        try:
            scores[axis] = float(verdict.get(axis))
        except (TypeError, ValueError):
            scores[axis] = None
    valid = [v for v in scores.values() if v is not None]
    scores["overall"] = (sum(valid) / len(valid)) if valid else None
    scores["comment"] = str(verdict.get("comment", ""))[:300]
    return scores
