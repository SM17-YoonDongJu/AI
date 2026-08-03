"""가드레일 3단계 구현 — 입력/생성/출력.

- 입력: 정규식 PII 마스킹(주민번호 앞 6자리 보존) + 보험·법률 외 도메인 차단
- 생성: 단정적 금액 표현 → "참고 추정 범위"로 치환
- 출력: 법적 고지문 삽입 + (리포트 한정) LLM Judge 인용 검증

결과 모델은 `core.contracts`(InputGuardResult/OutputGuardResult)가 단일 출처다.
PII 마스킹 규칙은 `ocr_worker` 입력단과 동일해야 한다(어긋나면 한쪽이 PII 유출).
"""

import re

from core import ai_client
from core.contracts import InputGuardResult, OutputGuardResult

DISCLAIMER = (
    "본 분석은 참고용이며 법적 효력이 없습니다. "
    "정확한 보험금 지급 여부는 담당 손해사정사의 검토 후 확정됩니다."
)

# 보험·법률 외 도메인 차단 키워드(간이)
_OFF_DOMAIN = ("부동산", "주식", "코인", "비트코인", "연애", "요리", "게임")


# ── 입력 가드레일 ──────────────────────────────────────────────
def _mask_pii(text: str) -> str:
    # 주민번호 6-7: 앞 6자리 보존, 뒤 7자리 마스킹
    text = re.sub(r"(\d{6})[- ]?\d{7}", r"\1-*******", text)
    # 전화번호
    text = re.sub(r"01[016789][- ]?\d{3,4}[- ]?\d{4}", "***-****-****", text)
    # 계좌번호(연속 10자리 이상 숫자, 하이픈 포함)
    text = re.sub(r"\b\d{2,6}-\d{2,6}-\d{2,8}\b", "****-****-****", text)
    return text


async def guard_input(text: str) -> InputGuardResult:
    masked = _mask_pii(text or "")
    for kw in _OFF_DOMAIN:
        if kw in (text or ""):
            return InputGuardResult(
                masked_text=masked, blocked=True, reason=f"보험·법률 외 질문({kw})"
            )
    return InputGuardResult(masked_text=masked, blocked=False, reason=None)


# ── 생성 가드레일 ──────────────────────────────────────────────
# 단정 금액 표현 → 참고 추정 범위로 치환
_ABS_AMOUNT = re.compile(
    r"(\d[\d,]*\s*(?:만\s*)?원)\s*(?:을|를)?\s*(?:받습니다|지급됩니다|지급합니다|입니다)"
)


def guard_generation(text: str) -> str:
    def _repl(m: re.Match) -> str:
        return f"참고 추정 범위(약 {m.group(1)} 내외, 약관·근거 기준)"

    return _ABS_AMOUNT.sub(_repl, text or "")


# ── 출력 가드레일 ──────────────────────────────────────────────
async def guard_output(
    text: str, *, run_judge: bool = True, chunks: list | None = None
) -> OutputGuardResult:
    final = text or ""
    if DISCLAIMER not in final:
        final = f"> {DISCLAIMER}\n\n{final}"

    judge_failures: list[str] = []
    if run_judge and chunks:
        # LLM Judge: 리포트의 인용·주장이 검색 청크 원문과 부합하는지 검증
        ctx = "\n---\n".join(
            (c.get("text", "") if isinstance(c, dict) else str(c))[:500] for c in chunks[:6]
        )
        verdict = await ai_client.chat_json(
            [
                {
                    "role": "system",
                    "content": (
                        "너는 보험 리포트 검증관이다. 리포트의 사실 주장이 제공된 약관 원문으로 "
                        "뒷받침되는지 검증한다. JSON만 출력."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"[약관 원문]\n{ctx}\n\n[리포트]\n{final[:2000]}\n\n"
                        '근거 없는(환각) 주장이 있으면 {"failures": ["문장1", ...]}, '
                        '없으면 {"failures": []} 형식으로만 답하라.'
                    ),
                },
            ]
        )
        if isinstance(verdict, dict):
            judge_failures = list(verdict.get("failures", []) or [])

    return OutputGuardResult(final_text=final, judge_failures=judge_failures)
