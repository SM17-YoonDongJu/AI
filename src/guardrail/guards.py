"""가드레일 구현 — 입력/생성/출력 3단계 + 외부 전송 전 PII 마스킹.

- 입력: 정규식 PII 마스킹(주민번호 앞 6자리 보존) + 보험·법률 외 도메인 차단
- 외부 전송 전 마스킹(`mask_pii`): OCR 입력단과 같은 규칙(`ocr_worker.masking` 재사용)
- 생성: 단정적 금액 표현 → "참고 추정 범위"로 치환
- 출력: 법적 고지문 삽입 + (리포트 한정) LLM Judge 인용 검증

결과 모델은 `core.contracts`(InputGuardResult/OutputGuardResult)가 단일 출처다.
PII 마스킹 규칙은 `ocr_worker` 입력단과 동일해야 한다(어긋나면 한쪽이 PII 유출). 그래서
`mask_pii`는 규칙을 복제하지 않고 OCR 마스커를 그대로 호출한다. `guard_input`이 쓰는
`_mask_pii`는 노션 06 명세(주민번호 앞 6자리 보존)를 따르는 별도 규칙이고, OCR이 이미
마스킹한 `masked_text`에만 건다.
"""

import re

from core import ai_client
from core.contracts import InputGuardResult, OutputGuardResult
from core.logging import get_logger
from ocr_worker import masking as ocr_masking

logger = get_logger(__name__)

DISCLAIMER = (
    "본 분석은 참고용이며 법적 효력이 없습니다. "
    "정확한 보험금 지급 여부는 담당 손해사정사의 검토 후 확정됩니다."
)

# LLM Judge 호출 자체가 실패했을 때 judge_failures에 남기는 표식. 빈 목록은 '검증 통과'로
# 읽히므로, 검증을 못 했다는 사실을 남겨 손해사정사가 인용을 직접 확인하게 한다.
JUDGE_UNAVAILABLE = "[인용 검증 미수행 — LLM Judge 호출 실패]"

# 보험·법률 외 도메인 차단 키워드(간이)
_OFF_DOMAIN = ("부동산", "주식", "코인", "비트코인", "연애", "요리", "게임")


# ── 입력 가드레일 ──────────────────────────────────────────────
def _mask_pii(text: str) -> str:
    """`guard_input`용 정규식 마스킹(노션 06 명세: 주민번호 앞 6자리 보존).

    OCR이 이미 마스킹한 `masked_text`에 한 번 더 거는 방어선이라 주민번호·휴대폰·하이픈
    계좌 3종만 본다. 외부 API로 보낼 자유입력 원문에는 쓰지 않는다. 그쪽은 `mask_pii`다.
    """
    # 주민번호 6-7: 앞 6자리 보존, 뒤 7자리 마스킹
    text = re.sub(r"(\d{6})[- ]?\d{7}", r"\1-*******", text)
    # 전화번호
    text = re.sub(r"01[016789][- ]?\d{3,4}[- ]?\d{4}", "***-****-****", text)
    # 계좌번호(연속 10자리 이상 숫자, 하이픈 포함)
    text = re.sub(r"\b\d{2,6}-\d{2,6}-\d{2,8}\b", "****-****-****", text)
    return text


async def guard_input(text: str) -> InputGuardResult:
    """입력 가드레일 — `_mask_pii` 마스킹 + 보험·법률 외 도메인 차단.

    report는 OCR이 이미 마스킹한 `masked_text`를 넣는다. 복호화한 자유입력 원문을 외부
    API로 보내기 전에는 이 함수가 아니라 `mask_pii`를 쓴다. 이 함수는 주민번호 앞 6자리를
    남기고, 이름·주소·이메일·카드는 가리지 않는다.

    Args:
        text: 검사할 텍스트.

    Returns:
        마스킹된 텍스트와 차단 여부·사유.
    """
    masked = _mask_pii(text or "")
    for kw in _OFF_DOMAIN:
        if kw in (text or ""):
            return InputGuardResult(
                masked_text=masked, blocked=True, reason=f"보험·법률 외 질문({kw})"
            )
    return InputGuardResult(masked_text=masked, blocked=False, reason=None)


# ── 외부 전송 전 PII 마스킹 (OCR 입력단 규칙 공유) ─────────────────
def mask_pii(text: str) -> str:
    """자유입력 원문의 PII를 OCR 입력단과 같은 규칙으로 가린다(외부 API 전송 전).

    Spring DB에서 복호화한 자유입력 필드(`reports.question`, `user_claims.description`,
    `user_claims.additional_information`)를 프롬프트·검색 쿼리에 넣기 전에 쓴다(§13).
    규칙을 복제하지 않고 `ocr_worker.masking`을 그대로 호출한다. 정규식은 항상 쓰고,
    `settings.use_ner`가 켜져 있으면 NER도 합친다. 그다음 OCR `_analyze`와 같은 잔류 게이트
    (`assert_no_residual`)를 거친다. 같은 텍스트면 OCR이 저장하는 `masked_text`와 결과가
    같고, OCR이 거절하는 텍스트는 여기서도 예외가 난다.

    `guard_input`과 다른 점: 주민번호를 앞 6자리까지 `[주민등록번호]`로 통째로 바꾼다.
    생년월일도 외부로 나가지 않는다.

    한계(자세한 목록은 `src/guardrail/README.md`):
    - 운영 report에는 NER이 없다(`USE_NER=false`, 이미지에 transformers 없음). 정규식은
      라벨 뒤 이름만 잡는다. 라벨 없는 자유문 속 이름("저는 홍길동입니다")은 그대로 나간다.
      라벨 뒤라도 이름과 조사·호칭을 합쳐 4글자를 넘으면("피보험자 홍길동님이") 놓친다.
    - 멱등이 아니다. 앵커 키워드를 품은 토큰(`[계좌번호]`·`[환자등록번호]` 등) 뒤에 숫자가
      있으면, 다시 걸 때 그 숫자(날짜·금액)까지 가린다. 원문에 한 번만 건다. OCR
      `masked_text`에는 다시 걸지 않는다.
    - 동기 함수다. NER을 켜면 CPU 추론이 끼므로 async 코드에서는 `asyncio.to_thread`로 뗀다.

    Args:
        text: 복호화한 자유입력 원문.

    Returns:
        PII를 `[분류]` 토큰으로 바꾼 텍스트. 빈 문자열은 빈 문자열 그대로다.

    Raises:
        MaskingError: 마스킹 뒤에도 고민감 PII(주민·외국인등록번호·카드)가 남았을 때
            (예: 점으로 구분한 주민번호). 이 텍스트는 외부로 보내지 않는다. 메시지에는
            분류와 건수만 담긴다.
        ModuleNotFoundError: `USE_NER=true`인데 transformers가 없을 때(report 이미지).
            비어 있지 않은 입력을 처음 넣을 때 난다.
    """
    masked = ocr_masking.mask(text)
    ocr_masking.assert_no_residual(masked)
    return masked


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
        try:
            verdict = await ai_client.chat_json(
                [
                    {
                        "role": "system",
                        "content": (
                            "너는 보험 리포트 검증관이다. 리포트의 사실 주장이 제공된 "
                            "약관 원문으로 뒷받침되는지 검증한다. JSON만 출력."
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
        except ai_client.AiClientError as exc:
            # 고지문은 이미 붙었다. 여기서 예외를 올리면 호출부(report safe_node)가 삼켜 고지문
            # 없는 원본이 저장된다 — 실패를 표식으로 남기고 고지문 붙은 본문을 돌려준다.
            logger.warning("guardrail.judge.failed", error_type=type(exc).__name__)
            return OutputGuardResult(final_text=final, judge_failures=[JUDGE_UNAVAILABLE])
        if isinstance(verdict, dict):
            judge_failures = list(verdict.get("failures", []) or [])

    return OutputGuardResult(final_text=final, judge_failures=judge_failures)
