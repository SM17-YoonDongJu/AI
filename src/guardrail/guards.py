"""가드레일 구현 — 입력/생성/출력 3단계 + 외부 전송 전 PII 마스킹.

- 입력: 문서 본문 정규식 PII 마스킹(주민번호 앞 6자리 보존) + 사용자 질문·사실 서술의 보험·법률
  외 주제 차단
- 외부 전송 전 마스킹(`mask_pii`): OCR 입력단과 같은 규칙(`ocr_worker.masking` 재사용)
- 마스킹 건수(`count_masked_pii`): OCR 마스커가 남긴 `[분류]` 토큰을 분류별로 센다
- 생성: 단정적 금액 표현 → "참고 추정 범위"로 치환
- 출력: 법적 고지문 삽입 + (리포트 한정) LLM Judge 인용 검증과 검증 범위 보고

결과 모델은 `core.contracts`(InputGuardResult/OutputGuardResult)가 단일 출처다.
PII 마스킹 규칙은 `ocr_worker` 입력단과 동일해야 한다(어긋나면 한쪽이 PII 유출). 그래서
`mask_pii`는 규칙을 복제하지 않고 OCR 마스커를 그대로 호출한다. `count_masked_pii`도 토큰
정의(`PiiLabel`)를 그쪽에서 읽는다. `guard_input`이 쓰는 `_mask_pii`는 노션 06 명세(주민번호
앞 6자리 보존)를 따르는 별도 규칙이고, OCR이 이미 마스킹한 `masked_text`에만 건다.
"""

import re
from collections.abc import Sequence
from typing import Any

from core import ai_client
from core.contracts import InputGuardResult, OutputGuardResult
from core.logging import get_logger
from ocr_worker import masking as ocr_masking

logger = get_logger(__name__)

DISCLAIMER = (
    "본 분석은 참고용이며 법적 효력이 없습니다. "
    "정확한 보험금 지급 여부는 담당 손해사정사의 검토 후 확정됩니다."
)

# LLM Judge가 판정하지 못했을 때(호출 실패, 또는 응답이 판정 형식이 아님) judge_failures에
# 남기는 표식. 빈 목록은 '검증 통과'로 읽히므로, 검증을 못 했다는 사실을 남겨 손해사정사가
# 인용을 직접 확인하게 한다. 저장된 초안에 그대로 남는 값이라 문구를 바꾸지 않는다.
JUDGE_UNAVAILABLE = "[인용 검증 미수행 — LLM Judge 호출 실패]"

# Judge 프롬프트에 넣는 범위. 실제로 넣은 양을 OutputGuardResult의 judge_checked_chars·
# judge_evidence_chunks로 돌려준다 — 빈 judge_failures가 "전부 검증해 통과"인지 "일부만
# 봤다"인지 호출자가 구분할 수 있게 하려는 것이다.
JUDGE_MAX_REPORT_CHARS = 12_000  # 리포트(고지문 포함 final_text) 앞부분 글자 수
JUDGE_MAX_EVIDENCE_CHUNKS = 20  # 약관 8 + 장해분류표 ≤8 + 판례 4가 다 들어가는 건수
JUDGE_EVIDENCE_CHARS = 800  # 청크당 글자 수. 장해 추출 ctx와 같다(지급률 숫자가 잘리지 않게)

# 보험·법률 외 주제 차단 키워드(간이). 사용자 질문에는 키워드만으로 건다. 문서 본문에는 걸지
# 않는다. 증권의 "○○보험 주식회사", 화재보험 담보의 "부동산"처럼 정상적으로 나오는 단어라, 문서에
# 걸었을 때는 실사용 리포트 대부분이 막혔다(#103). 사고 경위·추가 정보는 아래 서술용 규칙으로 본다.
_OFF_DOMAIN = ("부동산", "주식", "코인", "비트코인", "연애", "요리", "게임")
# 회사 표기. 사용자도 보험사 정식 이름("○○손해보험 주식회사")을 그대로 쓰므로 빼고 찾는다.
# "(주)"·"㈜"는 키워드를 품지 않아 뺄 필요가 없다.
_COMPANY_DESIGNATION = re.compile(r"주식\s*회사")

# 사고 경위·추가 정보(사실 서술)용 규칙. 서술에는 키워드가 활동·장소·직업으로 정상적으로 들어간다
# ("요리하다가 데었다", "코인노래방에서 넘어졌다", "주식 매매 업무를 담당합니다", "연애 상담사로
# 일합니다"). 그래서 키워드나 용건 표현만으로는 막지 않는다. 세 조건이 다 맞을 때만 막는다.
#   1. 도메인 밖 용건 표현이 있다(키워드 뒤 몇 글자 안의 투자·시세·레시피·공략 등).
#   2. 묻거나 부탁하는 말이 있다(`_REQUEST_MARKERS`). 사실만 적은 문장은 용건이 아니다.
#   3. 보험·법률 사안을 드러내는 말(`_INSURANCE_CONTEXT`)이 하나도 없다.
_NARRATIVE_GAP = 4  # 키워드와 용건 사이에 허용하는 글자 수(조사·띄어쓰기). 문장 부호는 넘지 않는다
_NARRATIVE_OFF_DOMAIN = tuple(
    re.compile(rf"(?P<keyword>{keywords})[^.!?\n]{{0,{_NARRATIVE_GAP}}}?(?P<intent>{intents})")
    for keywords, intents in (
        ("비트코인|코인|주식|부동산", "투자|시세|매수|매도|매매|종목|수익률|전망|추천|차트|상장"),
        ("요리", "레시피|만드는 ?법|하는 ?법|추천"),
        ("게임", "공략|추천|아이템|순위"),
        ("연애", "상담|고민|조언"),
    )
)
# 서술이 보험·법률 사안을 다룬다는 표시. 하나라도 있으면 용건 표현이 있어도 막지 않는다("주식
# 시세를 보며 운전하다 앞차를 추돌", "코인 투자 손실로 위궤양 진단"). 뜻이 갈리는 말은 뺐다.
# "사고"는 "사고 싶다"(사다), "추락"·"넘어"는 "시세 추락"·"1억을 넘어"로도 쓰여 도메인 밖
# 서술까지 통과시킨다.
_INSURANCE_CONTEXT = (
    # 사고·부상
    *("교통사고", "부상", "다치", "다쳐", "다쳤", "다친", "넘어지", "넘어져", "넘어졌", "넘어진"),
    *("미끄러", "부딪", "충돌", "추돌", "쓰러", "굴러", "굴렀", "헛디", "삐끗", "삐었", "접질"),
    *("부러졌", "부러진", "찢어", "긁혀", "끼었", "끼여", "기절", "폭행"),
    *("화상", "데었", "뎄", "데여", "베였", "베인", "베여", "찔려", "찔렸", "물렸", "물려"),
    *("골절", "염좌", "통증", "상처"),
    # 질병·진료
    *("병원", "응급", "입원", "수술", "진단", "치료", "진료", "질병", "질환", "증상"),
    *("식중독", "사망", "장해"),
    # 재물 손해
    *("화재", "침수", "도난", "파손"),
    # 보험·법률, 고지 대상인 직업
    *("보험", "보상", "배상", "피해", "청구", "소송", "분쟁"),
    *("직업", "근무", "종사", "업무", "직원"),
)
# 묻거나 부탁하는 말. 도메인 밖 용건 표현이 사실을 적은 문장("평소 코인 시세를 자주 봅니다")이
# 아니라 요청("코인 시세 알려주세요", "부동산 투자 해도 될까요?")일 때만 막으려고 본다.
_REQUEST_MARKERS = (
    *("?", "\uff1f", "알려", "궁금", "주세요", "해줘", "부탁", "가르쳐"),  # \uff1f: 전각 물음표
    *("싶어", "싶습니다", "싶은데", "어떻게", "어때", "어떨까", "할까", "될까", "나요", "까요"),
    "방법",
)


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


def _find_off_domain(text: str) -> str | None:
    """`_OFF_DOMAIN` 순서로 처음 걸리는 키워드. 없으면 None.

    회사 표기는 지우지 않고 공백으로 바꾼 뒤 찾는다. 그냥 지우면 앞뒤 글자가 붙어 없던 키워드가
    생길 수 있다.
    """
    target = _COMPANY_DESIGNATION.sub(" ", text)
    return next((kw for kw in _OFF_DOMAIN if kw in target), None)


def _find_off_domain_narrative(text: str) -> str | None:
    """사실 서술에서 도메인 밖 용건 요청을 찾는다. 없으면 None(`_NARRATIVE_OFF_DOMAIN` 주석 참고).

    보험·법률 사안을 다루거나 묻고 부탁하는 말이 없으면 용건 표현이 있어도 None이다. 돌려주는
    값은 "{키워드} {용건}"(예: "코인 시세")이다. 두 말 모두 고정 목록에서 나오므로 차단 사유에
    사용자 문장이 섞이지 않는다.
    """
    target = _COMPANY_DESIGNATION.sub(" ", text)
    if any(word in target for word in _INSURANCE_CONTEXT):
        return None
    if not any(marker in target for marker in _REQUEST_MARKERS):
        return None
    for pattern in _NARRATIVE_OFF_DOMAIN:
        if match := pattern.search(target):
            return f"{match['keyword']} {match['intent']}"
    return None


def _off_domain_reason(question: str | None, narratives: Sequence[str | None]) -> str | None:
    """질문을 먼저 보고, 다음에 서술을 하나씩 따로 본다. 처음 걸린 것의 차단 사유. 없으면 None."""
    if keyword := _find_off_domain(question or ""):
        return f"보험·법률 외 질문({keyword})"
    for narrative in narratives:
        if phrase := _find_off_domain_narrative(narrative or ""):
            return f"보험·법률 외 내용({phrase})"
    return None


async def guard_input(
    text: str, *, question: str | None, narratives: Sequence[str | None]
) -> InputGuardResult:
    """입력 가드레일 — 문서 본문 `_mask_pii` 마스킹 + 사용자 입력의 보험·법률 외 주제 차단.

    검사마다 대상이 다르다. 마스킹은 `text`에 건다. report는 OCR이 이미 마스킹한 문서 본문
    `masked_text`를 넣는다. 도메인 검사는 사용자가 직접 쓴 글에만 걸고 문서 본문에는 걸지 않는다.
    질문은 `_OFF_DOMAIN` 키워드가 있으면 막는다. 사고 경위·추가 정보 같은 사실 서술은 키워드만으로
    막지 않는다. 보험·법률 사안 얘기 없이 도메인 밖 용건을 묻거나 부탁할 때만 막는다
    (`_NARRATIVE_OFF_DOMAIN` 주석 참고). 둘 다 회사 표기("주식회사")는 빼고 찾는다.

    `question`·`narratives`는 기본값 없는 키워드 인자다. 빠뜨린 호출이 검사를 조용히 잃지 않고
    TypeError로 드러나게 하려는 것이다. 둘 다 검사만 하고 가리지 않는다. report는 `mask_pii`를
    거친 값을 넘긴다. 복호화한 자유입력 원문을 외부 API로 보내기 전에는 이 함수가 아니라
    `mask_pii`를 쓴다. 이 함수의 마스킹은 주민번호 앞 6자리를 남기고, 이름·주소·이메일·카드는
    가리지 않는다.

    Args:
        text: 마스킹할 문서 본문.
        question: 사용자 질문. None이나 빈 문자열이면 검사하지 않는다.
        narratives: 사용자가 쓴 사실 서술들(사고 경위·추가 정보). 항목마다 따로 검사하고 None·빈
            문자열은 건너뛴다.

    Returns:
        `text`의 마스킹본과 차단 여부·사유. 사유는 질문이면 `보험·법률 외 질문({키워드})`, 서술이면
        `보험·법률 외 내용({키워드} {용건})` 형식이다.
    """
    masked = _mask_pii(text or "")
    reason = _off_domain_reason(question, narratives)
    return InputGuardResult(masked_text=masked, blocked=reason is not None, reason=reason)


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


def count_masked_pii(text: str | None) -> dict[str, int]:
    """OCR 마스커가 남긴 `[분류]` 토큰을 분류별로 센다(순수 함수).

    리포트 수치(가린 PII 건수)용이다. 토큰 정의는 `ocr_worker.masking.PiiLabel` 하나만 본다.
    라벨이 늘거나 바뀌어도 여기는 고칠 필요가 없다. OCR 규칙으로 가린 텍스트(OCR
    `masked_text`, `mask_pii` 결과)에 쓴다. `guard_input`의 `***` 부분 가림은 토큰이 아니라
    세지 않는다.

    토큰 문자열을 세는 것이라 원문에 처음부터 `[이름]` 같은 글자가 있었으면 그것도 센다.
    겹친 탐지는 마스커가 토큰 하나로 합치므로 탐지 건수가 아니라 가린 자리 수다.

    Args:
        text: 마스킹된 텍스트. None이나 빈 문자열이면 빈 dict를 돌려준다.

    Returns:
        `{라벨값: 개수}`. 키는 한국어 라벨값(예 `"이름"`)이고 개수가 1 이상인 라벨만 담는다.
        순서는 `PiiLabel` 정의 순서다.
    """
    if not text:
        return {}
    counts = {label.value: text.count(label.token) for label in ocr_masking.PiiLabel}
    return {label: count for label, count in counts.items() if count > 0}


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
_JUDGE_SYSTEM_PROMPT = (
    "너는 보험 리포트 검증관이다. 리포트의 사실 주장이 제공된 "
    "근거 원문(약관·장해분류표·판례)으로 뒷받침되는지 검증한다. JSON만 출력."
)
_JUDGE_EVIDENCE_SEPARATOR = "\n---\n"


async def guard_output(
    text: str,
    *,
    run_judge: bool = True,
    chunks: list[Any] | None = None,
    max_report_chars: int = JUDGE_MAX_REPORT_CHARS,
) -> OutputGuardResult:
    """출력 가드레일 — 법적 고지문 삽입 + (리포트 한정) LLM Judge 인용 검증.

    Judge는 리포트(고지문 포함) 앞 `max_report_chars`자와 근거 앞 `JUDGE_MAX_EVIDENCE_CHUNKS`건
    (각 `JUDGE_EVIDENCE_CHARS`자)만 본다. 판정을 받으면 실제로 넣은 양을 `judge_checked_chars`·
    `judge_evidence_chunks`로 돌려준다. `judge_total_chars`는 언제나 `len(final_text)`다.

    Judge가 판정하지 못하면 예외를 올리지 않는다. 호출 실패(`AiClientError`, 거절 포함)와
    판정 형식이 아닌 응답(`{"failures": [문장, ...]}`이 아님 — `chat_json`이 파싱 실패 때
    돌려주는 `{}` 포함)이 그렇다. 이때는 고지문 붙은 본문과 `judge_failures=[JUDGE_UNAVAILABLE]`을
    돌려주고, 검증 범위 두 필드는 0이다. 예외를 올리면 호출부(report `safe_node`)가 삼켜 고지문
    없는 원본이 저장되기 때문이다.

    Args:
        text: 리포트 본문.
        run_judge: False면 고지문만 붙이고 Judge를 부르지 않는다.
        chunks: Judge 근거(약관·장해분류표·판례 청크). dict면 `text` 값을, 아니면 `str()`을
            넣는다. 앞에서부터 `JUDGE_MAX_EVIDENCE_CHUNKS`건만 넣으므로 중요한 근거를 앞에
            둔다. 비었거나 None이면 Judge를 부르지 않는다.
        max_report_chars: Judge에 넣을 리포트(고지문 포함) 앞부분 글자 수.

    Returns:
        고지문이 붙은 본문, Judge 판정(`judge_failures`), Judge가 본 범위.

    Raises:
        ValueError: `max_report_chars`가 1보다 작을 때(호출자 버그).
    """
    if max_report_chars < 1:
        raise ValueError("max_report_chars must be at least 1")
    final = text or ""
    if DISCLAIMER not in final:
        final = f"> {DISCLAIMER}\n\n{final}"

    if not (run_judge and chunks):
        return OutputGuardResult(final_text=final, judge_failures=[], judge_total_chars=len(final))

    # LLM Judge: 리포트의 인용·주장이 근거 원문과 부합하는지 검증
    report_part = final[:max_report_chars]
    evidence = [_evidence_text(chunk) for chunk in chunks[:JUDGE_MAX_EVIDENCE_CHUNKS]]
    try:
        verdict = await ai_client.chat_json(_judge_messages(evidence, report_part))
    except ai_client.AiClientError as exc:
        # 고지문은 이미 붙었다. 여기서 예외를 올리면 호출부(report safe_node)가 삼켜 고지문
        # 없는 원본이 저장된다 — 실패를 표식으로 남기고 고지문 붙은 본문을 돌려준다.
        logger.warning("guardrail.judge.failed", error_type=type(exc).__name__)
        return _judge_unavailable(final)

    reason = _unusable_verdict_reason(verdict)
    if reason is not None:
        # 형식만 어긋난 응답을 "failures 없음 = 통과"로 읽으면 검증 못 한 리포트가 통과로 남는다
        logger.warning("guardrail.judge.unusable_verdict", reason=reason)
        return _judge_unavailable(final)

    return OutputGuardResult(
        final_text=final,
        judge_failures=list(verdict["failures"]),
        judge_checked_chars=len(report_part),
        judge_total_chars=len(final),
        judge_evidence_chunks=len(evidence),
    )


def _evidence_text(chunk: object) -> str:
    """근거 청크 1건에서 Judge에 넣을 원문 앞부분. `text`가 없거나 None이면 빈 문자열이다."""
    raw = (chunk.get("text") or "") if isinstance(chunk, dict) else chunk
    return str(raw)[:JUDGE_EVIDENCE_CHARS]


def _judge_messages(evidence: list[str], report_part: str) -> list[dict[str, str]]:
    """Judge 프롬프트. 근거는 약관·장해분류표·판례가 호출자가 넘긴 순서대로 섞여 온다."""
    ctx = _JUDGE_EVIDENCE_SEPARATOR.join(evidence)
    return [
        {"role": "system", "content": _JUDGE_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"[근거 원문(약관·장해분류표·판례)]\n{ctx}\n\n[리포트]\n{report_part}\n\n"
                '근거 없는(환각) 주장이 있으면 {"failures": ["문장1", ...]}, '
                '없으면 {"failures": []} 형식으로만 답하라.'
            ),
        },
    ]


def _unusable_verdict_reason(verdict: object) -> str | None:
    """Judge 응답을 판정으로 쓸 수 없으면 그 분류를, 쓸 수 있으면 None을 돌려준다.

    쓸 수 있는 응답은 프롬프트가 요구한 `{"failures": [문장, ...]}` 하나다. 문장이 아닌 항목도
    판정 불가로 본다. `judge_failures`는 `list[str]`이라 그대로 넣으면 결과 모델 검증에서
    예외가 나고, 그러면 고지문 없는 원본이 저장된다. 분류값은 로그에만 쓴다(응답 본문은 남기지
    않는다).
    """
    if not isinstance(verdict, dict):
        return "verdict_not_object"
    if "failures" not in verdict:
        return "failures_missing"
    failures = verdict["failures"]
    if not isinstance(failures, list):
        return "failures_not_list"
    if not all(isinstance(item, str) for item in failures):
        return "failure_item_not_str"
    return None


def _judge_unavailable(final: str) -> OutputGuardResult:
    """Judge가 판정하지 못한 결과 — 고지문 붙은 본문과 표식. Judge가 본 범위는 0이다."""
    return OutputGuardResult(
        final_text=final, judge_failures=[JUDGE_UNAVAILABLE], judge_total_chars=len(final)
    )
