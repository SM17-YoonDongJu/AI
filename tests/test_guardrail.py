"""guardrail 테스트 — PII 마스킹(mask_pii·guard_input)·도메인 차단·단정금액 치환·고지문 삽입·
LLM Judge 검증 범위와 판정 처리·마스킹 건수(count_masked_pii).

LLM Judge(run_judge=True)는 ai_client.chat_json을 페이크로 바꿔 LLM 없이 검증한다. 프롬프트에
들어간 범위와 응답 처리만 본다. 실제 모델의 판정 품질은 e2e에서 확인한다.

mask_pii 테스트는 OCR 기본 마스커를 정규식 전용으로 고정한다(autouse 픽스처). 운영 report
설정(USE_NER=false)과 같고, 셸·.env의 USE_NER 값과 상관없이 결과가 같게 하려는 것이다.
모든 PII는 합성 값이다.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from core import ai_client
from core.config import get_settings
from core.contracts import InputGuardResult, OutputGuardResult
from guardrail import (
    DISCLAIMER,
    JUDGE_EVIDENCE_CHARS,
    JUDGE_MAX_EVIDENCE_CHUNKS,
    JUDGE_MAX_REPORT_CHARS,
    MaskingError,
    count_masked_pii,
    guard_generation,
    guard_input,
    guard_output,
    guards,
    mask_pii,
)
from guardrail.guards import JUDGE_UNAVAILABLE, _mask_pii
from ocr_worker import masking as ocr_masking
from ocr_worker.masking import Masker, PiiLabel, Span
from ocr_worker.masking import masker as masker_module
from ocr_worker.masking import ner_detector as ner_detector_module

_SRC_DIR = Path(__file__).resolve().parents[1] / "src"


@pytest.fixture(autouse=True)
def regex_only_masker(monkeypatch: pytest.MonkeyPatch) -> None:
    """OCR 기본 마스커를 정규식 전용으로 고정한다(운영 report 설정: USE_NER=false)."""
    monkeypatch.setattr(masker_module, "_default_masker", Masker(use_ner=False))


def test_mask_pii_preserves_rrn_front6_and_masks_rest() -> None:
    masked = _mask_pii("환자 860312-1948571 연락처 010-9876-5432 계좌 110-234-567890")
    assert "860312-*******" in masked  # 앞 6자리 보존
    assert "1948571" not in masked  # 뒤 7자리 마스킹
    assert "010-9876-5432" not in masked  # 전화 마스킹
    assert "110-234-567890" not in masked  # 계좌 마스킹


async def test_guard_input_blocks_off_domain_question() -> None:
    result = await guard_input("", question="비트코인 시세랑 부동산 투자 문의입니다.")
    assert isinstance(result, InputGuardResult)
    assert result.blocked is True
    assert result.reason == "보험·법률 외 질문(부동산)"


async def test_guard_input_allows_domain_and_masks() -> None:
    result = await guard_input("진단명 경추 염좌, 환자 860312-1948571", question=None)
    assert result.blocked is False
    assert "860312-*******" in result.masked_text


# ── guard_input: 도메인 검사 대상 (#103) ─────────────────────────
# 실제 증권·진단서처럼 키워드가 정상적으로 들어간 문서 본문(가상 보험사).
_DOCUMENT_WITH_KEYWORDS = (
    "--- 문서 1: 보험증권 ---\n누리손해보험 주식회사 누리화재보험 담보: 부동산 화재손해\n\n"
    "--- 문서 2: 진단서 ---\n병명: 손목 2도 화상 발병 경위: 요리 중 끓는 물에 데임"
)


async def test_guard_input_skips_domain_check_on_document_text() -> None:
    result = await guard_input(_DOCUMENT_WITH_KEYWORDS, question=None)
    assert result.blocked is False
    assert result.reason is None


@pytest.mark.parametrize(
    "question",
    [
        "누리손해보험 주식회사에서 보험금을 덜 받았어요",
        "누리손해보험 주식 회사에 청구했습니다",
        # 회사 표기를 공백 대신 빈 문자열로 지우면 "에코인천"이 되어 "코인"이 생긴다.
        "에코주식회사인천지점 직원인데 출장 중에 다쳤어요",
    ],
)
async def test_guard_input_ignores_company_designation_in_question(question: str) -> None:
    result = await guard_input("", question=question)
    assert result.blocked is False


async def test_guard_input_blocks_stock_question_next_to_company_name() -> None:
    result = await guard_input("", question="누리손해보험 주식회사 주식을 사도 될까요?")
    assert result.blocked is True
    assert result.reason == "보험·법률 외 질문(주식)"


async def test_guard_input_requires_question_argument() -> None:
    # 한 인자 호출이 도메인 검사를 조용히 잃지 않고 드러나야 한다.
    with pytest.raises(TypeError):
        await guard_input("문서 본문")  # type: ignore[call-arg]


def test_guard_generation_softens_absolute_amount() -> None:
    out = guard_generation("보험금 1,200만원을 받습니다.")
    assert "참고 추정 범위" in out
    assert "받습니다" not in out


# ── guard_output: 고지문 + LLM Judge(페이크) ─────────────────────
class _FakeJudge:
    """ai_client.chat_json 대역. 받은 메시지를 남기고 정해 둔 응답을 그대로 돌려준다."""

    def __init__(self, verdict: object) -> None:
        self.verdict = verdict
        self.calls: list[list[dict[str, str]]] = []

    async def chat_json(self, messages: list[dict[str, str]], **opts: object) -> object:
        self.calls.append(messages)
        return self.verdict

    @property
    def system_prompt(self) -> str:
        return self.calls[-1][0]["content"]

    @property
    def user_prompt(self) -> str:
        return self.calls[-1][1]["content"]


def _install_judge(monkeypatch: pytest.MonkeyPatch, verdict: object) -> _FakeJudge:
    judge = _FakeJudge(verdict)
    monkeypatch.setattr(ai_client, "chat_json", judge.chat_json)
    return judge


async def _refusing_judge(messages: list[dict[str, str]], **opts: object) -> object:
    """Judge 호출 실패 대역(거절 — 429·잘림도 같은 AiClientError 계열이다)."""
    raise ai_client.LlmRefusalError("거절")


class _WarningRecorder:
    """guards.logger 대역. 이벤트와 필드를 그대로 남긴다(로그에 본문이 없는지 확인용)."""

    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, object]]] = []

    def warning(self, event: str, **fields: object) -> None:
        self.records.append((event, fields))


def _install_log_recorder(monkeypatch: pytest.MonkeyPatch) -> _WarningRecorder:
    recorder = _WarningRecorder()
    monkeypatch.setattr(guards, "logger", recorder)
    return recorder


# 약관·장해분류표·판례가 섞인 근거(호출자가 넘기는 순서 그대로).
_EVIDENCE: list[dict[str, str]] = [
    {"text": "제3조(상해입원일당) 1일당 3만원을 지급한다"},
    {"text": "별표3 척추에 약간의 운동장해를 남긴 때 지급률 10%"},
    {"text": "2020-123 요추 염좌 후유장해 지급 분쟁 사례"},
]

# 근거 상한보다 1건 많은 근거. 마지막(21번째) 건은 Judge 프롬프트에서 빠져야 한다.
# 표식을 꺾쇠로 닫아 "<근거2>"가 "<근거20>"에 부분 일치하지 않게 한다.
_EVIDENCE_OVER_LIMIT: list[dict[str, str]] = [
    {"text": f"<근거{n}>"} for n in range(1, JUDGE_MAX_EVIDENCE_CHUNKS + 2)
]
_LAST_INCLUDED_EVIDENCE = f"<근거{JUDGE_MAX_EVIDENCE_CHUNKS}>"
_FIRST_EXCLUDED_EVIDENCE = f"<근거{JUDGE_MAX_EVIDENCE_CHUNKS + 1}>"

# 판정으로 쓸 수 없는 Judge 응답과 로그에 남는 분류. chat_json은 파싱에 실패하면 {}를 준다.
_UNUSABLE_VERDICTS = [
    pytest.param({}, "failures_missing", id="parse-failure-empty-object"),
    pytest.param({"failures": None}, "failures_not_list", id="failures-null"),
    pytest.param({"failures": "환자 홍길동 관련 문장"}, "failures_not_list", id="failures-string"),
    pytest.param({"failures": {"문장": "제3조"}}, "failures_not_list", id="failures-object"),
    pytest.param(["제3조 문장"], "verdict_not_object", id="verdict-array"),
    pytest.param("근거 없음", "verdict_not_object", id="verdict-string"),
    pytest.param(None, "verdict_not_object", id="verdict-null"),
    pytest.param(
        {"failures": [{"문장": "제99조", "사유": "근거 없음"}]},
        "failure_item_not_str",
        id="failure-item-object",
    ),
]


async def test_guard_output_inserts_disclaimer_without_judge() -> None:
    result = await guard_output("리포트 본문", run_judge=False, chunks=None)
    assert isinstance(result, OutputGuardResult)
    assert result.final_text.startswith(f"> {DISCLAIMER}")
    assert result.judge_failures == []


@pytest.mark.parametrize(
    ("run_judge", "chunks"),
    [
        pytest.param(False, _EVIDENCE, id="run-judge-false"),
        pytest.param(True, [], id="empty-chunks"),
        pytest.param(True, None, id="no-chunks"),
    ],
)
async def test_guard_output_skips_judge_without_flag_or_evidence(
    monkeypatch: pytest.MonkeyPatch, run_judge: bool, chunks: list[dict[str, str]] | None
) -> None:
    # Arrange
    judge = _install_judge(monkeypatch, {"failures": ["불리면 안 된다"]})

    # Act
    result = await guard_output("리포트 본문", run_judge=run_judge, chunks=chunks)

    # Assert: Judge를 부르지 않았고, 본 범위는 0이지만 전체 길이는 채운다
    assert judge.calls == []
    assert result.judge_failures == []
    assert result.judge_checked_chars == 0
    assert result.judge_evidence_chunks == 0
    assert result.judge_total_chars == len(result.final_text)


async def test_guard_output_reports_judge_scope_on_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    _install_judge(monkeypatch, {"failures": ["제99조에 따라 전액 지급된다"]})

    # Act
    result = await guard_output("리포트 본문", run_judge=True, chunks=_EVIDENCE)

    # Assert: 판정을 그대로 돌려주고, 짧은 리포트는 전부 봤다(검증 범위 100%)
    assert result.judge_failures == ["제99조에 따라 전액 지급된다"]
    assert result.judge_checked_chars == len(result.final_text)
    assert result.judge_total_chars == len(result.final_text)
    assert result.judge_evidence_chunks == len(_EVIDENCE)


async def test_guard_output_judges_report_up_to_window(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 고지문 포함 정확히 창 크기. 끝 표식은 예전 창(앞 2,000자) 밖에 있다
    tail = "<<본문 끝>>"
    report = DISCLAIMER + "가" * (JUDGE_MAX_REPORT_CHARS - len(DISCLAIMER) - len(tail)) + tail
    judge = _install_judge(monkeypatch, {"failures": []})

    # Act
    result = await guard_output(report, run_judge=True, chunks=_EVIDENCE)

    # Assert
    assert f"[리포트]\n{report}\n\n" in judge.user_prompt
    assert result.judge_checked_chars == JUDGE_MAX_REPORT_CHARS
    assert result.judge_total_chars == JUDGE_MAX_REPORT_CHARS


async def test_guard_output_cuts_report_after_window(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 창 바로 뒤에 표식
    overflow = "<<창 밖>>"
    report = DISCLAIMER + "가" * (JUDGE_MAX_REPORT_CHARS - len(DISCLAIMER)) + overflow
    judge = _install_judge(monkeypatch, {"failures": []})

    # Act
    result = await guard_output(report, run_judge=True, chunks=_EVIDENCE)

    # Assert: 앞 창만 들어갔고, 전체 길이와 본 길이로 검증 범위를 알 수 있다
    assert f"[리포트]\n{report[:JUDGE_MAX_REPORT_CHARS]}\n\n" in judge.user_prompt
    assert overflow not in judge.user_prompt
    assert result.judge_checked_chars == JUDGE_MAX_REPORT_CHARS
    assert result.judge_total_chars == JUDGE_MAX_REPORT_CHARS + len(overflow)


async def test_guard_output_honors_custom_report_window(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    judge = _install_judge(monkeypatch, {"failures": []})

    # Act
    result = await guard_output(
        "리포트 본문", run_judge=True, chunks=_EVIDENCE, max_report_chars=10
    )

    # Assert
    assert f"[리포트]\n{result.final_text[:10]}\n\n" in judge.user_prompt
    assert result.judge_checked_chars == 10
    assert result.judge_total_chars == len(result.final_text)


@pytest.mark.parametrize("max_report_chars", [0, -1])
async def test_guard_output_rejects_non_positive_report_window(
    monkeypatch: pytest.MonkeyPatch, max_report_chars: int
) -> None:
    # Arrange: 잘못 통과하더라도 실제 LLM을 부르지 않게 페이크를 깐다
    judge = _install_judge(monkeypatch, {"failures": []})

    # Act / Assert
    with pytest.raises(ValueError, match="max_report_chars"):
        await guard_output(
            "리포트 본문", run_judge=True, chunks=_EVIDENCE, max_report_chars=max_report_chars
        )
    assert judge.calls == []


async def test_guard_output_excludes_evidence_beyond_chunk_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    judge = _install_judge(monkeypatch, {"failures": []})

    # Act
    result = await guard_output("리포트 본문", run_judge=True, chunks=_EVIDENCE_OVER_LIMIT)

    # Assert: 20번째까지 들어가고 21번째는 빠진다
    assert _LAST_INCLUDED_EVIDENCE in judge.user_prompt
    assert _FIRST_EXCLUDED_EVIDENCE not in judge.user_prompt
    assert result.judge_evidence_chunks == JUDGE_MAX_EVIDENCE_CHUNKS


async def test_guard_output_cuts_each_evidence_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 청크 상한 바로 뒤에 표식(예전 상한 500자보다 긴 원문)
    kept = "나" * JUDGE_EVIDENCE_CHARS
    cut = "<<청크 밖>>"
    judge = _install_judge(monkeypatch, {"failures": []})

    # Act
    await guard_output("리포트 본문", run_judge=True, chunks=[{"text": kept + cut}])

    # Assert: 청크는 정확히 상한 글자에서 끝나고 바로 리포트가 이어진다
    assert f"{kept}\n\n[리포트]" in judge.user_prompt
    assert cut not in judge.user_prompt


async def test_guard_output_reads_evidence_text_leniently(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: text가 없는 dict·None인 dict·문자열 청크가 섞여 있다
    judge = _install_judge(monkeypatch, {"failures": []})
    chunks: list[object] = [{"source_ref": "[별표3]"}, {"text": None}, "제3조 원문"]

    # Act
    result = await guard_output("리포트 본문", run_judge=True, chunks=chunks)

    # Assert: 예외 없이 판정을 받는다(예외가 나면 고지문 없는 원본이 저장된다). None 원문은
    # 빈 문자열로 읽어 "None"이라는 글자를 근거처럼 Judge에 보내지 않는다
    assert "제3조 원문" in judge.user_prompt
    assert "None" not in judge.user_prompt
    assert result.judge_failures == []


async def test_guard_output_labels_evidence_as_mixed_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    judge = _install_judge(monkeypatch, {"failures": []})

    # Act
    await guard_output("리포트 본문", run_judge=True, chunks=_EVIDENCE)

    # Assert: 근거가 약관만이 아니다(장해분류표·판례가 함께 온다)
    assert "[근거 원문(약관·장해분류표·판례)]" in judge.user_prompt
    assert "[약관 원문]" not in judge.user_prompt
    assert "근거 원문(약관·장해분류표·판례)" in judge.system_prompt
    assert "약관 원문" not in judge.system_prompt


async def test_guard_output_keeps_disclaimer_when_judge_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: Judge 호출이 거절(또는 429·잘림)로 실패
    monkeypatch.setattr(ai_client, "chat_json", _refusing_judge)
    recorder = _install_log_recorder(monkeypatch)

    # Act
    result = await guard_output("리포트 본문", run_judge=True, chunks=[{"text": "약관 원문"}])

    # Assert: 고지문은 남고, 검증 미수행이 표식으로 기록된다
    assert result.final_text.startswith(f"> {DISCLAIMER}")
    assert result.judge_failures == [JUDGE_UNAVAILABLE]
    assert recorder.records == [("guardrail.judge.failed", {"error_type": "LlmRefusalError"})]


async def test_guard_output_reports_zero_scope_when_judge_call_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setattr(ai_client, "chat_json", _refusing_judge)

    # Act
    result = await guard_output("리포트 본문", run_judge=True, chunks=_EVIDENCE)

    # Assert: Judge가 본 범위는 0(검증 미수행), 전체 길이는 채운다
    assert result.judge_checked_chars == 0
    assert result.judge_evidence_chunks == 0
    assert result.judge_total_chars == len(result.final_text)


@pytest.mark.parametrize(("verdict", "reason"), _UNUSABLE_VERDICTS)
async def test_guard_output_treats_unusable_verdict_as_unavailable(
    monkeypatch: pytest.MonkeyPatch, verdict: object, reason: str
) -> None:
    # Arrange: 호출은 성공했지만 응답이 {"failures": [문장, ...]} 형식이 아니다
    _install_judge(monkeypatch, verdict)
    recorder = _install_log_recorder(monkeypatch)

    # Act
    result = await guard_output("리포트 본문", run_judge=True, chunks=_EVIDENCE)

    # Assert: "실패 없음 = 통과"로 읽지 않고 호출 실패와 똑같이 남긴다. 로그엔 분류만
    assert result.final_text.startswith(f"> {DISCLAIMER}")
    assert result.judge_failures == [JUDGE_UNAVAILABLE]
    assert result.judge_checked_chars == 0
    assert result.judge_evidence_chunks == 0
    assert result.judge_total_chars == len(result.final_text)
    assert recorder.records == [("guardrail.judge.unusable_verdict", {"reason": reason})]


# ── count_masked_pii: OCR 마스킹 토큰 건수 ───────────────────────
# PiiLabel 토큰을 한 번씩 이어 붙인 텍스트와 기대 건수. [면허번호]와 [운전면허번호]처럼 이름이
# 겹치는 라벨도 대괄호 덕분에 서로의 건수에 섞이지 않아야 한다.
_EVERY_LABEL_TOKEN_ONCE = "".join(label.token for label in PiiLabel)
_EVERY_LABEL_COUNTED_ONCE = {label.value: 1 for label in PiiLabel}


def test_count_masked_pii_counts_tokens_by_label() -> None:
    # Arrange
    text = "환자 성명: [이름], 보호자 성명: [이름], 주민번호 [주민등록번호], 연락처 [전화번호]"

    # Act
    counts = count_masked_pii(text)

    # Assert: 개수가 있는 라벨만, 한국어 라벨값을 키로
    assert counts == {"이름": 2, "주민등록번호": 1, "전화번호": 1}


def test_count_masked_pii_knows_every_ocr_label() -> None:
    assert count_masked_pii(_EVERY_LABEL_TOKEN_ONCE) == _EVERY_LABEL_COUNTED_ONCE


def test_count_masked_pii_keeps_overlapping_label_names_apart() -> None:
    assert count_masked_pii("면허 [운전면허번호] 의사 [면허번호] [운전면허번호]") == {
        "운전면허번호": 2,
        "면허번호": 1,
    }


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("", id="empty"),
        pytest.param(None, id="none"),
        pytest.param("진단명: 경추 염좌(S13.4) [별표3] [참고]", id="no-pii-tokens"),
    ],
)
def test_count_masked_pii_returns_empty_without_tokens(text: str | None) -> None:
    assert count_masked_pii(text) == {}


def test_count_masked_pii_counts_what_mask_pii_emits() -> None:
    # Arrange: 실제 OCR 규칙으로 가린 텍스트(토큰 형식이 갈라지면 여기서 깨진다)
    masked = mask_pii("환자 성명: 홍길동, 보호자 성명: 김영희, 주민번호 860312-1948571")

    # Act
    counts = count_masked_pii(masked)

    # Assert
    assert counts == {"이름": 2, "주민등록번호": 1}


# ── mask_pii: 외부 전송 전 마스킹(OCR 입력단 규칙 공유) ─────────────────

# 대표 유형별 (원문, 기대 토큰, 결과에서 사라져야 할 원문 조각).
_REPRESENTATIVE_PII = [
    pytest.param(
        "메일 hong.gildong@example.com 로 주세요",
        "[이메일]",
        "hong.gildong@example.com",
        id="email",
    ),
    pytest.param("카드 1234-5678-9012-3456 결제", "[카드번호]", "1234-5678-9012-3456", id="card"),
    pytest.param("환자 성명: 홍길동", "[이름]", "홍길동", id="labeled-name"),
    pytest.param(
        "주소는 서울특별시 강남구 테헤란로 123 101동 202호", "[주소]", "테헤란로", id="address"
    ),
    # 앞 6자리(생년월일)까지 사라져야 한다 — guard_input과 다른 점.
    pytest.param("주민번호 860312-1948571 입니다", "[주민등록번호]", "860312", id="rrn"),
    pytest.param("연락처 010-1234-5678", "[전화번호]", "010-1234-5678", id="mobile"),
    pytest.param(
        "보험금 받을 계좌는 110-234-567890 입니다",
        "[계좌번호]",
        "110-234-567890",
        id="keyword-account",
    ),
]

# OCR 마스커와 대조할 입력. 여러 유형이 섞인 라벨형 문장, 리포트 질문 같은 자유문,
# 앞뒤 공백·줄바꿈이 있는 여러 줄 입력(description류), 가리면 안 되는 임상 정보(화이트리스트)를
# 함께 둔다.
_OCR_PARITY_SAMPLES = [
    pytest.param(
        "환자 성명: 홍길동, 주민번호 860312-1948571, 연락처 010-1234-5678, "
        "메일 hong@example.com, 카드 1234-5678-9012-3456",
        id="mixed-labeled",
    ),
    pytest.param(
        "작년에 서울특별시 강남구 테헤란로 123에서 교통사고가 났어요. 보험금 받을 계좌는 "
        "110-234-567890이고 연락처는 010-9876-5432입니다. 저는 홍길동입니다.",
        id="free-text-question",
    ),
    pytest.param(
        "\n  사고 경위: 퇴근길 추돌사고\n연락처 010-1234-5678\n메일 hong@example.com\n",
        id="multiline-padded",
    ),
    pytest.param(
        "진단병원: 서울아산병원, 진단명: 경추 염좌(S13.4), 입원 2024-01-05",
        id="clinical-whitelist",
    ),
]


@pytest.mark.parametrize(("text", "token", "raw"), _REPRESENTATIVE_PII)
def test_mask_pii_masks_representative_types(text: str, token: str, raw: str) -> None:
    masked = mask_pii(text)

    assert token in masked
    assert raw not in masked


@pytest.mark.parametrize("text", _OCR_PARITY_SAMPLES)
def test_mask_pii_matches_ocr_masker(text: str) -> None:
    # Act
    ours = mask_pii(text)
    ocr = ocr_masking.mask(text)

    # Assert: 같은 입력 → 같은 출력(규칙이 갈라지면 한쪽이 PII를 흘린다)
    assert ours == ocr


@pytest.mark.parametrize("text", _OCR_PARITY_SAMPLES)
def test_mask_pii_is_idempotent_on_masked_text(text: str) -> None:
    # Arrange
    once = mask_pii(text)

    # Act
    twice = mask_pii(once)

    # Assert
    assert twice == once


def test_mask_pii_returns_empty_string_for_empty_input() -> None:
    assert mask_pii("") == ""


def test_mask_pii_hides_rrn_front6_unlike_guard_input_masking() -> None:
    # Arrange
    text = "주민번호 860312-1948571"

    # Act
    external = mask_pii(text)  # 외부 전송용(OCR 규칙)
    internal = _mask_pii(text)  # guard_input용(노션 06: 앞 6자리 보존)

    # Assert
    assert external == "주민번호 [주민등록번호]"
    assert internal == "주민번호 860312-*******"


def test_mask_pii_rejects_residual_pii_like_ocr_input_stage() -> None:
    # Arrange: 점으로 구분한 주민번호는 디텍터가 놓치고 잔류 게이트가 잡는다
    text = "주민번호 860312.1948571"

    # Act / Assert: OCR 입력단(mask → assert_no_residual)과 같은 판정으로 거절한다
    with pytest.raises(MaskingError) as ours:
        mask_pii(text)
    with pytest.raises(MaskingError):
        ocr_masking.assert_no_residual(ocr_masking.mask(text))
    assert "1948571" not in str(ours.value)  # 메시지에 원문 없음(§9)


@pytest.mark.parametrize("text", ["홍길동입니다", "저는 홍길동이고 어머니는 김영희입니다"])
def test_mask_pii_without_ner_keeps_unlabeled_names(text: str) -> None:
    # 한계 고정: NER 없는 운영 report에서는 라벨 없는 자유문 속 이름이 그대로 나간다.
    # 이 테스트가 깨지면 src/guardrail/README.md "mask_pii 한계"도 함께 고친다.
    assert mask_pii(text) == text


class _FakeNerDetector:
    """transformers 없이 NER 경로를 확인하는 페이크. "저는 홍길동입니다"의 '홍길동' 위치를 준다."""

    def __init__(self, model_name: str, score_threshold: float, *, batch_size: int = 16) -> None:
        self.model_name = model_name
        self.score_threshold = score_threshold
        self.batch_size = batch_size

    def detect(self, text: str) -> list[Span]:
        return [Span(3, 6, PiiLabel.NAME, source="ner", score=0.99)]


def test_mask_pii_adds_ner_when_settings_enable_it(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: settings.use_ner=True로 기본 마스커를 새로 만들게 하고, NER 모델은 페이크로 바꾼다
    monkeypatch.setattr(get_settings(), "use_ner", True)
    monkeypatch.setattr(masker_module, "_default_masker", None)
    monkeypatch.setattr(ner_detector_module, "NerDetector", _FakeNerDetector)

    # Act
    masked = mask_pii("저는 홍길동입니다")

    # Assert
    assert masked == "저는 [이름]입니다"


# report 이미지(`uv sync --no-dev --extra report`)에 없는 패키지를 하위 프로세스에서 import하지
# 못하게 막고 mask_pii를 부른다. dev venv에는 pillow가 있어 평범한 import로는 회귀를 못 잡는다.
# ocr_worker.masking의 import 체인(image_masker → ocr → storage 등)에 이 패키지가 최상위
# import로 들어오면 report 워커가 기동부터 실패한다. OCR 엔진(paddleocr·paddle)도 ocr.py에서
# lazy import여야 한다.
_REPORT_IMAGE_SMOKE = """
import importlib.abc
import sys

ABSENT_IN_REPORT_IMAGE = {
    "PIL", "torch", "transformers", "surya", "fitz", "cv2", "sentence_transformers",
    "paddle", "paddleocr", "paddlex",
}


class _BlockAbsent(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.partition(".")[0] in ABSENT_IN_REPORT_IMAGE:
            raise ModuleNotFoundError(f"absent in report image: {fullname}")
        return None


sys.meta_path.insert(0, _BlockAbsent())

from guardrail import mask_pii

print(mask_pii("환자 성명: 홍길동, 주민번호 860312-1948571"))
"""


def test_mask_pii_imports_and_runs_without_report_absent_packages(tmp_path: Path) -> None:
    # Arrange: 운영 report 설정(USE_NER=false). cwd를 레포 밖에 둬 .env를 읽지 않게 한다.
    env = {
        **os.environ,
        "PYTHONPATH": str(_SRC_DIR),
        "PYTHONIOENCODING": "utf-8",
        "USE_NER": "false",
    }

    # Act — 고정 스크립트를 현재 인터프리터로 실행한다(외부 입력 없음).
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _REPORT_IMAGE_SMOKE],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=tmp_path,
        timeout=60,
        check=False,
    )

    # Assert
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "환자 성명: [이름], 주민번호 [주민등록번호]"
