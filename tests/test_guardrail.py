"""guardrail 테스트 — PII 마스킹(mask_pii·guard_input)·도메인 차단·단정금액 치환·고지문 삽입.

LLM Judge(run_judge=True)는 ai_client에 의존하므로 여기서는 run_judge=False로
LLM 없이 검증한다(Judge 경로는 e2e에서 확인).

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
    MaskingError,
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


async def test_guard_input_blocks_off_domain() -> None:
    result = await guard_input("비트코인 시세랑 부동산 투자 문의입니다.")
    assert isinstance(result, InputGuardResult)
    assert result.blocked is True
    assert result.reason is not None


async def test_guard_input_allows_domain_and_masks() -> None:
    result = await guard_input("진단명 경추 염좌, 환자 860312-1948571")
    assert result.blocked is False
    assert "860312-*******" in result.masked_text


def test_guard_generation_softens_absolute_amount() -> None:
    out = guard_generation("보험금 1,200만원을 받습니다.")
    assert "참고 추정 범위" in out
    assert "받습니다" not in out


async def test_guard_output_inserts_disclaimer_without_judge() -> None:
    result = await guard_output("리포트 본문", run_judge=False, chunks=None)
    assert isinstance(result, OutputGuardResult)
    assert result.final_text.startswith(f"> {DISCLAIMER}")
    assert result.judge_failures == []


async def test_guard_output_keeps_disclaimer_when_judge_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: Judge 호출이 거절(또는 429·잘림)로 실패
    async def failing_judge(messages: list[dict[str, str]], **opts: object) -> object:
        raise ai_client.LlmRefusalError("거절")

    records: list[tuple[str, dict[str, object]]] = []

    class _Recorder:
        def warning(self, event: str, **fields: object) -> None:
            records.append((event, fields))

    monkeypatch.setattr(ai_client, "chat_json", failing_judge)
    monkeypatch.setattr(guards, "logger", _Recorder())

    # Act
    result = await guard_output("리포트 본문", run_judge=True, chunks=[{"text": "약관 원문"}])

    # Assert: 고지문은 남고, 검증 미수행이 표식으로 기록된다
    assert result.final_text.startswith(f"> {DISCLAIMER}")
    assert result.judge_failures == [JUDGE_UNAVAILABLE]
    assert records == [("guardrail.judge.failed", {"error_type": "LlmRefusalError"})]


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

    def __init__(self, model_name: str, score_threshold: float) -> None:
        self.model_name = model_name
        self.score_threshold = score_threshold

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
