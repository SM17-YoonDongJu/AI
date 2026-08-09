"""guardrail 3단계 테스트 — PII 마스킹·도메인 차단·단정금액 치환·고지문 삽입.

LLM Judge(run_judge=True)는 ai_client에 의존하므로 여기서는 run_judge=False로
LLM 없이 검증한다(Judge 경로는 e2e에서 확인).
"""

from core.contracts import InputGuardResult, OutputGuardResult
from guardrail import DISCLAIMER, guard_generation, guard_input, guard_output
from guardrail.guards import _mask_pii


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
