"""report_compose의 본문·쟁점 동시 호출 헬퍼(성능 실험 R1) — 실패 시 형제 호출 취소·예외 보존."""

from __future__ import annotations

import asyncio

import pytest

from core import ai_client
from report_worker.nodes import agents


async def test_returns_results_in_call_order() -> None:
    # Arrange: 늦게 끝나는 쪽이 먼저 넘겨진다
    async def slow() -> str:
        await asyncio.sleep(0.02)
        return "본문"

    async def fast() -> dict[str, list[str]]:
        return {"issues": []}

    # Act
    body, issues = await agents._gather_or_cancel(slow(), fast())

    # Assert: 완료 순서가 아니라 넘긴 순서대로 돌려준다
    assert (body, issues) == ("본문", {"issues": []})


async def test_failure_cancels_sibling_and_reraises_original_error() -> None:
    # Arrange: 쟁점 호출이 실패하는 동안 본문 호출은 아직 응답을 기다린다
    sibling_cancelled = asyncio.Event()

    async def waiting_body() -> str:
        try:
            await asyncio.Event().wait()  # 끝나지 않는 응답
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise
        return "도달하지 않는다"

    async def failing_issues() -> None:
        await asyncio.sleep(0)
        raise ai_client.AiClientError("chat 호출 실패")

    # Act
    with pytest.raises(ai_client.AiClientError):
        await agents._gather_or_cancel(waiting_body(), failing_issues())

    # Assert: 결과를 버릴 형제 호출은 취소된다(과금되는 LLM 호출을 끝까지 두지 않는다).
    # 예외는 ExceptionGroup이 아니라 원래 타입 그대로라 safe_node의 llm_failure 분류가 유지된다
    assert sibling_cancelled.is_set()
