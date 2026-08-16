"""ReportJob.ocr_quality == needs_reupload 시 리포트 생성을 건너뛰고
reports.status='NEEDS_REUPLOAD'로 Backend에 알리는지 검증."""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from core.contracts import DocType, ReportJob
from report_worker import worker


def _job(**overrides: Any) -> ReportJob:
    base: dict[str, Any] = {
        "report_id": "r-1",
        "ocr_result_id": "o-1",
        "job_id": "j-1",
        "doc_type": DocType.DIAGNOSIS,
        "user_ref": "u-1",
        "created_at": datetime.now(UTC),
    }
    base.update(overrides)
    return ReportJob(**base)


async def test_needs_reupload_skips_graph_and_marks_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 그래프가 호출되면 실패하는 스텁 + mark_needs_reupload 호출 캡처
    async def _must_not_run(_state: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("needs_reupload 잡에서 그래프가 실행되면 안 된다")

    monkeypatch.setattr(worker, "_graph", SimpleNamespace(ainvoke=_must_not_run))

    marked: list[str] = []

    async def _mark(report_id: str) -> None:
        marked.append(report_id)

    monkeypatch.setattr(worker, "mark_needs_reupload", _mark)

    # Act
    await worker.handle_job(_job(ocr_quality="needs_reupload"))

    # Assert: 그래프는 안 태우고 status 갱신은 호출됨(→ 예외 없이 커밋)
    assert marked == ["r-1"]


async def test_needs_reupload_mark_failure_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: status 갱신(DB 쓰기)이 실패하는 상황을 흉내
    async def _fail(_report_id: str) -> None:
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(worker, "mark_needs_reupload", _fail)

    # Act / Assert: 삼키지 않고 그대로 올라가야 SQS가 재전달한다(무음 실패 방지)
    with pytest.raises(RuntimeError):
        await worker.handle_job(_job(ocr_quality="needs_reupload"))


async def test_ok_quality_runs_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    seen: dict[str, Any] = {}

    async def _capture(state: dict[str, Any]) -> dict[str, Any]:
        seen.update(state)
        return {"errors": []}

    monkeypatch.setattr(worker, "_graph", SimpleNamespace(ainvoke=_capture))

    # Act
    await worker.handle_job(_job())

    # Assert: 기본 ocr_quality("ok")는 그래프를 태운다
    assert seen["report_id"] == "r-1"
