"""ReportJob.ocr_quality == needs_reupload 시 리포트 생성을 건너뛰는지 검증."""

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


async def test_needs_reupload_skips_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 그래프가 호출되면 실패하는 스텁
    async def _must_not_run(_state: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("needs_reupload 잡에서 그래프가 실행되면 안 된다")

    monkeypatch.setattr(worker, "_graph", SimpleNamespace(ainvoke=_must_not_run))

    # Act / Assert: 예외 없이 조용히 반환(→오프셋 커밋)
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
