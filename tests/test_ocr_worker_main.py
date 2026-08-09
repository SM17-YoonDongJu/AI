"""OCR 워커 진입점의 삭제 outbox 스윕 루프 테스트 (``ocr_worker.__main__``).

스윕 루프는 "원본이 조용히 남는" 경로를 닫는 **유일한 재시도 수단**이라, 루프가 죽거나
종료 때 어중간한 상태를 남기면 그 보증이 통째로 사라진다. Kafka·DB·S3 없이 파이프라인을
페이크로 주입해 루프 자체의 계약만 고정한다:

- 설정 간격(``ocr_delete_retry_interval_seconds``)대로 사이클을 돈다.
- 한 사이클이 예외로 끝나도 루프는 계속 돈다(§8 top-loop 격리).
- ``stopping`` 이벤트로 대기 중에도 즉시 깨어나 종료한다(SIGTERM 유예 안에 끝나야 한다).
- 사이클 진행 중(S3 왕복 중) 취소는 **삼키지 않고** 전파된다 — 취소를 흡수한 채 루프를
  계속 돌면 종료가 지연되고, 취소 시점의 행은 outbox에 ``pending``으로 남아야 한다
  (기록 없이 취소되는지는 ``test_pipeline`` 쪽 스윕 취소 테스트가 본다).
"""

import asyncio
from typing import Any

import pytest

from core.config import Settings
from ocr_worker import __main__ as ocr_main

# 루프가 멈추지 않는 회귀가 나도 테스트가 영원히 매달리지 않도록 두는 상한(초).
# 정상 경로는 이벤트로 즉시 깨어나므로 벽시계 시간을 쓰지 않는다.
_TIMEOUT_S = 5.0


class _FakeSweepPipeline:
    """``sweep_pending_deletes``만 흉내내는 파이프라인 대역."""

    def __init__(
        self,
        *,
        errors: list[Exception | None] | None = None,
        gate: asyncio.Event | None = None,
    ) -> None:
        self.calls = 0
        self.started = asyncio.Event()  # 사이클에 진입했는가(취소 타이밍 제어용)
        self._errors = list(errors or [])
        self._gate = gate

    async def sweep_pending_deletes(self, *, batch_size: int = 50) -> int:
        self.calls += 1
        self.started.set()
        if self._gate is not None:
            await self._gate.wait()  # S3 왕복 중을 흉내 — 여기서 취소당한다
        await asyncio.sleep(0)  # 실제 I/O처럼 루프에 양보한다
        error = self._errors.pop(0) if self._errors else None
        if error is not None:
            raise error
        return 0


async def test_sweep_loop_uses_configured_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 대기를 페이크로 갈아끼워 벽시계 시간 없이 간격 값만 관찰한다.
    intervals: list[float] = []
    pipeline = _FakeSweepPipeline()
    stopping = asyncio.Event()

    async def fake_sleep(event: asyncio.Event, seconds: float) -> None:
        intervals.append(seconds)
        if len(intervals) >= 3:
            event.set()  # 3사이클 뒤 종료

    monkeypatch.setattr(ocr_main, "_sleep_or_stop", fake_sleep)
    settings = Settings(ocr_delete_retry_interval_seconds=900.0)

    # Act
    async with asyncio.timeout(_TIMEOUT_S):
        await ocr_main._run_delete_sweep(pipeline, settings, stopping)  # type: ignore[arg-type]

    # Assert: 스윕 주기 = 재시도 백오프 간격(같은 설정값)이어야 한다.
    assert intervals == [900.0, 900.0, 900.0]
    assert pipeline.calls == 3


async def test_sweep_loop_survives_cycle_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 첫 사이클이 DB 오류로 죽는다. 여기서 루프가 멈추면 재시도 경로 자체가
    # 사라지고(원본이 영영 남는다) 프로세스는 멀쩡해 보여 아무도 눈치채지 못한다.
    pipeline = _FakeSweepPipeline(errors=[RuntimeError("일시적 DB 오류"), None, None])
    stopping = asyncio.Event()
    cycles = 0

    async def fake_sleep(event: asyncio.Event, seconds: float) -> None:
        nonlocal cycles
        cycles += 1
        if cycles >= 3:
            event.set()

    monkeypatch.setattr(ocr_main, "_sleep_or_stop", fake_sleep)

    # Act
    async with asyncio.timeout(_TIMEOUT_S):
        await ocr_main._run_delete_sweep(pipeline, Settings(), stopping)  # type: ignore[arg-type]

    # Assert: 예외 사이클 이후에도 계속 돌았다.
    assert pipeline.calls == 3


async def test_sweep_loop_exits_immediately_when_stopping_is_set() -> None:
    # Arrange: 실제 대기(_sleep_or_stop)를 쓰되 간격을 아주 길게 잡는다 — 종료 신호로
    # 깨어나지 못하면 SIGTERM→SIGKILL 유예(k8s 기본 30초)를 넘겨 강제 종료된다.
    pipeline = _FakeSweepPipeline()
    stopping = asyncio.Event()
    settings = Settings(ocr_delete_retry_interval_seconds=3600.0)
    task = asyncio.create_task(
        ocr_main._run_delete_sweep(pipeline, settings, stopping)  # type: ignore[arg-type]
    )
    await pipeline.started.wait()  # 첫 사이클을 마치고 대기에 들어갈 때까지

    # Act
    stopping.set()

    # Assert: 취소 없이도 스스로 끝난다(대기 중 깨어남 + while 조건 재확인).
    async with asyncio.timeout(_TIMEOUT_S):
        await task
    assert pipeline.calls == 1


async def test_sweep_loop_cancellation_is_not_swallowed() -> None:
    # Arrange: 종료 시 진행 중인 사이클을 취소하는 경로(__main__의 finally). 사이클
    # 예외 격리(except Exception)가 CancelledError까지 먹으면 루프가 계속 돌아 종료가
    # 지연된다 — CancelledError는 BaseException이라 잡히지 않아야 한다.
    gate = asyncio.Event()  # 열지 않는다 — 사이클을 진행 중 상태로 붙잡아 둔다.
    pipeline = _FakeSweepPipeline(gate=gate)
    stopping = asyncio.Event()
    task = asyncio.create_task(
        ocr_main._run_delete_sweep(pipeline, Settings(), stopping)  # type: ignore[arg-type]
    )
    await pipeline.started.wait()

    # Act: __main__의 종료 순서(stopping → cancel → await)를 그대로 흉내낸다.
    stopping.set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        async with asyncio.timeout(_TIMEOUT_S):
            await task

    # Assert: 취소된 사이클을 다시 돌지 않았다(다음 기동이 pending 행을 이어받는다).
    assert task.cancelled()
    assert pipeline.calls == 1


async def test_sleep_or_stop_wakes_immediately_when_stopping_is_set() -> None:
    # Arrange
    stopping = asyncio.Event()
    stopping.set()

    # Act / Assert: 긴 간격이어도 즉시 반환한다.
    async with asyncio.timeout(_TIMEOUT_S):
        await ocr_main._sleep_or_stop(stopping, 3600.0)


async def test_sleep_or_stop_returns_quietly_after_interval() -> None:
    # Arrange: 정상 간격 경과 경로 — TimeoutError를 밖으로 흘리면 루프가 죽는다.
    stopping = asyncio.Event()

    # Act
    await ocr_main._sleep_or_stop(stopping, 0.001)

    # Assert
    assert not stopping.is_set()


def test_sweep_loop_is_wired_into_worker_startup() -> None:
    # Arrange / Act: 배선 자체(진입점이 스윕 task를 띄우고 종료 시 정리하는지)는
    # Kafka·DB 없이 실행할 수 없어, 소스에 계약이 남아 있는지로 최소 회귀만 막는다.
    source = _run_source()

    # Assert
    assert "_run_delete_sweep" in source  # 소비 루프와 병행해 띄운다
    assert "sweep_task.cancel()" in source  # 종료 시 정리
    assert "wait_for_pending_deletes" in source  # 기존 즉시 삭제 흡수도 유지


def _run_source() -> str:
    """``__main__._run``의 소스(배선 회귀 확인용)."""
    import inspect

    source: Any = inspect.getsource(ocr_main._run)
    return str(source)
