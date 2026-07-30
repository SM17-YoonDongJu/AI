"""벤치마크 계측 — ai_client 래핑(호출별 지연 기록) + Judge 모델 고정 + 노드별 타이밍.

production 코드(impl)는 건드리지 않는다. `core.ai_client`의 chat/chat_json/embed를 런타임에
감싸(monkeypatch) 호출마다 지연을 기록하고, 출력 가드레일의 LLM Judge 호출만 고정 레퍼런스
모델로 라우팅해 자기채점 편향을 없앤다. 노드별 시간은 LangGraph `astream(updates)`로 잰다.

왜 프롬프트 시그니처로 Judge를 식별하나: `guards.guard_output`은 chat_json을 model 지정 없이
호출(=settings.llm_model)하므로, 후보 모델을 바꾸면 채점자도 같이 바뀐다. 시스템 프롬프트의
고정 문구로 Judge 호출만 걸러 별도 모델로 보낸다(guards.py와 문구가 일치해야 한다).

정밀 tok/s는 여기서 재지 않는다(chat()이 usage를 버림) — `speed_probe`가 usage 기반으로 측정.
여기서는 실제 파이프라인의 지연(사용자 체감값)과 호출 구조를 기록한다.
"""

from __future__ import annotations

import contextlib
import contextvars
import time
from dataclasses import dataclass, field
from typing import Any

from core import ai_client
from core.config import settings

# guards.guard_output의 Judge 시스템 프롬프트 고정 문구(식별용). guards.py와 반드시 일치.
JUDGE_SIGNATURE = "너는 보험 리포트 검증관이다"


@dataclass(slots=True)
class CallRecord:
    """단일 LLM 호출 기록. 정밀 토큰 대신 output_chars 프록시(정밀치는 speed_probe)."""

    kind: str  # "chat" | "chat_json" | "embed"
    model: str
    latency_s: float
    t_start: float  # perf_counter 기준(노드 귀속 계산용)
    t_end: float
    ok: bool
    output_chars: int
    is_judge: bool = False


@dataclass(slots=True)
class RunTelemetry:
    """리포트 1건 실행 동안 수집된 호출 기록."""

    calls: list[CallRecord] = field(default_factory=list)


# 실행별 텔레메트리(동시 실행/스레드 안전). 래퍼는 여기에 기록한다.
_current: contextvars.ContextVar[RunTelemetry | None] = contextvars.ContextVar(
    "bench_telemetry", default=None
)
_judge_model: str | None = None
_installed = False
_orig: dict[str, Any] = {}


def _is_judge_call(args: tuple, kwargs: dict[str, Any]) -> bool:
    """Judge 호출인지 시스템 프롬프트 첫 메시지로 판별한다."""
    messages = args[0] if args else kwargs.get("messages")
    if not messages or not isinstance(messages[0], dict):
        return False
    return JUDGE_SIGNATURE in messages[0].get("content", "")


def _wrap_chat(orig: Any, kind: str) -> Any:
    """chat/chat_json 래퍼 — 지연 기록 + (해당 시) Judge 모델 고정."""

    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        judge = _is_judge_call(args, kwargs)
        if judge and _judge_model:
            kwargs.setdefault("model", _judge_model)  # Judge만 고정 레퍼런스로
        model = kwargs.get("model") or settings.llm_model
        t0 = time.perf_counter()
        ok = True
        result: Any = ""
        try:
            result = await orig(*args, **kwargs)
            return result
        except Exception:  # 계측 경계: 실패도 기록해야 하므로 잡고 즉시 재발생
            ok = False
            raise
        finally:
            t1 = time.perf_counter()
            tel = _current.get()
            if tel is not None:
                chars = len(result) if isinstance(result, str) else len(str(result))
                tel.calls.append(CallRecord(kind, model, t1 - t0, t0, t1, ok, chars, judge))

    return wrapper


def _wrap_embed(orig: Any) -> Any:
    """embed 래퍼 — 지연 기록(검색 임베딩 비용)."""

    async def wrapper(text: str) -> Any:
        model = settings.embedding_model
        t0 = time.perf_counter()
        ok = True
        try:
            return await orig(text)
        except Exception:
            ok = False
            raise
        finally:
            t1 = time.perf_counter()
            tel = _current.get()
            if tel is not None:
                tel.calls.append(CallRecord("embed", model, t1 - t0, t0, t1, ok, 0))

    return wrapper


def install(judge_model: str | None = None) -> None:
    """ai_client.chat/chat_json/embed를 계측 래퍼로 교체한다(1회).

    Args:
        judge_model: 출력 가드레일 LLM Judge를 고정할 레퍼런스 모델. None이면 고정하지 않음
            (후보 모델이 자기 리포트를 채점 — 편향 측정용).
    """
    global _installed, _judge_model
    _judge_model = judge_model
    if _installed:
        return
    _orig["chat"] = ai_client.chat
    _orig["chat_json"] = ai_client.chat_json
    _orig["embed"] = ai_client.embed
    ai_client.chat = _wrap_chat(_orig["chat"], "chat")
    ai_client.chat_json = _wrap_chat(_orig["chat_json"], "chat_json")
    ai_client.embed = _wrap_embed(_orig["embed"])
    _installed = True


def uninstall() -> None:
    """원본 ai_client 함수를 복원한다."""
    global _installed
    if not _installed:
        return
    ai_client.chat = _orig["chat"]
    ai_client.chat_json = _orig["chat_json"]
    ai_client.embed = _orig["embed"]
    _installed = False


@contextlib.contextmanager
def record_run() -> Any:
    """리포트 1건 실행 동안의 호출을 수집하는 컨텍스트. `with record_run() as tel:`."""
    tel = RunTelemetry()
    token = _current.set(tel)
    try:
        yield tel
    finally:
        _current.reset(token)


async def astream_timed(
    app: Any, job: dict[str, Any]
) -> tuple[dict[str, Any], list[tuple[str, float]]]:
    """`astream(updates)`로 노드별 시간을 재며 최종 상태를 재구성한다.

    ReportState는 reducer가 없어(순차·덮어쓰기, state.py) 델타를 dict.update로 누적하면
    ainvoke와 동일한 최종 상태가 된다. 한 번의 실행으로 노드 타이밍과 최종 상태를 함께 얻는다.

    Args:
        app: 컴파일된 LangGraph 앱(build_graph()).
        job: 초기 상태(report_id·ocr_result_id·claim_id·user_ref·doc_type).

    Returns:
        (최종 상태 dict, [(노드명, 소요초)] 실행 순서).
    """
    state: dict[str, Any] = {}
    timings: list[tuple[str, float]] = []
    t_prev = time.perf_counter()
    async for update in app.astream(job, stream_mode="updates"):
        t_now = time.perf_counter()
        for node, delta in update.items():
            if delta:
                state.update(delta)
            timings.append((node, t_now - t_prev))
        t_prev = t_now
    return state, timings


def attribute_calls_to_nodes(
    calls: list[CallRecord], timings: list[tuple[str, float]], t_stream_start: float
) -> dict[str, list[CallRecord]]:
    """호출 시각(t_start)을 노드 실행 구간에 귀속시킨다(노드별 지연 분해용).

    Args:
        calls: 수집된 호출 기록.
        timings: astream_timed가 반환한 [(노드명, 소요초)].
        t_stream_start: astream 시작 시각(perf_counter). 첫 노드 구간의 하한.

    Returns:
        노드명 → 그 구간에 속한 호출들. 어느 구간에도 안 들면 "_unattributed".
    """
    windows: list[tuple[str, float, float]] = []
    lo = t_stream_start
    for node, dur in timings:
        hi = lo + dur
        windows.append((node, lo, hi))
        lo = hi
    out: dict[str, list[CallRecord]] = {}
    for call in calls:
        placed = "_unattributed"
        for node, w_lo, w_hi in windows:
            if w_lo <= call.t_start < w_hi:
                placed = node
                break
        out.setdefault(placed, []).append(call)
    return out
