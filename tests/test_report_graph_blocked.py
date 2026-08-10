"""리포트 그래프 차단 경로 테스트 (LLM/PG 없이 페이크로 격리).

input_guardrail이 blocked를 반환하면 (a) diagnosis 등 LLM 노드가 호출되지 않고,
(b) persist_blocked가 reports.status='BLOCKED' UPDATE만 수행하며 report_drafts 초안은
만들지 않음을 검증한다. 페이크 DB 풀·가드레일로 외부 의존을 끊는다.
"""

import uuid

from core.contracts import InputGuardResult
from report_worker.graph import build_graph
from report_worker.nodes import agents


class _FakeConn:
    def __init__(self, executed: list[tuple[str, tuple]]) -> None:
        self.executed = executed

    async def fetchrow(self, query: str, *args: object) -> dict | None:
        if "ocr_results" in query:
            return {"masked_text": "도메인 외 질문 원문", "entities": None}
        if "FROM reports" in query:
            return {
                "accident_type": "traffic",
                "treatment": None,
                "offered_amount": 0,
                "question": None,
                "claim_id": None,
            }
        return None  # user_insurances 등

    async def fetchval(self, query: str, *args: object) -> int:
        return 0

    async def execute(self, query: str, *args: object) -> None:
        self.executed.append((query, args))


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePool:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self._conn = _FakeConn(self.executed)

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._conn)


async def _blocked_guard_input(text: str) -> InputGuardResult:
    return InputGuardResult(masked_text=text, blocked=True, reason="off_domain")


async def _forbidden_llm(*args: object, **kwargs: object):
    raise AssertionError("차단 경로에서 LLM 노드가 호출되면 안 된다")


async def test_blocked_path_skips_llm_and_persists_blocked_status(monkeypatch) -> None:
    # Arrange
    pool = _FakePool()
    monkeypatch.setattr(agents.db, "get_pool", lambda: pool)
    monkeypatch.setattr(agents.guardrail, "guard_input", _blocked_guard_input)
    monkeypatch.setattr(agents.ai_client, "chat", _forbidden_llm)
    monkeypatch.setattr(agents.ai_client, "chat_json", _forbidden_llm)

    report_id = str(uuid.uuid4())
    state = {
        "report_id": report_id,
        "ocr_result_id": str(uuid.uuid4()),
        "claim_id": None,
        "user_ref": "u-1",
        "doc_type": "medical",
    }

    # Act
    final = await build_graph().ainvoke(state)

    # Assert: 차단 사유가 errors에 기록됨 (LLM 미호출은 _forbidden_llm이 보증)
    assert any(str(e).startswith("input_blocked") for e in final.get("errors", []))

    # Assert: reports.status='BLOCKED' UPDATE 1회 수행, 초안(report_drafts)은 미생성
    assert any("status = 'BLOCKED'" in q for q, _ in pool.executed)
    assert not any("report_drafts" in q for q, _ in pool.executed)
    assert not any("report_issues" in q for q, _ in pool.executed)
