"""리포트 실패 단락 테스트 — 실패한 리포트는 저장하지 않는다 (DB·LLM·RAG 페이크로 격리).

여기서 고정하는 계약:
  (a) 컨텍스트 하드 실패(load_context_failed·ocr_result_missing)면 load_context 직후 끝난다.
      LLM·검색·DB 쓰기가 0회이고, worker는 재전달(ReportWorkerError)한다.
  (b) 노드 안의 LLM 시스템 실패(AiClientError — 429 등)면 첫 실패에서 끝난다. 이후 LLM 호출과
      저장이 없고 worker는 재전달한다. 몇 번째 LLM 호출에서 실패하든 같다.
  (c) 거절(LlmRefusalError)이면 초안 없이 reports.status='BLOCKED'만 쓴다. worker는
      NonRetryableError로 즉시 종결(ack)한다. 상태 기록이 실패하면 재전달한다.
  (d) Judge 실패(시스템 실패·거절)와 콘텐츠 부분 실패(rag_empty 등)는 저장을 막지 않는다.
  (e) 종료 노드를 뺀 모든 노드의 간선에 단락이 걸려 있다(구조).
  (f) 운영 핸들러(journaled_handler)·poison 훅으로 돌리면 위 실패가 실패 저널
      (ai.report_job_failures)에 분류대로 남고, poison 직전 terminal로 확정된다. 거절과 저장된
      리포트는 저널에 남지 않는다(이전 시도의 행은 지운다).

429·거절은 실제 anthropic SDK에 httpx2.MockTransport를 꽂아 재현한다. SDK 예외가 AiClientError로
바뀌어 그래프가 멈추는 운영 경로를 그대로 탄다. 네트워크 호출은 없다.
"""

import copy
import datetime
import json
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import anthropic
import httpx2
import pytest

from core import ai_client
from core.config import Settings, settings
from core.contracts import DocType, ReportJob
from core.exceptions import NonRetryableError
from core.sqs.consumer import SqsConsumer
from guardrail.guards import JUDGE_UNAVAILABLE
from report_worker import __main__ as report_main
from report_worker import failure_journal, worker
from report_worker.failure_journal import FailureClass
from report_worker.graph import TERMINAL_NODES, build_graph
from report_worker.nodes import agents

_QUEUE = "https://sqs.ap-northeast-2.amazonaws.com/123456789012/report-job"
_MODEL = "claude-opus-5-5"

# 장해 경로까지 전부 타는 성공 응답. 노드마다 자기 키만 읽는다(test_report_pii_masking과 같은 구성).
_LLM_JSON: dict[str, Any] = {
    "diagnosis": "요추 염좌",
    "icd_codes": ["S33.5"],
    "accident_type": "medical_indemnity",
    "surgery": False,
    "hospitalization": True,
    "requires_disability_review": True,
    "applicable": ["상해입원일당"],
    "missing": ["상해후유장해"],
    "analysis": "면책 사유 없음",
    "relevant": [0],
    "items": [
        {
            "injury": "요추 운동장해",
            "body_region": "척추",
            "laterality": "none",
            "category_label": "척추에 약간의 운동장해를 남긴 때",
            "rate": 10,
            "rate_quote": "지급률 10%",
            "temporary": False,
            "temporary_years": None,
            "citation": "[별표3, 행복보험]",
        }
    ],
    "uncertain": False,
    "notes": "",
    "reviews": [{"idx": 0, "appropriate": True, "suggested_rate": None, "reason": "적정"}],
    "issues": [{"title": "입원일당", "description": "입원일수 확인", "ai_status": "INFO"}],
    "failures": [],
}
# 성공 경로의 LLM 호출 순서(시스템 프롬프트로 구분). 마지막은 출력 가드레일 Judge다.
_LLM_CALL_ORDER = (
    ("diagnosis", "진단 분석가"),
    ("coverage_analysis", "약관 분석가"),
    ("case_search", "판례 선별가"),
    ("disability_rag", "장해분류표 분석가"),
    ("disability_verify", "장해평가 감수자"),
    ("report_compose", "리포트 작성자"),
    ("report_compose", "핵심 쟁점"),
    ("output_guardrail", "검증관"),
)
_TERMS_CHUNK: dict[str, Any] = {
    "text": "장해의 분류 척추에 약간의 운동장해를 남긴 때 지급률 10%",
    "namespace": "terms",
    "score": 1.0,
    "source_ref": "[별표3, 행복보험]",
    "chunk_id": "terms-1",
    "article_number": "별표3",
    "product_name": "행복보험",
    "chunk_type": "schedule",
}
_CASE_CHUNK: dict[str, Any] = {
    "text": "요추 염좌 후유장해 지급 분쟁 사례",
    "namespace": "case",
    "score": 1.0,
    "source_ref": "[2020-123, ]",
    "chunk_id": "case-1",
    "article_number": None,
    "product_name": None,
    "chunk_type": "case",
}


# ── DB 페이크 ─────────────────────────────────────────────────
class _NullAsyncContext:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeConn:
    """load_context·policy_in_db·case_search·persist·persist_refused가 쓰는 쿼리만 흉내 낸다."""

    def __init__(self, *, ocr_row: dict[str, Any] | None) -> None:
        self.ocr_row = ocr_row
        self.read_error: Exception | None = None  # 읽기 실패(load_context 예외) 재현용
        self.write_error: Exception | None = None  # 쓰기 실패(상태 기록 실패) 재현용
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        return []  # 청구 문서 없음(단건 경로) · 판례 메타 없음

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        if self.read_error is not None:
            raise self.read_error
        if "FROM ocr_results" in query:
            return self.ocr_row
        # user_insurances 쿼리는 상관 서브쿼리에 "FROM reports"를 포함하므로 먼저 거른다.
        if "FROM user_insurances" in query:
            return {
                "insurer_name": "행복화재",
                "product_name": "행복보험",
                "coverages": json.dumps(["상해입원일당", "상해후유장해"], ensure_ascii=False),
                "enrolled_at": datetime.date(2020, 3, 1),
            }
        if "FROM reports" in query:
            return {
                "accident_type": "medical_indemnity",
                "treatment": "요추 염좌",
                "offered_amount": 1_000_000,
                "question": None,
                "claim_id": None,
            }
        return None

    async def fetchval(self, query: str, *args: object) -> int:
        return 1  # policy_in_db: 가입 약관이 policy_chunks에 있다 → coverage_parse 경로

    async def execute(self, query: str, *args: object) -> None:
        if self.write_error is not None:
            raise self.write_error
        self.executed.append((query, args))

    def transaction(self) -> _NullAsyncContext:
        return _NullAsyncContext()


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._conn)


async def _fake_pii_dek(*args: object, **kwargs: object) -> bytes:
    """페이크 DB 컬럼은 전부 평문이라 실제 복호화는 일어나지 않는다."""
    return b"\x00" * 32


def _install_db(monkeypatch: pytest.MonkeyPatch) -> _FakeConn:
    conn = _FakeConn(
        ocr_row={
            "masked_text": "진단명: 요추 염좌(S33.5) 입원 5일",
            "entities": {"admission_days": 5},
        }
    )
    monkeypatch.setattr(agents.db, "get_pool", lambda: _FakePool(conn))
    monkeypatch.setattr(agents.crypto, "get_pii_dek", _fake_pii_dek)
    return conn


def _install_db_without_documents(monkeypatch: pytest.MonkeyPatch) -> _FakeConn:
    conn = _install_db(monkeypatch)
    conn.ocr_row = None  # 청구 문서 0건 + 대표 문서 행도 없음
    return conn


def _install_db_failing_reads(monkeypatch: pytest.MonkeyPatch) -> _FakeConn:
    conn = _install_db(monkeypatch)
    conn.read_error = RuntimeError("db down")
    return conn


# ── 외부 API 페이크 ───────────────────────────────────────────
class _ScriptedLlm:
    """chat·chat_json 호출을 한 줄로 세고, 지정한 순번(1부터)에서 지정한 예외를 던진다.

    출력 가드레일 Judge도 같은 모듈 함수(ai_client.chat_json)를 부르므로 함께 세어진다.
    """

    def __init__(
        self,
        fail_at: int | None = None,
        error: Exception | None = None,
        *,
        fail_node: str | None = None,
    ) -> None:
        self._fail_at = fail_at
        self._fail_node = fail_node
        self._error = error
        self.system_prompts: list[str] = []  # 호출 순서대로 system 메시지

    async def _record(self, messages: list[dict[str, str]]) -> None:
        self.system_prompts.append(messages[0]["content"])
        if self._error is None:
            return
        if len(self.system_prompts) == self._fail_at:
            raise self._error
        # 노드 지정: 보장·장해 줄기가 동시에 돌아(R6) 몇 번째 호출이 어느 노드인지 정해져 있지 않다
        if self._fail_node is not None and _call_sites(self.system_prompts[-1:]) == [
            self._fail_node
        ]:
            raise self._error

    async def chat(self, messages: list[dict[str, str]], **opts: Any) -> str:
        await self._record(messages)
        return "리포트 본문"

    async def chat_json(self, messages: list[dict[str, str]], **opts: Any) -> Any:
        await self._record(messages)
        return copy.deepcopy(_LLM_JSON)


class _FakeSearch:
    """hybrid.search 대역. namespace별로 청크 1건을 주고, empty에 든 namespace는 비운다."""

    def __init__(self, empty: frozenset[str] = frozenset()) -> None:
        self._empty = empty
        self.queries: list[str] = []

    async def search(
        self,
        query: str,
        insurance_type: str | None = None,
        namespaces: list[str] | None = None,
        top_k: int = 8,
        *,
        insurer: str | None = None,
        product: str | None = None,
        contract_date: datetime.date | None = None,
    ) -> dict[str, Any]:
        self.queries.append(query)
        namespace = (namespaces or ["terms"])[0]
        if namespace in self._empty:
            return {"ranked_chunks": [], "citations": []}
        chunk = _CASE_CHUNK if namespace == "case" else _TERMS_CHUNK
        return {"ranked_chunks": [dict(chunk)], "citations": [chunk["source_ref"]]}


def _calls_through_node(node: str) -> list[str]:
    """그 노드의 마지막 호출까지의 호출 노드 목록(정렬) — 노드 안 호출(R1)과 보장·장해 줄기(R6)는
    동시에 나가 순서가 정해져 있지 않아 정렬해 비교한다."""
    end = max(i for i, (name, _) in enumerate(_LLM_CALL_ORDER) if name == node) + 1
    return sorted(name for name, _ in _LLM_CALL_ORDER[:end])


# 보장·장해 줄기(R6, agents.coverage_and_disability)의 LLM 노드 — 줄기 안에서는 순서대로 돈다
_LANES = (("coverage_analysis", "case_search"), ("disability_rag", "disability_verify"))


def _allowed_calls_after_failure(node: str) -> set[str]:
    """그 노드가 시스템 실패로 멈췄을 때 나갈 수 있는 LLM 호출 노드.

    병렬 묶음 밖 노드는 순차대로 그 노드까지다. 묶음 안 노드면 진단 + 다른 줄기(실패 시점에 이미
    나간 호출) + 자기 줄기의 그 노드까지다 — 자기 줄기의 다음 노드와 묶음 뒤 노드는 0회다.
    """
    lane = next((lane for lane in _LANES if node in lane), None)
    if lane is None:
        return set(_calls_through_node(node))
    others = {name for other in _LANES if other is not lane for name in other}
    return {"diagnosis", *others, *lane[: lane.index(node) + 1]}


def _call_sites(system_prompts: list[str]) -> list[str]:
    """LLM 호출을 호출한 노드 이름으로 바꾼다(_LLM_CALL_ORDER의 system 프롬프트 힌트로 구분)."""
    return [
        next((node for node, hint in _LLM_CALL_ORDER if hint in prompt), "?")
        for prompt in system_prompts
    ]


def _install_llm(monkeypatch: pytest.MonkeyPatch, llm: _ScriptedLlm) -> _ScriptedLlm:
    monkeypatch.setattr(agents.ai_client, "chat", llm.chat)
    monkeypatch.setattr(agents.ai_client, "chat_json", llm.chat_json)
    return llm


def _install_search(monkeypatch: pytest.MonkeyPatch, search: _FakeSearch) -> _FakeSearch:
    monkeypatch.setattr(agents.hybrid, "search", search.search)
    return search


# ── Claude SDK 경로(httpx2.MockTransport) ─────────────────────
def _sse(text: str | None, *, stop_reason: str = "end_turn") -> bytes:
    """Messages 스트리밍(SSE) 응답 본문. text가 None이면 출력 없이 stop_reason만 온다."""
    message = {
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "model": _MODEL,
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 12, "output_tokens": 1},
    }
    events: list[dict[str, Any]] = [{"type": "message_start", "message": message}]
    if text is not None:
        events += [
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text},
            },
            {"type": "content_block_stop", "index": 0},
        ]
    delta = {"stop_reason": stop_reason, "stop_sequence": None, "stop_details": None}
    events += [
        {"type": "message_delta", "delta": delta, "usage": {"output_tokens": 7}},
        {"type": "message_stop"},
    ]
    return "".join(
        f"event: {event['type']}\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
        for event in events
    ).encode()


def _claude_ok() -> httpx2.Response:
    body = _sse(json.dumps(_LLM_JSON, ensure_ascii=False))
    return httpx2.Response(200, headers={"content-type": "text/event-stream"}, content=body)


def _claude_rate_limited() -> httpx2.Response:
    error = {"type": "error", "error": {"type": "rate_limit_error", "message": "slow down"}}
    return httpx2.Response(429, json=error)


def _claude_refused() -> httpx2.Response:
    body = _sse(None, stop_reason="refusal")
    return httpx2.Response(200, headers={"content-type": "text/event-stream"}, content=body)


class _ClaudeApi:
    """요청을 세고, 순서대로 준비한 응답을 돌려주는 Messages API 대역."""

    def __init__(self) -> None:
        self.requests: list[httpx2.Request] = []
        self.replies: list[Callable[[], httpx2.Response]] = []

    def handle(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        return self.replies[len(self.requests) - 1]()


@pytest.fixture
async def claude_api(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_ClaudeApi]:
    """ai_client를 실제 Claude 경로(SDK 재시도 0회)로 돌리고 HTTP 계층만 대역으로 바꾼다."""
    api = _ClaudeApi()
    monkeypatch.setattr(settings, "llm_provider", "anthropic")
    monkeypatch.setattr(settings, "ai_api_key", "sk-ant-test")
    monkeypatch.setattr(settings, "llm_model", _MODEL)
    monkeypatch.setattr(settings, "llm_effort", "high")
    monkeypatch.setattr(settings, "llm_max_tokens", 64000)
    client = anthropic.AsyncAnthropic(
        api_key="sk-ant-test",
        base_url="https://api.anthropic.com",
        max_retries=0,
        http_client=anthropic.DefaultAsyncHttpxClient(transport=httpx2.MockTransport(api.handle)),
    )
    monkeypatch.setattr(ai_client, "_anthropic_client", client)
    yield api
    await client.close()


# ── SQS 컨슈머(실제 처분 로직) ────────────────────────────────
class _FakeSqs:
    """delete_message만 흉내 내는 boto3 SQS 대역. 삭제됐으면 ack된 것이다."""

    def __init__(self) -> None:
        self.deleted: list[str] = []

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> None:
        self.deleted.append(ReceiptHandle)


def _job() -> ReportJob:
    return ReportJob(
        report_id=str(uuid.uuid4()),
        ocr_result_id=str(uuid.uuid4()),
        job_id=str(uuid.uuid4()),
        doc_type=DocType.DIAGNOSIS,
        user_ref="u-1",
        created_at=datetime.datetime.now(datetime.UTC),
    )


def _message(job: ReportJob) -> dict[str, Any]:
    return {
        "MessageId": "m-1",
        "ReceiptHandle": "rh-1",
        "Body": job.model_dump_json(),
        "Attributes": {"ApproximateReceiveCount": "1"},
    }


def _consumer(sqs: _FakeSqs) -> SqsConsumer[ReportJob]:
    # 운영 report 배포값(SQS_MAX_RECEIVE_COUNT=3)과 같은 상한.
    return SqsConsumer(
        _QUEUE,
        ReportJob,
        worker.handle_job,
        settings=Settings(sqs_max_receive_count=3),
        client=sqs,
    )


class _LogRecorder:
    """worker.logger 대역 — 레벨·이벤트·필드를 그대로 쌓는다."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, object]]] = []

    def info(self, event: str, **fields: object) -> None:
        self.records.append(("info", event, fields))

    def warning(self, event: str, **fields: object) -> None:
        self.records.append(("warning", event, fields))

    def error(self, event: str, **fields: object) -> None:
        self.records.append(("error", event, fields))


def _drafts(conn: _FakeConn) -> list[dict[str, Any]]:
    """persist가 report_drafts에 쓴 초안(JSONB 인자)을 꺼낸다."""
    return [json.loads(str(args[1])) for q, args in conn.executed if "report_drafts" in q]


# ── (e) 구조: 모든 간선에 단락 ────────────────────────────────
def test_every_non_terminal_node_routes_failures_before_its_next_node() -> None:
    # Arrange
    graph = build_graph().get_graph()
    conditional = {(edge.source, edge.target) for edge in graph.edges if edge.conditional}
    nodes = set(graph.nodes) - {"__start__", "__end__", *TERMINAL_NODES}

    # Act
    missing = sorted(
        node for node in nodes if not {(node, "__end__"), (node, "persist_refused")} <= conditional
    )

    # Assert: 새 노드를 add_edge로 직결하면 여기 걸린다. 종료 노드는 END로만 간다
    assert missing == []
    assert {
        (edge.source, edge.target) for edge in graph.edges if edge.source in TERMINAL_NODES
    } == {(node, "__end__") for node in TERMINAL_NODES}


# ── 정상 경로(회귀 없음) ──────────────────────────────────────
async def test_success_path_saves_and_acks(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    conn = _install_db(monkeypatch)
    llm = _install_llm(monkeypatch, _ScriptedLlm())
    _install_search(monkeypatch, _FakeSearch())
    sqs = _FakeSqs()

    # Act
    await _consumer(sqs)._process(_message(_job()))

    # Assert: LLM 8곳을 순서대로 전부 타고(마지막이 Judge) 저장·ack했다
    # 보장·장해 줄기(R6)는 동시에 돌아 그 사이 순서는 정해져 있지 않다 — 정렬해 비교한다
    assert sorted(_call_sites(llm.system_prompts)) == sorted(node for node, _ in _LLM_CALL_ORDER)
    assert any("status = 'AWAITING_ADOPTION'" in q for q, _ in conn.executed)
    assert _drafts(conn)[0]["errors"] == []
    assert sqs.deleted == ["rh-1"]


# ── (a) 컨텍스트 하드 실패 ────────────────────────────────────
@pytest.mark.parametrize(
    ("install_db", "marker"),
    [
        pytest.param(_install_db_without_documents, "ocr_result_missing", id="no-documents"),
        pytest.param(
            _install_db_failing_reads, "load_context_failed:RuntimeError", id="load-context-raises"
        ),
    ],
)
async def test_context_failure_ends_graph_before_any_llm_or_write(
    monkeypatch: pytest.MonkeyPatch,
    install_db: Callable[[pytest.MonkeyPatch], _FakeConn],
    marker: str,
) -> None:
    # Arrange
    conn = install_db(monkeypatch)
    llm = _install_llm(monkeypatch, _ScriptedLlm())
    search = _install_search(monkeypatch, _FakeSearch())

    # Act
    with pytest.raises(worker.ReportWorkerError) as caught:
        await worker.handle_job(_job())

    # Assert: load_context 직후 끝났다 — LLM·검색·DB 쓰기 0회, 재전달 대상(종결 아님)
    assert llm.system_prompts == []
    assert search.queries == []
    assert conn.executed == []
    assert not isinstance(caught.value, NonRetryableError)
    assert marker in str(caught.value)


# ── (b) LLM 시스템 실패 ──────────────────────────────────────
async def test_claude_rate_limit_stops_at_first_failure_and_redelivers(
    monkeypatch: pytest.MonkeyPatch, claude_api: _ClaudeApi
) -> None:
    # Arrange: 진단은 성공, 그다음 호출(보장·장해 줄기가 동시에 출발 — R6)이 429
    conn = _install_db(monkeypatch)
    _install_search(monkeypatch, _FakeSearch())
    claude_api.replies = [_claude_ok, _claude_rate_limited, _claude_rate_limited]
    sqs = _FakeSqs()

    # Act
    await _consumer(sqs)._process(_message(_job()))

    # Assert: 429 뒤로 다음 단계 요청이 없다 — 진단 1 + 두 줄기의 첫 호출뿐이다. 아무것도 저장하지
    # 않았고, 메시지는 재전달된다(삭제 안 함)
    assert len(claude_api.requests) <= 3
    assert conn.executed == []
    assert sqs.deleted == []


@pytest.mark.parametrize(
    "node",
    [pytest.param(node, id=node) for node in dict.fromkeys(n for n, _ in _LLM_CALL_ORDER[:-1])],
)
async def test_llm_system_failure_anywhere_stops_further_llm_calls_and_saves_nothing(
    monkeypatch: pytest.MonkeyPatch, node: str
) -> None:
    # Arrange: Judge 앞의 LLM 노드 하나가 시스템 실패(AiClientError)
    conn = _install_db(monkeypatch)
    llm = _install_llm(
        monkeypatch,
        _ScriptedLlm(fail_node=node, error=ai_client.AiClientError("chat 호출 실패")),
    )
    _install_search(monkeypatch, _FakeSearch())

    # Act
    with pytest.raises(worker.ReportWorkerError) as caught:
        await worker.handle_job(_job())

    # Assert: 실패한 노드에서 멈췄고(자기 줄기의 다음 노드·병렬 묶음 뒤 노드의 LLM 0회) 저장·상태
    # 갱신이 없다. 재전달 대상이다. report_compose는 본문·쟁점을 동시에 부르므로(R1) 두 호출이
    # 함께 나가고, 보장·장해 줄기(R6)는 동시에 돌아 다른 줄기의 호출은 실패 시점까지 나갈 수 있다.
    sites = _call_sites(llm.system_prompts)
    assert node in sites
    assert set(sites) <= _allowed_calls_after_failure(node)
    assert conn.executed == []
    assert not isinstance(caught.value, NonRetryableError)
    assert f"{node}_failed:AiClientError" in str(caught.value)


async def test_llm_system_failure_logs_only_markers_before_redelivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 첫 LLM 호출(진단)에서 시스템 실패
    _install_db(monkeypatch)
    _install_llm(monkeypatch, _ScriptedLlm(fail_at=1, error=ai_client.AiClientError("x")))
    _install_search(monkeypatch, _FakeSearch())
    recorder = _LogRecorder()
    monkeypatch.setattr(worker, "logger", recorder)

    # Act
    with pytest.raises(worker.ReportWorkerError):
        await worker.handle_job(_job())

    # Assert: 컨슈머 로그엔 사유가 없으므로 worker가 표식(노드명·예외 타입)만 남긴다
    graph_events = [r for r in recorder.records if r[1] == "report.graph.finished"]
    assert [r for r in recorder.records if r[1] != "report.graph.finished"] == [
        (
            "warning",
            "report not saved → redeliver",
            {"failure": agents.LLM_SYSTEM_FAILURE, "errors": ["diagnosis_failed:AiClientError"]},
        )
    ]
    # 성능 계측(report.graph.finished)은 저장 실패여도 남고, 노드명·수치만 싣는다
    [(level, _, fields)] = graph_events
    assert level == "info"
    assert list(fields["node_ms"]) == ["load_context", "input_guardrail", "diagnosis"]
    assert fields["failed_nodes"] == ["diagnosis"]
    assert fields["llm_failure"] == agents.LLM_SYSTEM_FAILURE
    assert fields["total_ms"] == sum(fields["node_ms"].values())
    # 조건 분기(노드 밖) 시간까지 담은 벽시계 — 노드 합보다 작을 수 없다
    assert fields["wall_ms"] >= 0
    assert fields["outside_nodes_ms"] == max(0, fields["wall_ms"] - fields["total_ms"])


# ── (c) 거절 ─────────────────────────────────────────────────
async def test_claude_refusal_marks_blocked_without_draft_and_acks(
    monkeypatch: pytest.MonkeyPatch, claude_api: _ClaudeApi
) -> None:
    # Arrange: 첫 호출(진단)에서 폴백 모델까지 거절
    conn = _install_db(monkeypatch)
    _install_search(monkeypatch, _FakeSearch())
    claude_api.replies = [_claude_refused]
    sqs = _FakeSqs()
    job = _job()

    # Act
    await _consumer(sqs)._process(_message(job))

    # Assert: 이후 요청 없이 BLOCKED만 기록하고(초안·쟁점 없음) 즉시 ack했다
    assert len(claude_api.requests) == 1
    assert len(conn.executed) == 1
    query, args = conn.executed[0]
    assert "status = 'BLOCKED'" in query
    assert args == (uuid.UUID(job.report_id),)
    assert sqs.deleted == ["rh-1"]


async def test_refusal_after_partial_progress_still_blocks_without_draft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 본문 작성(6번째 호출)에서 거절 — 앞 노드 결과가 state에 쌓인 뒤다
    conn = _install_db(monkeypatch)
    llm = _install_llm(
        monkeypatch, _ScriptedLlm(fail_at=6, error=ai_client.LlmRefusalError("refused"))
    )
    _install_search(monkeypatch, _FakeSearch())

    # Act
    with pytest.raises(worker.ReportRefusedError) as caught:
        await worker.handle_job(_job())

    # Assert: 쟁점 추출은 본문과 동시에 출발했고(R1) Judge는 안 불렸다. 부분 결과도 저장하지 않고
    # 상태만 남겼다
    assert sorted(_call_sites(llm.system_prompts)) == _calls_through_node("report_compose")
    assert [q for q, _ in conn.executed] == [agents._MARK_BLOCKED_SQL]
    assert isinstance(caught.value, NonRetryableError)
    assert "report_compose_failed:LlmRefusalError" in str(caught.value)


async def test_refusal_status_write_failure_redelivers_instead_of_acking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 거절 뒤 BLOCKED 기록이 실패
    conn = _install_db(monkeypatch)
    conn.write_error = RuntimeError("db down")
    _install_llm(monkeypatch, _ScriptedLlm(fail_at=1, error=ai_client.LlmRefusalError("refused")))
    _install_search(monkeypatch, _FakeSearch())

    # Act
    with pytest.raises(worker.ReportWorkerError) as caught:
        await worker.handle_job(_job())

    # Assert: 기록 없이 ack하면 영원히 처리 중이다 — 종결하지 않고 재전달한다
    assert not isinstance(caught.value, NonRetryableError)
    assert "persist_refused_failed:RuntimeError" in str(caught.value)


# ── (d) 저장을 막지 않는 실패 ─────────────────────────────────
@pytest.mark.parametrize(
    "error",
    [
        pytest.param(ai_client.AiClientError("chat 호출 실패"), id="judge-system-failure"),
        pytest.param(ai_client.LlmRefusalError("refused"), id="judge-refusal"),
    ],
)
async def test_judge_failure_still_saves_completed_report(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    # Arrange: 본문까지 완성된 뒤 마지막 호출(출력 가드레일 Judge)이 실패
    conn = _install_db(monkeypatch)
    llm = _install_llm(monkeypatch, _ScriptedLlm(fail_at=len(_LLM_CALL_ORDER), error=error))
    _install_search(monkeypatch, _FakeSearch())

    # Act
    await worker.handle_job(_job())

    # Assert: 검증 미수행 표식을 남기고 정상 저장했다(예외 없음 = ack)
    assert _call_sites(llm.system_prompts)[-1] == "output_guardrail"
    assert any("status = 'AWAITING_ADOPTION'" in q for q, _ in conn.executed)
    assert _drafts(conn)[0]["judge_failures"] == [JUDGE_UNAVAILABLE]


async def test_rag_empty_keeps_saving_partial_report(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 약관·판례·표준 장해표 검색이 전부 빈다(데이터 부재 — LLM 오류 아님)
    conn = _install_db(monkeypatch)
    _install_llm(monkeypatch, _ScriptedLlm())
    _install_search(monkeypatch, _FakeSearch(empty=frozenset({"terms", "case", "level"})))

    # Act
    await worker.handle_job(_job())

    # Assert: 기존 방침(이슈 #11)대로 부분 결과를 저장한다
    assert any("status = 'AWAITING_ADOPTION'" in q for q, _ in conn.executed)
    assert _drafts(conn)[0]["errors"] == [
        "rag_empty",
        "case_data_missing",
        "disability_schedule_missing",
    ]


# ── (f) 실패 저널 — 실제 그래프 경로 ──────────────────────────
# 운영 배선(__main__)과 같은 핸들러(journaled_handler)·poison 훅으로, 그래프가 남긴 실제 표식이
# 저널 분류로 옮겨지는지 본다. 저널 풀은 그래프가 쓰는 DB 페이크와 따로 둔다 — 리포트 쓰기
# (conn.executed)와 저널 쓰기가 섞이면 "실패한 리포트는 저장하지 않는다"를 확인할 수 없다.
class _JournalPool:
    """저널 SQL·인자만 포착하는 asyncpg.Pool 대역(실행하지 않는다)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append((sql, args))
        return "OK"

    def kinds(self) -> list[str]:
        names = {
            failure_journal._UPSERT_FAILURE_SQL: "upsert",
            failure_journal._CLEAR_FAILURE_SQL: "clear",
            failure_journal._MARK_TERMINAL_SQL: "terminal",
            failure_journal._INSERT_UNKEYED_TERMINAL_SQL: "unkeyed",
        }
        return [names.get(sql, "?") for sql, _ in self.calls]


class _OutageLlm:
    """모든 chat·chat_json 호출이 시스템 실패(AiClientError)인 LLM 장애 대역."""

    def __init__(self) -> None:
        self.calls = 0

    async def chat(self, messages: list[dict[str, str]], **opts: Any) -> str:
        self.calls += 1
        raise ai_client.AiClientError("chat 호출 실패")

    async def chat_json(self, messages: list[dict[str, str]], **opts: Any) -> Any:
        self.calls += 1
        raise ai_client.AiClientError("chat 호출 실패")


def _journaled_consumer(sqs: _FakeSqs, journal: _JournalPool) -> SqsConsumer[ReportJob]:
    return SqsConsumer(
        _QUEUE,
        ReportJob,
        worker.journaled_handler(journal),  # type: ignore[arg-type]
        settings=Settings(sqs_max_receive_count=3),
        client=sqs,
        on_poison=report_main._poison_journal(journal),  # type: ignore[arg-type]
    )


def _redelivery(job: ReportJob, receive_count: int) -> dict[str, Any]:
    return {
        "MessageId": "m-1",
        "ReceiptHandle": f"rh-{receive_count}",
        "Body": job.model_dump_json(),
        "Attributes": {"ApproximateReceiveCount": str(receive_count)},
    }


async def _deliver(consumer: SqsConsumer[ReportJob], job: ReportJob, *receive_counts: int) -> None:
    """같은 메시지를 수신 횟수만 바꿔 차례로 처리한다(SQS 재전달 흉내)."""
    for receive_count in receive_counts:
        await consumer._process(_redelivery(job, receive_count))


async def test_persistent_llm_outage_is_journaled_per_delivery_then_terminal_on_poison(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 한도(3) 내내 LLM 장애 — 고치기 전에는 poison 뒤 Spring이 영원히 PROCESSING이었다
    conn = _install_db(monkeypatch)
    llm = _OutageLlm()
    monkeypatch.setattr(agents.ai_client, "chat", llm.chat)
    monkeypatch.setattr(agents.ai_client, "chat_json", llm.chat_json)
    _install_search(monkeypatch, _FakeSearch())
    sqs = _FakeSqs()
    journal = _JournalPool()
    job = _job()

    # Act: 수신 1~3은 그래프가 첫 LLM 호출에서 멈추고, 4는 poison(핸들러 없이 걷힘)
    await _deliver(_journaled_consumer(sqs, journal), job, 1, 2, 3, 4)

    # Assert: 시도마다 비종결 업서트(attempts는 SQL이 쌓는다) → 걷기 직전 terminal 확정
    report_uuid, job_uuid = uuid.UUID(job.report_id), uuid.UUID(job.job_id)
    assert journal.kinds() == ["upsert", "upsert", "upsert", "terminal"]
    assert journal.calls[0][1] == (report_uuid, None, job_uuid, "llm_unavailable", "AiClientError")
    assert journal.calls[-1][1] == (report_uuid, "m-1", None, job_uuid, 4)
    assert llm.calls == 3  # 수신 1~3에서 한 번씩, poison에선 LLM을 부르지 않는다
    assert conn.executed == []  # 리포트 쓰기는 끝까지 없다
    assert sqs.deleted == ["rh-4"]


@pytest.mark.parametrize(
    ("install_db", "error_type"),
    [
        pytest.param(_install_db_without_documents, None, id="no-documents"),
        pytest.param(_install_db_failing_reads, "RuntimeError", id="load-context-raises"),
    ],
)
async def test_context_failure_through_graph_is_journaled_as_context_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    install_db: Callable[[pytest.MonkeyPatch], _FakeConn],
    error_type: str | None,
) -> None:
    # Arrange
    install_db(monkeypatch)
    _install_llm(monkeypatch, _ScriptedLlm())
    _install_search(monkeypatch, _FakeSearch())
    journal = _JournalPool()
    job = _job()

    # Act
    with pytest.raises(worker.ReportWorkerError):
        await worker.journaled_handler(journal)(job)  # type: ignore[arg-type]

    # Assert
    assert journal.calls == [
        (
            failure_journal._UPSERT_FAILURE_SQL,
            (
                uuid.UUID(job.report_id),
                None,
                uuid.UUID(job.job_id),
                str(FailureClass.CONTEXT_UNAVAILABLE),
                error_type,
            ),
        )
    ]


async def test_refusal_through_graph_blocks_without_journal_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 거절은 BLOCKED로 사용자에게 보인다 — 저널(FAILED 근거)에 겹쳐 남기지 않는다
    conn = _install_db(monkeypatch)
    _install_llm(monkeypatch, _ScriptedLlm(fail_at=1, error=ai_client.LlmRefusalError("refused")))
    _install_search(monkeypatch, _FakeSearch())
    sqs = _FakeSqs()
    journal = _JournalPool()

    # Act
    await _deliver(_journaled_consumer(sqs, journal), _job(), 1)

    # Assert: BLOCKED만 쓰고 즉시 ack, 저널은 이전 시도의 행을 지우기만 한다
    assert [q for q, _ in conn.executed] == [agents._MARK_BLOCKED_SQL]
    assert journal.kinds() == ["clear"]
    assert sqs.deleted == ["rh-1"]


async def test_saved_report_through_graph_clears_journal_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    conn = _install_db(monkeypatch)
    _install_llm(monkeypatch, _ScriptedLlm())
    _install_search(monkeypatch, _FakeSearch())
    journal = _JournalPool()
    job = _job()

    # Act
    await worker.journaled_handler(journal)(job)  # type: ignore[arg-type]

    # Assert: 저장됐으니 이전 시도가 남긴 실패 행을 지운다(회복)
    assert any("status = 'AWAITING_ADOPTION'" in q for q, _ in conn.executed)
    assert journal.calls == [(failure_journal._CLEAR_FAILURE_SQL, (uuid.UUID(job.report_id),))]
