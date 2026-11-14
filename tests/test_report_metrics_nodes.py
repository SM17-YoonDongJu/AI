"""리포트 수치 노드 배선 테스트 (DB·LLM·RAG 페이크로 격리).

여기서 고정하는 계약:
  (a) safe_node는 정상·예외 모든 반환에 node_trace(노드·ms·ok|failed)를 덧붙인다. 기존 errors·
      llm_failure 동작은 그대로다.
  (b) load_context는 청구 문서·대표 문서 두 경로에서 input_documents를 채운다. 신뢰도 컬럼이 없거나
      NULL이어도 실패하지 않는다(load_context 실패 = 컨텍스트 하드 실패 = 리포트 없음).
  (c) case_search는 모든 반환 경로에 case_funnel을 남긴다.
  (d) output_guardrail은 약관 → 장해분류표 → 판례 근거를 중복 없이 Judge에 넘기고 judge_stats를
      남긴다.
  (e) persist는 draft["metrics"]와 reports.confidence_level을 쓴다. confidence_level은 세이브포인트
      안에서 쓰고, 컬럼 권한이 없으면 그 값만 건너뛴다. 다른 DB 오류는 지금처럼 persist_failed다.
  (f) 그래프는 output_guardrail → collect_metrics → persist다. 수치 계산이 실패해도 리포트는
      저장되고(소프트 실패), GRANT 적용 전 배포여도 리포트는 저장된다.
"""

import copy
import datetime
import json
import uuid
from typing import Any

import asyncpg
import pytest

from core import ai_client
from core.config import settings
from core.contracts import DocType, OutputGuardResult, ReportJob
from report_worker import worker
from report_worker.graph import build_graph
from report_worker.nodes import agents

# ── LLM·RAG 페이크 데이터 ─────────────────────────────────────
# 장해 경로까지 전부 타는 성공 응답. 노드마다 자기 키만 읽는다
# (test_report_failure_routing과 같은 구성).
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
_SCHEDULE_CHUNK: dict[str, Any] = {
    "text": "장해의 분류 척추에 약간의 운동장해를 남긴 때 지급률 10%",
    "namespace": "terms",
    "score": 1.0,
    "source_ref": "[별표3, 행복보험]",
    "chunk_id": "terms-1",
    "article_number": "별표3",
    "product_name": "행복보험",
    "chunk_type": "schedule",
}
_TERMS_CHUNK: dict[str, Any] = {
    "text": "제3조(보험금의 지급사유) 입원일당을 지급합니다.",
    "namespace": "terms",
    "score": 0.9,
    "source_ref": "[제3조, 행복보험]",
    "chunk_id": "terms-3",
    "article_number": "제3조",
    "product_name": "행복보험",
    "chunk_type": "clause",
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
# 단건 경로의 대표 문서(OCR 마스킹본 — [이름] 토큰 1개)
_OCR_ROW: dict[str, Any] = {
    "masked_text": "진단명: 요추 염좌(S33.5) 환자 [이름] 입원 5일",
    "entities": {"admission_days": 5},
    "ocr_confidence": 0.95,
    "doc_type_confidence": 0.9,
}
# 그래프가 output_guardrail까지 지나는 노드(약관 DB 있음 + 장해 경로)
_NODES_BEFORE_METRICS = [
    "load_context",
    "input_guardrail",
    "diagnosis",
    "coverage_parse",
    "coverage_analysis",
    "case_search",
    "disability_rag",
    "disability_verify",
    "disability_calc",
    "payment_calc",
    "report_compose",
    "output_guardrail",
]
_PRIVILEGE_ERROR = asyncpg.exceptions.InsufficientPrivilegeError("permission denied for table")


# ── DB 페이크 ─────────────────────────────────────────────────
def _sql_label(query: str) -> str:
    """persist SQL을 짧은 이름으로 바꾼다(실행 순서 비교용)."""
    if query == agents._UPDATE_CONFIDENCE_LEVEL_SQL:
        return "UPDATE confidence_level"
    labels = (
        ("INSERT INTO report_drafts", "UPSERT report_drafts"),
        ("UPDATE reports SET", "UPDATE reports"),
        ("DELETE FROM report_issues", "DELETE report_issues"),
        ("INSERT INTO report_issues", "INSERT report_issues"),
    )
    return next((label for marker, label in labels if marker in query), query)


class _Transaction:
    """conn.transaction() 대역. 시작·커밋·롤백을 conn.log에 남긴다(중첩 = 세이브포인트)."""

    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> None:
        self._conn.log.append("BEGIN")

    async def __aexit__(self, exc_type: type[BaseException] | None, *exc: object) -> bool:
        self._conn.log.append("ROLLBACK" if exc_type else "COMMIT")
        return False  # 예외는 그대로 올린다(asyncpg와 같다)


class _FakeConn:
    """load_context·policy_in_db·case_search·persist가 쓰는 쿼리만 흉내 낸다.

    `fail_on` SQL을 실행하면 `error`를 던진다(그 문장은 executed에 남지 않는다).
    """

    def __init__(
        self,
        *,
        ocr_row: dict[str, Any] | None = None,
        claim_docs: list[dict[str, Any]] | None = None,
        fail_on: str | None = None,
        error: Exception | None = None,
    ) -> None:
        self.ocr_row = ocr_row
        self.claim_docs = claim_docs or []
        self.fail_on = fail_on
        self.error = error
        self.queries: list[str] = []  # 읽기 SQL
        self.log: list[str] = []  # 트랜잭션 경계 + 쓰기 SQL 이름(실행 순서)
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        self.queries.append(query)
        if "FROM ocr_results" in query:
            return list(self.claim_docs)
        return []  # 판례 메타 역추적(case_chunks) — 메타 없음

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        self.queries.append(query)
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
        self.log.append(_sql_label(query))
        if query == self.fail_on and self.error is not None:
            raise self.error
        self.executed.append((query, args))

    def transaction(self) -> _Transaction:
        return _Transaction(self)


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


def _install_db(monkeypatch: pytest.MonkeyPatch, conn: _FakeConn) -> _FakeConn:
    monkeypatch.setattr(agents.db, "get_pool", lambda: _FakePool(conn))
    monkeypatch.setattr(agents.crypto, "get_pii_dek", _fake_pii_dek)
    return conn


# ── LLM·RAG 페이크 ────────────────────────────────────────────
class _Llm:
    """chat·chat_json 대역. 출력 가드레일 Judge 프롬프트(user)를 따로 모은다."""

    def __init__(self) -> None:
        self.judge_prompts: list[str] = []

    async def chat(self, messages: list[dict[str, str]], **opts: Any) -> str:
        return "리포트 본문"

    async def chat_json(self, messages: list[dict[str, str]], **opts: Any) -> Any:
        if "검증관" in messages[0]["content"]:
            self.judge_prompts.append(messages[1]["content"])
        return copy.deepcopy(_LLM_JSON)


async def _fake_search(
    query: str,
    insurance_type: str | None = None,
    namespaces: list[str] | None = None,
    top_k: int = 8,
    *,
    insurer: str | None = None,
    product: str | None = None,
    contract_date: datetime.date | None = None,
) -> dict[str, Any]:
    """hybrid.search 대역. 판례는 판례 청크, 그 밖(약관·장해표)은 장해분류표 청크 1건."""
    chunk = _CASE_CHUNK if namespaces == ["case"] else _SCHEDULE_CHUNK
    return {"ranked_chunks": [dict(chunk)], "citations": [chunk["source_ref"]]}


def _install_apis(monkeypatch: pytest.MonkeyPatch) -> _Llm:
    llm = _Llm()
    monkeypatch.setattr(agents.ai_client, "chat", llm.chat)
    monkeypatch.setattr(agents.ai_client, "chat_json", llm.chat_json)
    monkeypatch.setattr(agents.hybrid, "search", _fake_search)
    return llm


class _LogRecorder:
    """agents.logger 대역 — 레벨·이벤트·필드를 그대로 쌓는다."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, object]]] = []

    def info(self, event: str, **fields: object) -> None:
        self.records.append(("info", event, fields))

    def warning(self, event: str, **fields: object) -> None:
        self.records.append(("warning", event, fields))

    def error(self, event: str, **fields: object) -> None:
        self.records.append(("error", event, fields))


def _install_clock(monkeypatch: pytest.MonkeyPatch, *ticks: float) -> None:
    """safe_node가 읽는 perf_counter를 정해진 값으로 바꾼다(ms를 결정적으로)."""
    clock = iter(ticks)
    monkeypatch.setattr(agents, "perf_counter", lambda: next(clock))


def _state(*, claim_id: str | None = None, doc_type: str = "diagnosis") -> dict[str, Any]:
    return {
        "report_id": str(uuid.uuid4()),
        "ocr_result_id": str(uuid.uuid4()),
        "claim_id": claim_id,
        "user_ref": "u-1",
        "doc_type": doc_type,
    }


def _job() -> ReportJob:
    return ReportJob(
        report_id=str(uuid.uuid4()),
        ocr_result_id=str(uuid.uuid4()),
        job_id=str(uuid.uuid4()),
        doc_type=DocType.DIAGNOSIS,
        user_ref="u-1",
        created_at=datetime.datetime.now(datetime.UTC),
    )


def _drafts(conn: _FakeConn) -> list[dict[str, Any]]:
    """persist가 report_drafts에 쓴 초안(JSONB 인자)을 꺼낸다."""
    return [json.loads(str(args[1])) for query, args in conn.executed if "report_drafts" in query]


def _confidence_writes(conn: _FakeConn) -> list[tuple[object, ...]]:
    return [args for query, args in conn.executed if query == agents._UPDATE_CONFIDENCE_LEVEL_SQL]


@pytest.fixture(autouse=True)
def ocr_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    """OCR 저신뢰도 임계값을 기본값(0.90)으로 고정한다(셸의 OCR_LOW_CONFIDENCE_THRESHOLD 무관)."""
    monkeypatch.setattr(settings, "ocr_low_confidence_threshold", 0.90)


# ── (a) safe_node 소요 시간 ───────────────────────────────────
@agents.safe_node
async def _passing_node(state: dict[str, Any]) -> dict[str, Any]:
    return {"masked_text": "가공된 본문"}


@agents.safe_node
async def _crashing_node(state: dict[str, Any]) -> dict[str, Any]:
    raise ValueError("원문 조각이 섞일 수 있는 메시지")


@agents.safe_node
async def _llm_down_node(state: dict[str, Any]) -> dict[str, Any]:
    raise ai_client.AiClientError("429")


@agents.safe_node
async def _silent_node(state: dict[str, Any]) -> dict[str, Any]:
    return None  # type: ignore[return-value]  # LangGraph는 None(갱신 없음)을 허용한다


async def test_safe_node_appends_ok_trace_after_existing_entries(monkeypatch) -> None:
    # Arrange
    _install_clock(monkeypatch, 10.0, 10.25)
    state = {"node_trace": [{"node": "load_context", "ms": 7, "status": "ok"}]}

    # Act
    out = await _passing_node(state)

    # Assert: 노드 결과는 그대로, 소요 시간은 기존 목록 뒤에 붙는다
    assert out == {
        "masked_text": "가공된 본문",
        "node_trace": [
            {"node": "load_context", "ms": 7, "status": "ok"},
            {"node": "_passing_node", "ms": 250, "status": "ok"},
        ],
    }


async def test_safe_node_traces_failed_node_without_changing_error_marker(monkeypatch) -> None:
    # Arrange
    _install_clock(monkeypatch, 1.0, 2.5)

    # Act
    out = await _crashing_node({"errors": ["rag_empty"]})

    # Assert: 표식은 예전 그대로(타입명만), 소요 시간은 failed로 남는다
    assert out == {
        "errors": ["rag_empty", "_crashing_node_failed:ValueError"],
        "node_trace": [{"node": "_crashing_node", "ms": 1500, "status": "failed"}],
    }


async def test_safe_node_traces_llm_failure_and_keeps_short_circuit_kind(monkeypatch) -> None:
    # Arrange
    _install_clock(monkeypatch, 3.0, 3.002)

    # Act
    out = await _llm_down_node({})

    # Assert
    assert out == {
        "errors": ["_llm_down_node_failed:AiClientError"],
        "llm_failure": agents.LLM_SYSTEM_FAILURE,
        "node_trace": [{"node": "_llm_down_node", "ms": 2, "status": "failed"}],
    }


async def test_safe_node_tolerates_node_returning_none(monkeypatch) -> None:
    # Arrange
    _install_clock(monkeypatch, 5.0, 5.0)

    # Act
    out = await _silent_node({})

    # Assert
    assert out == {"node_trace": [{"node": "_silent_node", "ms": 0, "status": "ok"}]}


# ── (b) load_context: 입력 문서 품질 ──────────────────────────
async def test_load_context_records_quality_of_every_claim_document(monkeypatch) -> None:
    # Arrange: 청구 문서 2건(doc_index 순)
    conn = _install_db(
        monkeypatch,
        _FakeConn(
            claim_docs=[
                {
                    "doc_type": "policy",
                    "masked_text": "증권",
                    "entities": {},
                    "ocr_confidence": 0.96,
                    "doc_type_confidence": 0.8,
                },
                {
                    "doc_type": "diagnosis",
                    "masked_text": "진단",
                    "entities": {},
                    "ocr_confidence": 0.88,
                    "doc_type_confidence": 1.0,
                },
            ]
        ),
    )

    # Act
    out = await agents.load_context(_state(claim_id="claim-1"))

    # Assert
    assert out["input_documents"] == [
        {"doc_type": "policy", "ocr_confidence": 0.96, "doc_type_confidence": 0.8},
        {"doc_type": "diagnosis", "ocr_confidence": 0.88, "doc_type_confidence": 1.0},
    ]
    assert out["errors"] == []
    assert "ocr_confidence, doc_type_confidence" in conn.queries[0]


async def test_load_context_records_quality_of_representative_document(monkeypatch) -> None:
    # Arrange: claim에 안 묶인 단건 경로 — 유형은 ReportJob의 doc_type이다
    conn = _install_db(monkeypatch, _FakeConn(ocr_row=_OCR_ROW))

    # Act
    out = await agents.load_context(_state(doc_type="diagnosis"))

    # Assert
    assert out["input_documents"] == [
        {"doc_type": "diagnosis", "ocr_confidence": 0.95, "doc_type_confidence": 0.9}
    ]
    assert "ocr_confidence, doc_type_confidence" in conn.queries[0]


@pytest.mark.parametrize(
    ("claim_id", "conn"),
    [
        pytest.param(
            "claim-1",
            _FakeConn(claim_docs=[{"doc_type": "policy", "masked_text": "증권", "entities": {}}]),
            id="claim-row-without-columns",
        ),
        pytest.param(
            None,
            _FakeConn(
                ocr_row={
                    "masked_text": "증권",
                    "entities": {},
                    "ocr_confidence": None,
                    "doc_type_confidence": "n/a",
                }
            ),
            id="single-row-with-null-and-garbage",
        ),
    ],
)
async def test_load_context_does_not_fail_on_missing_confidence(
    monkeypatch, claim_id: str | None, conn: _FakeConn
) -> None:
    # Arrange
    _install_db(monkeypatch, conn)

    # Act
    out = await agents.load_context(_state(claim_id=claim_id, doc_type="policy"))

    # Assert: 컨텍스트는 정상이고 신뢰도만 비어 있다(하드 실패 아님)
    assert out["errors"] == []
    assert out["input_documents"] == [
        {"doc_type": "policy", "ocr_confidence": None, "doc_type_confidence": None}
    ]


# ── (c) case_search: 판례 깔때기 ──────────────────────────────
def _case(chunk_id: str) -> dict[str, Any]:
    return {**_CASE_CHUNK, "chunk_id": chunk_id, "source_ref": f"[{chunk_id}, ]"}


def _install_case_search(
    monkeypatch: pytest.MonkeyPatch,
    chunks: list[dict[str, Any]],
    case_numbers: dict[str, str],
    relevant: list[int],
) -> None:
    async def _search(query: str, *args: object, **kwargs: object) -> dict[str, Any]:
        return {"ranked_chunks": [dict(c) for c in chunks], "citations": []}

    async def _enrich(chunk_ids: list[str]) -> dict[str, dict[str, Any]]:
        return {cid: {"case_no": no} for cid, no in case_numbers.items() if cid in chunk_ids}

    async def _filter(messages: list[dict[str, str]], **opts: Any) -> dict[str, Any]:
        return {"relevant": relevant}

    monkeypatch.setattr(agents.hybrid, "search", _search)
    monkeypatch.setattr(agents, "_enrich_cases", _enrich)
    monkeypatch.setattr(agents.ai_client, "chat_json", _filter)


async def test_case_search_funnel_without_results_is_all_zero(monkeypatch) -> None:
    # Arrange
    _install_case_search(monkeypatch, [], {}, [])

    # Act
    out = await agents.case_search({"diagnosis": {"diagnosis": "요추 염좌"}})

    # Assert
    assert out["case_funnel"] == {"candidates": 0, "deduped": 0, "relevant": 0, "kept": 0}
    assert out["errors"] == ["case_data_missing"]


async def test_case_search_funnel_counts_dedup_and_relevance(monkeypatch) -> None:
    # Arrange: 후보 3건 중 2건이 같은 사건, 관련성 필터는 1건만 남긴다
    _install_case_search(
        monkeypatch,
        [_case("case-1"), _case("case-2"), _case("case-3")],
        {"case-1": "2020가단1", "case-2": "2020가단1", "case-3": "2021나2"},
        [0],
    )

    # Act
    out = await agents.case_search({"diagnosis": {"diagnosis": "요추 염좌"}})

    # Assert
    assert out["case_funnel"] == {"candidates": 3, "deduped": 2, "relevant": 1, "kept": 1}


async def test_case_search_funnel_kept_is_capped_at_top_n(monkeypatch) -> None:
    # Arrange: 서로 다른 사건 6건이 전부 관련 있다
    chunks = [_case(f"case-{n}") for n in range(6)]
    _install_case_search(monkeypatch, chunks, {}, [0, 1, 2, 3, 4, 5])

    # Act
    out = await agents.case_search({"diagnosis": {"diagnosis": "요추 염좌"}})

    # Assert
    assert out["case_funnel"] == {"candidates": 6, "deduped": 6, "relevant": 6, "kept": 4}
    assert len(out["legal_references"]) == agents._CASE_TOP_N


# ── (d) output_guardrail: Judge 근거와 범위 ───────────────────
class _GuardOutputSpy:
    """guardrail.guard_output 대역 — 넘겨받은 근거를 기록하고 정해진 결과를 돌려준다."""

    def __init__(self, result: OutputGuardResult) -> None:
        self.result = result
        self.chunks: list[Any] = []

    async def __call__(
        self, text: str, *, run_judge: bool = True, chunks: list[Any] | None = None
    ) -> OutputGuardResult:
        self.chunks = list(chunks or [])
        return self.result


async def test_output_guardrail_sends_ordered_deduped_evidence_and_keeps_judge_stats(
    monkeypatch,
) -> None:
    # Arrange: 장해분류표가 약관보다 먼저 병합됐고, 약관 청크가 두 번 들어 있다. Judge 범위는
    # 세 값이 모두 달라 매핑이 뒤바뀌면(checked↔total 등) 드러난다
    spy = _GuardOutputSpy(
        OutputGuardResult(
            final_text="> 고지문\n\n본문",
            judge_failures=["근거 없는 문장"],
            judge_checked_chars=12_000,
            judge_total_chars=12_480,
            judge_evidence_chunks=3,
        )
    )
    monkeypatch.setattr(agents.guardrail, "guard_output", spy)
    state = {
        "report": "본문",
        "retrieved_clauses": [_SCHEDULE_CHUNK, _TERMS_CHUNK, dict(_TERMS_CHUNK)],
        "legal_references": [_CASE_CHUNK],
    }

    # Act
    out = await agents.output_guardrail(state)

    # Assert: 약관 → 장해분류표 → 판례, 중복 없이
    assert spy.chunks == [_TERMS_CHUNK, _SCHEDULE_CHUNK, _CASE_CHUNK]
    assert out["report"] == "> 고지문\n\n본문"
    assert out["judge_failures"] == ["근거 없는 문장"]
    assert out["judge_stats"] == {
        "checked_chars": 12_000,
        "total_chars": 12_480,
        "evidence_chunks": 3,
    }


async def test_output_guardrail_judge_now_sees_case_evidence(monkeypatch) -> None:
    # Arrange: 실제 guard_output — 약관 근거가 없어도 판례만으로 Judge가 돈다
    llm = _install_apis(monkeypatch)
    state = {
        "report": "요추 염좌 리포트",
        "retrieved_clauses": [],
        "legal_references": [_CASE_CHUNK],
    }

    # Act
    out = await agents.output_guardrail(state)

    # Assert: 판례 원문이 Judge 프롬프트에 들어갔고, 범위는 고지문 포함 전체다
    assert _CASE_CHUNK["text"] in llm.judge_prompts[0]
    assert out["judge_stats"]["evidence_chunks"] == 1
    assert out["judge_stats"]["checked_chars"] == len(out["report"])
    assert out["judge_stats"]["total_chars"] == len(out["report"])


# ── collect_metrics ───────────────────────────────────────────
async def test_collect_metrics_returns_metrics_and_matching_level() -> None:
    # Arrange: 범위 산출 불가 상태(LOW)
    state = {"estimated_range": {"min": 0, "max": 0}, "errors": []}

    # Act
    out = await agents.collect_metrics(state)

    # Assert
    assert out["report_metrics"]["version"] == 1
    assert out["confidence_level"] == "LOW"
    assert out["confidence_level"] == out["report_metrics"]["confidence"]["level"]
    assert out["node_trace"][-1]["node"] == "collect_metrics"


# ── (e) persist: draft.metrics · confidence_level ─────────────
def _persist_state(**overrides: Any) -> dict[str, Any]:
    return {
        "report_id": str(uuid.uuid4()),
        "sections": {"6_본문": "본문"},
        "estimated_range": {"min": 1_000_000, "max": 1_080_000},
        "issues": [{"title": "입원일당", "description": "입원일수 확인", "ai_status": "INFO"}],
        "errors": [],
        "report_metrics": {"version": 1, "confidence": {"level": "MEDIUM", "reasons": ["사유"]}},
        "confidence_level": "MEDIUM",
        **overrides,
    }


async def test_persist_saves_metrics_and_confidence_level_in_a_savepoint(monkeypatch) -> None:
    # Arrange
    conn = _install_db(monkeypatch, _FakeConn())
    state = _persist_state()

    # Act
    out = await agents.persist(state)

    # Assert: confidence_level은 바깥 트랜잭션 안의 중첩 트랜잭션(세이브포인트)에서 쓴다
    assert conn.log == [
        "BEGIN",
        "UPSERT report_drafts",
        "UPDATE reports",
        "BEGIN",
        "UPDATE confidence_level",
        "COMMIT",
        "DELETE report_issues",
        "INSERT report_issues",
        "COMMIT",
    ]
    assert _drafts(conn)[0]["metrics"] == state["report_metrics"]
    assert _confidence_writes(conn) == [(uuid.UUID(state["report_id"]), "MEDIUM")]
    assert out["errors"] == []


async def test_persist_skips_confidence_level_without_column_privilege(monkeypatch) -> None:
    # Arrange: deploy/schema_split*.sql의 컬럼 GRANT가 아직 적용되지 않은 DB
    conn = _install_db(
        monkeypatch,
        _FakeConn(fail_on=agents._UPDATE_CONFIDENCE_LEVEL_SQL, error=_PRIVILEGE_ERROR),
    )
    recorder = _LogRecorder()
    monkeypatch.setattr(agents, "logger", recorder)

    # Act
    out = await agents.persist(_persist_state())

    # Assert: 세이브포인트만 되돌리고 나머지는 커밋 — 리포트는 저장됐다(하드 실패 아님)
    assert conn.log == [
        "BEGIN",
        "UPSERT report_drafts",
        "UPDATE reports",
        "BEGIN",
        "UPDATE confidence_level",
        "ROLLBACK",
        "DELETE report_issues",
        "INSERT report_issues",
        "COMMIT",
    ]
    assert out["errors"] == []
    assert recorder.records == [
        (
            "warning",
            "report confidence level not saved",
            {"error_type": "InsufficientPrivilegeError"},
        )
    ]


async def test_persist_other_confidence_level_errors_still_fail_the_save(monkeypatch) -> None:
    # Arrange: 권한 오류가 아닌 DB 오류(컬럼 없음 등)는 삼키지 않는다
    error = asyncpg.exceptions.UndefinedColumnError("column does not exist")
    conn = _install_db(
        monkeypatch, _FakeConn(fail_on=agents._UPDATE_CONFIDENCE_LEVEL_SQL, error=error)
    )

    # Act
    out = await agents.persist(_persist_state())

    # Assert: 바깥 트랜잭션까지 롤백되고 하드 실패(persist_failed)로 재전달된다
    assert conn.log[-3:] == ["UPDATE confidence_level", "ROLLBACK", "ROLLBACK"]
    assert out["errors"] == ["persist_failed:UndefinedColumnError"]
    assert out["errors"][0].startswith(worker._HARD_FAILURE_PREFIXES)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"report_metrics": None, "confidence_level": None}, id="metrics-failed"),
        pytest.param({"confidence_level": "medium"}, id="unknown-level"),
    ],
)
async def test_persist_writes_confidence_level_only_for_known_levels(
    monkeypatch, overrides: dict[str, Any]
) -> None:
    # Arrange
    conn = _install_db(monkeypatch, _FakeConn())

    # Act
    await agents.persist(_persist_state(**overrides))

    # Assert
    assert "UPDATE confidence_level" not in conn.log
    assert "UPDATE reports" in conn.log


async def test_persist_omits_metrics_key_when_metrics_were_not_computed(monkeypatch) -> None:
    # Arrange
    conn = _install_db(monkeypatch, _FakeConn())

    # Act
    await agents.persist(_persist_state(report_metrics=None, confidence_level=None))

    # Assert: 읽는 쪽은 키가 없을 수 있음을 허용한다(contracts.md §4)
    assert "metrics" not in _drafts(conn)[0]


# ── (f) 그래프 ────────────────────────────────────────────────
def test_collect_metrics_sits_between_output_guardrail_and_persist() -> None:
    # Arrange
    graph = build_graph().get_graph()

    # Act
    edges = {(edge.source, edge.target) for edge in graph.edges}

    # Assert
    assert ("output_guardrail", "collect_metrics") in edges
    assert ("collect_metrics", "persist") in edges
    assert ("output_guardrail", "persist") not in edges


async def test_graph_saves_metrics_and_confidence_level(monkeypatch) -> None:
    # Arrange
    conn = _install_db(monkeypatch, _FakeConn(ocr_row=_OCR_ROW))
    _install_apis(monkeypatch)
    state = _state()

    # Act
    final = await build_graph().ainvoke(state)

    # Assert: 수치는 output_guardrail까지의 노드를 담고, 그 뒤로 collect_metrics·persist가 돈다
    report_metrics = _drafts(conn)[0]["metrics"]
    assert [node["node"] for node in report_metrics["pipeline"]["nodes"]] == _NODES_BEFORE_METRICS
    assert [node["node"] for node in final["node_trace"]] == [
        *_NODES_BEFORE_METRICS,
        "collect_metrics",
        "persist",
    ]
    # Assert: load_context → input, case_search → funnel, output_guardrail → judge가 이어진다
    assert report_metrics["input"] == {
        "document_count": 1,
        "doc_types": ["diagnosis"],
        "ocr_confidence": {"min": 0.95, "avg": 0.95},
        "doc_type_confidence": {"min": 0.9, "avg": 0.9},
        "pii_masked": {"total": 1, "by_label": {"이름": 1}, "documents": 1, "free_text": 0},
    }
    assert report_metrics["retrieval"]["case_funnel"] == {
        "candidates": 1,
        "deduped": 1,
        "relevant": 1,
        "kept": 1,
    }
    assert report_metrics["retrieval"]["evidence"] == {"terms": 0, "schedule": 1, "case": 1}
    assert report_metrics["verification"]["judge"]["coverage"] == 1.0
    assert report_metrics["verification"]["judge"]["evidence_chunks"] == 2
    # Assert: 운영처럼 가입금액이 없으니 MEDIUM이고, 같은 값이 reports.confidence_level로 간다
    assert report_metrics["confidence"] == {
        "level": "MEDIUM",
        "reasons": ["가입금액 미보유 — 제시액 기준 어림 범위"],
    }
    assert _confidence_writes(conn) == [(uuid.UUID(state["report_id"]), "MEDIUM")]
    assert final["confidence_level"] == "MEDIUM"


async def test_metrics_failure_is_soft_and_the_report_is_still_saved(monkeypatch) -> None:
    # Arrange: 수치 계산이 터진다
    conn = _install_db(monkeypatch, _FakeConn(ocr_row=_OCR_ROW))
    _install_apis(monkeypatch)

    def _broken(state: dict[str, Any]) -> dict[str, Any]:
        raise ZeroDivisionError

    monkeypatch.setattr(agents.metrics, "build_report_metrics", _broken)

    # Act
    await worker.handle_job(_job())

    # Assert: 예외 없이(ack) 초안을 저장했다 — metrics 키와 confidence_level만 없다
    draft = _drafts(conn)[0]
    assert "metrics" not in draft
    assert draft["errors"] == ["collect_metrics_failed:ZeroDivisionError"]
    assert _confidence_writes(conn) == []
    assert any("status = 'AWAITING_ADOPTION'" in query for query, _ in conn.executed)


async def test_report_is_saved_before_the_confidence_level_grant_is_applied(monkeypatch) -> None:
    # Arrange: 워커를 GRANT보다 먼저 배포한 DB
    conn = _install_db(
        monkeypatch,
        _FakeConn(
            ocr_row=_OCR_ROW, fail_on=agents._UPDATE_CONFIDENCE_LEVEL_SQL, error=_PRIVILEGE_ERROR
        ),
    )
    _install_apis(monkeypatch)

    # Act
    await worker.handle_job(_job())

    # Assert: 예외 없이(ack) 초안·reports·쟁점을 커밋했고 confidence_level만 빠졌다
    assert conn.log[-4:] == ["ROLLBACK", "DELETE report_issues", "INSERT report_issues", "COMMIT"]
    assert _drafts(conn)[0]["metrics"]["confidence"]["level"] == "MEDIUM"
    assert _confidence_writes(conn) == []
