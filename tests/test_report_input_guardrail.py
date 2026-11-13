"""입력 가드레일 노드의 도메인 검사 대상 테스트 (#103, DB·LLM 페이크로 격리).

보험증권에는 거의 항상 "○○보험 주식회사"가 찍혀 있다. input_guardrail이 청구 문서를 이은 OCR
본문에 도메인 검사를 걸던 때는 이 "주식" 때문에 실사용 리포트가 BLOCKED로 끝났다. 정작 사용자가
직접 쓴 질문(case_info의 question)은 검사하지 않았다. 여기서 고정하는 계약:
  (a) 문서 본문의 키워드, 질문 속 회사 표기, 사고 경위·추가 정보(description·
      additional_information)의 키워드로는 차단하지 않는다(route_after_input → diagnosis).
      사고 경위·추가 정보는 사실 서술이라 "요리하다가 데었다"처럼 키워드가 정상적으로 들어간다.
  (b) 질문에 도메인 밖 주제가 있으면 `input_blocked:보험·법률 외 질문({키워드})`로 차단한다.
  (c) 그래프 전체로 돌리면 차단 시 LLM을 부르지 않고 reports.status='BLOCKED'만 남긴다.

마스커는 정규식 전용으로 고정한다(운영 report 설정 USE_NER=false). 보험사·문서는 가상이다.
"""

import datetime
import uuid
from typing import Any

import pytest

from ocr_worker.masking import Masker
from ocr_worker.masking import masker as masker_module
from report_worker.graph import build_graph
from report_worker.nodes import agents

# 재현에 쓴 청구와 같은 구성(회사 표기가 찍힌 보험증권 + 진단서). 진단서에도 키워드("요리")가
# 정상적으로 들어 있다. 회사 표기 제외만으로는 문서 본문 검사가 되살아난 것을 잡지 못해서다.
_CLAIM_DOCS = [
    {
        "doc_type": "policy",
        "masked_text": "누리손해보험 주식회사 누리상해보험 피보험자 [이름] 상해입원일당 가입",
        "entities": {"insurer": "누리손해보험 주식회사"},
    },
    {
        "doc_type": "diagnosis",
        "masked_text": "병명: 손목 2도 화상(T23.2) 발병 경위: 요리 중 끓는 물에 데임",
        "entities": {"diagnosis_name": "손목 2도 화상", "icd": "T23.2"},
    },
]
_OFF_DOMAIN_TEXT = "코인 투자 손실도 보상되나요?"
_BLOCKED_REASON = "input_blocked:보험·법률 외 질문(코인)"


@pytest.fixture(autouse=True)
def regex_only_masker(monkeypatch: pytest.MonkeyPatch) -> None:
    """OCR 기본 마스커를 정규식 전용으로 고정한다(운영 report 설정: USE_NER=false)."""
    monkeypatch.setattr(masker_module, "_default_masker", Masker(use_ner=False))


class _FakeConn:
    """load_context·persist_blocked가 쓰는 쿼리만 흉내 낸다.

    PII 컬럼 값은 평문(str|None)이라 maybe_decrypt가 그대로 통과시킨다.
    """

    def __init__(
        self,
        *,
        question: str | None = None,
        description: str | None = None,
        additional_information: str | None = None,
    ) -> None:
        self.report_row = {
            "accident_type": "medical_indemnity",
            "treatment": "요추 염좌",
            "offered_amount": 0,
            "question": question,
            # reports.claim_id가 있어야 user_claims 행(description 등)을 읽는다.
            "claim_id": uuid.uuid4(),
        }
        self.claim_row = {
            "accident_date": datetime.date(2026, 11, 1),
            "accident_type": "medical_indemnity",
            "offered_amount": 0,
            "description": description,
            "additional_information": additional_information,
            "details": None,
        }
        self.executed: list[str] = []

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        return [dict(doc) for doc in _CLAIM_DOCS]

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        # user_insurances 쿼리는 상관 서브쿼리에 "FROM reports"를 포함하므로 먼저 거른다.
        if "FROM user_insurances" in query:
            return None
        if "FROM user_claims" in query:
            return self.claim_row
        if "FROM reports" in query:
            return self.report_row
        return None

    async def execute(self, query: str, *args: object) -> None:
        self.executed.append(query)


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
    return b"\x00" * 32


async def _forbidden_llm(*args: object, **kwargs: object) -> None:
    raise AssertionError("차단 경로에서 LLM을 부르면 안 된다")


def _install(monkeypatch: pytest.MonkeyPatch, conn: _FakeConn) -> None:
    monkeypatch.setattr(agents.db, "get_pool", lambda: _FakePool(conn))
    monkeypatch.setattr(agents.crypto, "get_pii_dek", _fake_pii_dek)


def _state() -> dict[str, Any]:
    return {
        "report_id": str(uuid.uuid4()),
        "ocr_result_id": str(uuid.uuid4()),
        "claim_id": str(uuid.uuid4()),
        "user_ref": "u-1",
        "doc_type": "policy",
    }


async def _run_input_stage(state: dict[str, Any]) -> dict[str, Any]:
    """그래프 앞 두 노드(load_context → input_guardrail)를 돌린 뒤의 state."""
    state = {**state, **await agents.load_context(state)}
    return {**state, **await agents.input_guardrail(state)}


# ── (a) 질문 밖의 키워드·질문 속 회사 표기는 차단 사유가 아니다 ─────────
async def test_keywords_in_claim_documents_do_not_block(monkeypatch) -> None:
    # Arrange: 증권에 "누리손해보험 주식회사", 진단서에 "요리". 질문은 비었다
    _install(monkeypatch, _FakeConn())

    # Act
    state = await _run_input_stage(_state())

    # Assert: 문서 본문은 그대로 다음 노드로 간다
    assert "누리손해보험 주식회사" in state["masked_text"]
    assert state["errors"] == []
    assert agents.route_after_input(state) == "diagnosis"


async def test_keywords_in_description_and_additional_information_do_not_block(
    monkeypatch,
) -> None:
    # Arrange: 사고 경위·추가 정보는 사실 서술이라 키워드가 정상적으로 들어간다
    conn = _FakeConn(
        description="요리하다가 끓는 물에 손목을 데었습니다.",
        additional_information="직업은 요리사이고, 지난달 코인노래방에서도 넘어진 적이 있습니다.",
    )
    _install(monkeypatch, conn)

    # Act
    state = await _run_input_stage(_state())

    # Assert
    assert state["errors"] == []
    assert agents.route_after_input(state) == "diagnosis"


async def test_company_name_in_question_does_not_block(monkeypatch) -> None:
    # Arrange: 사용자도 보험사 정식 이름을 그대로 쓴다
    _install(
        monkeypatch, _FakeConn(question="누리손해보험 주식회사에 입원일당을 청구할 수 있나요?")
    )

    # Act
    state = await _run_input_stage(_state())

    # Assert
    assert state["errors"] == []
    assert agents.route_after_input(state) == "diagnosis"


# ── (b) 질문의 도메인 밖 주제는 차단한다 ─────────────────────────
async def test_off_domain_question_blocks(monkeypatch) -> None:
    # Arrange
    _install(monkeypatch, _FakeConn(question=_OFF_DOMAIN_TEXT))

    # Act
    state = await _run_input_stage(_state())

    # Assert: 사유 형식은 예전과 같다
    assert state["errors"] == [_BLOCKED_REASON]
    assert agents.route_after_input(state) == "blocked"


# ── (c) 그래프 전체: 차단이면 LLM 없이 BLOCKED만 기록 ──────────────
async def test_graph_blocks_off_domain_question_without_llm(monkeypatch) -> None:
    # Arrange
    conn = _FakeConn(question=_OFF_DOMAIN_TEXT)
    _install(monkeypatch, conn)
    monkeypatch.setattr(agents.ai_client, "chat", _forbidden_llm)
    monkeypatch.setattr(agents.ai_client, "chat_json", _forbidden_llm)

    # Act
    final = await build_graph().ainvoke(_state())

    # Assert: LLM 노드가 돌았다면 safe_node가 남긴 실패 표식이 errors에 더 붙는다
    assert final["errors"] == [_BLOCKED_REASON]
    assert conn.executed == [agents._MARK_BLOCKED_SQL]
