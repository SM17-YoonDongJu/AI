"""리포트 워커의 외부 전송 전 PII 마스킹 테스트 (DB·LLM·RAG 페이크로 격리).

LLM(Claude)과 임베딩(Voyage)은 외부 API다(CODE_CONVENTIONS §13). 그래서 Spring DB에서
복호화한 자유입력 필드(question·description·additional_information)는 load_context가
guardrail.mask_pii를 한 번 거친 값만 case_info에 담는다. 여기서 고정하는 계약:
  (a) 세 필드는 마스킹본만 case_info에 들어간다. None은 None 그대로다.
  (b) MaskingError가 난 필드는 None으로 비우고 errors에 pii_mask_failed:<필드>를 남긴다.
      리포트 생성은 계속한다(차단 아님). 로그엔 필드명·예외 타입만 남는다.
  (c) USE_NER 설정 오류(ModuleNotFoundError)는 삼키지 않는다 — 하드 실패로 드러난다.
  (d) OCR masked_text에는 mask_pii를 다시 걸지 않는다(멱등이 아니라 날짜가 깨진다).
  (e) 그래프 전체를 돌려 외부로 나가는 인자(ai_client.chat·chat_json — Judge 포함,
      hybrid.search 쿼리)를 실제 값 그대로 캡처하고, 원문 PII 조각이 없는지 본다.

마스커는 정규식 전용으로 고정한다(autouse 픽스처). 운영 report 설정(USE_NER=false)과 같고,
셸 환경의 USE_NER와 상관없이 결과가 같게 하려는 것이다. 모든 PII는 합성 값이다.
"""

import copy
import datetime
import json
import uuid
from typing import Any
from unittest.mock import ANY

import pytest

import guardrail
from ocr_worker.masking import Masker
from ocr_worker.masking import masker as masker_module
from report_worker import worker
from report_worker.graph import build_graph
from report_worker.nodes import agents

# ── 합성 PII 샘플 ─────────────────────────────────────────────
# 이메일·전화·라벨 뒤 이름이 섞인 자유입력 원문(복호화 직후 값).
_RAW_QUESTION = (
    "성명: 홍길동, 연락처 010-1234-5678, 메일 hong.gildong@example.com 입니다. "
    "요추 염좌로 입원했는데 보험금 받을 수 있나요?"
)
_RAW_DESCRIPTION = "피보험자 김영희 사고 경위: 퇴근길 계단에서 넘어짐. 연락처 010-9876-5432"
_RAW_ADDITIONAL = "추가 연락은 kim.younghee@example.com 로 부탁드립니다"
_MASKED_QUESTION = (
    "성명: [이름], 연락처 [전화번호], 메일 [이메일] 입니다. "
    "요추 염좌로 입원했는데 보험금 받을 수 있나요?"
)
_MASKED_DESCRIPTION = "피보험자 [이름] 사고 경위: 퇴근길 계단에서 넘어짐. 연락처 [전화번호]"
_MASKED_ADDITIONAL = "추가 연락은 [이메일] 로 부탁드립니다"
# 원문에만 있고 마스킹본·외부 전송 인자에는 없어야 하는 조각.
_RAW_FRAGMENTS = (
    "홍길동",
    "010-1234-5678",
    "hong.gildong@example.com",
    "김영희",
    "010-9876-5432",
    "kim.younghee@example.com",
)
# 점으로 구분한 주민번호 — 디텍터가 놓치고 잔류 게이트가 잡아 MaskingError가 난다.
_UNMASKABLE_QUESTION = "주민번호 860312.1948571 입니다. 보험금 청구 가능한가요?"
# OCR이 이미 마스킹한 텍스트. mask_pii를 다시 걸면 입원일이 "[환자등록번호]5"로 깨진다.
_OCR_MASKED_TEXT = "환자등록번호: [환자등록번호] 입원일 2024-01-05 진단 요추 염좌 S33.5"


@pytest.fixture(autouse=True)
def regex_only_masker(monkeypatch: pytest.MonkeyPatch) -> None:
    """OCR 기본 마스커를 정규식 전용으로 고정한다(운영 report 설정: USE_NER=false)."""
    monkeypatch.setattr(masker_module, "_default_masker", Masker(use_ner=False))


# ── DB 페이크 ─────────────────────────────────────────────────
class _NullAsyncContext:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeConn:
    """load_context·policy_in_db·case_search·persist가 쓰는 쿼리만 흉내 낸다.

    DB가 돌려주는 PII 컬럼은 전부 평문(str|None)이라 maybe_decrypt가 그대로 통과시킨다 —
    즉 페이크 행의 값이 곧 "복호화 직후 원문"이다.
    """

    def __init__(
        self,
        *,
        report_row: dict[str, Any],
        claim_row: dict[str, Any],
        claim_docs: list[dict[str, Any]],
    ) -> None:
        self.report_row = report_row
        self.claim_row = claim_row
        self.claim_docs = claim_docs
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        if "FROM case_chunks" in query:
            return []  # 판례 메타 역추적(_enrich_cases) — 메타 없음
        return list(self.claim_docs)

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        if "FROM ocr_results" in query:
            return {"masked_text": _OCR_MASKED_TEXT, "entities": {"icd": "S33.5"}}
        # user_insurances 쿼리는 상관 서브쿼리에 "FROM reports"를 포함하므로 먼저 거른다.
        if "FROM user_insurances" in query:
            return {
                "insurer_name": "행복화재",
                "product_name": "행복보험",
                "coverages": json.dumps(["상해입원일당", "상해후유장해"], ensure_ascii=False),
                "enrolled_at": datetime.date(2020, 3, 1),
            }
        if "FROM user_claims" in query:
            return self.claim_row
        if "FROM reports" in query:
            return self.report_row
        return None

    async def fetchval(self, query: str, *args: object) -> int:
        return 1  # policy_in_db: 가입 약관이 policy_chunks에 있다 → coverage_parse 경로

    async def execute(self, query: str, *args: object) -> None:
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
    return b"\x00" * 32


def _make_conn(
    *,
    question: str | None,
    description: str | None,
    additional_information: str | None,
    with_claim: bool = True,
    claim_docs: list[dict[str, Any]] | None = None,
) -> _FakeConn:
    return _FakeConn(
        report_row={
            "accident_type": "medical_indemnity",
            "treatment": "요추 염좌",
            "offered_amount": 1_000_000,
            "question": question,
            # reports.claim_id가 있어야 user_claims 행(description 등)을 읽는다.
            "claim_id": uuid.uuid4() if with_claim else None,
        },
        claim_row={
            "accident_date": datetime.date(2024, 1, 5),
            "accident_type": "medical_indemnity",
            "offered_amount": 1_000_000,
            "description": description,
            "additional_information": additional_information,
            "details": None,
        },
        claim_docs=claim_docs or [],
    )


def _install_db(monkeypatch: pytest.MonkeyPatch, conn: _FakeConn) -> None:
    monkeypatch.setattr(agents.db, "get_pool", lambda: _FakePool(conn))
    monkeypatch.setattr(agents.crypto, "get_pii_dek", _fake_pii_dek)


def _state(claim_id: str | None = None) -> dict[str, Any]:
    return {
        "report_id": str(uuid.uuid4()),
        "ocr_result_id": str(uuid.uuid4()),
        "claim_id": claim_id,
        "user_ref": "u-1",
        "doc_type": "diagnosis",
    }


def _spy_mask_pii(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """실제 mask_pii를 감싸 입력을 기록한다(무엇에 몇 번 걸렸는지 보려고)."""
    inputs: list[str] = []
    real_mask_pii = guardrail.mask_pii

    def _spy(text: str) -> str:
        inputs.append(text)
        return real_mask_pii(text)

    monkeypatch.setattr(agents.guardrail, "mask_pii", _spy)
    return inputs


def _dump(value: object) -> str:
    """원문 조각을 문자열 검색할 수 있게 직렬화한다(date 등은 str)."""
    return json.dumps(value, ensure_ascii=False, default=str)


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


# ── 외부 API 페이크 ───────────────────────────────────────────
# 노드마다 자기 키만 읽으므로 응답 하나로 모든 chat_json 호출부를 통과시킨다: 진단(장해 검토
# 필요) → 특약 분석 → 판례 필터 → 장해표 추출(검증 통과 1건) → 장해 감수 → 쟁점 → Judge.
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


class _ExternalApis:
    """외부 API로 나가는 호출을 실제 인자 그대로 캡처한다.

    ai_client.chat·chat_json(Claude — 출력 가드레일 Judge도 같은 모듈 함수를 부른다)과
    hybrid.search(쿼리가 임베딩 API로 간다)가 대상이다.
    """

    def __init__(self) -> None:
        self.chat_calls: list[list[dict[str, str]]] = []
        self.chat_json_calls: list[list[dict[str, str]]] = []
        self.search_calls: list[dict[str, Any]] = []

    async def chat(self, messages: list[dict[str, str]], **opts: Any) -> str:
        self.chat_calls.append(messages)
        return "리포트 본문"

    async def chat_json(self, messages: list[dict[str, str]], **opts: Any) -> Any:
        self.chat_json_calls.append(messages)
        return copy.deepcopy(_LLM_JSON)

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
        self.search_calls.append(
            {
                "query": query,
                "insurance_type": insurance_type,
                "namespaces": namespaces,
                "insurer": insurer,
                "product": product,
                "contract_date": contract_date,
            }
        )
        chunk = _CASE_CHUNK if namespaces == ["case"] else _TERMS_CHUNK
        return {"ranked_chunks": [dict(chunk)], "citations": [chunk["source_ref"]]}

    def sent(self) -> str:
        """캡처한 인자 전부를 한 문자열로 만든다(원문 조각 검색용).

        검색 쿼리는 임베딩 API로 가지만 insurer·product·contract_date는 로컬 SQL 필터라
        외부로 나가지 않는다. 그래도 보수적으로 함께 본다.
        """
        return _dump([self.chat_calls, self.chat_json_calls, self.search_calls])


def _install_external_apis(monkeypatch: pytest.MonkeyPatch) -> _ExternalApis:
    apis = _ExternalApis()
    monkeypatch.setattr(agents.ai_client, "chat", apis.chat)
    monkeypatch.setattr(agents.ai_client, "chat_json", apis.chat_json)
    monkeypatch.setattr(agents.hybrid, "search", apis.search)
    return apis


# ── load_context: 복호화 직후 마스킹 ──────────────────────────
async def test_load_context_puts_only_masked_free_text_in_case_info(monkeypatch) -> None:
    # Arrange
    _install_db(
        monkeypatch,
        _make_conn(
            question=_RAW_QUESTION,
            description=_RAW_DESCRIPTION,
            additional_information=_RAW_ADDITIONAL,
        ),
    )

    # Act
    out = await agents.load_context(_state())

    # Assert: 세 필드 모두 토큰으로 바뀌었다
    assert out["case_info"]["question"] == _MASKED_QUESTION
    assert out["case_info"]["description"] == _MASKED_DESCRIPTION
    assert out["case_info"]["additional_information"] == _MASKED_ADDITIONAL
    # Assert: 노드 출력(= state에 머지되는 값) 어디에도 원문 조각이 없다
    assert [f for f in _RAW_FRAGMENTS if f in _dump(out)] == []
    assert out["errors"] == []


@pytest.mark.parametrize(
    "with_claim",
    [pytest.param(False, id="no-claim-row"), pytest.param(True, id="null-columns")],
)
async def test_load_context_keeps_missing_free_text_as_none(monkeypatch, with_claim: bool) -> None:
    # Arrange: 청구 행이 없거나, 있어도 세 컬럼이 NULL
    _install_db(
        monkeypatch,
        _make_conn(
            question=None, description=None, additional_information=None, with_claim=with_claim
        ),
    )
    mask_inputs = _spy_mask_pii(monkeypatch)

    # Act
    out = await agents.load_context(_state())

    # Assert: None은 None 그대로, mask_pii에는 넘기지 않는다
    assert out["case_info"]["question"] is None
    assert out["case_info"]["description"] is None
    assert out["case_info"]["additional_information"] is None
    assert mask_inputs == []
    assert out["errors"] == []


async def test_load_context_empties_field_when_masking_fails(monkeypatch) -> None:
    # Arrange: question에 점 구분 주민번호 — 잔류 게이트가 MaskingError로 거절한다
    _install_db(
        monkeypatch,
        _make_conn(
            question=_UNMASKABLE_QUESTION,
            description=_RAW_DESCRIPTION,
            additional_information=None,
        ),
    )
    recorder = _LogRecorder()
    monkeypatch.setattr(agents, "logger", recorder)

    # Act
    out = await agents.load_context(_state())

    # Assert: 그 필드만 비우고 PII 없는 표식을 남긴다. 다른 필드는 마스킹본 그대로다
    assert out["case_info"]["question"] is None
    assert out["case_info"]["description"] == _MASKED_DESCRIPTION
    assert out["errors"] == ["pii_mask_failed:question"]
    # Assert: 차단이 아니다 — 리포트 생성 경로로 계속 간다
    assert agents.route_after_input(out) == "diagnosis"
    # Assert: 로그엔 필드명·예외 타입만, 원문은 노드 출력·로그 어디에도 없다
    assert recorder.records == [
        (
            "warning",
            "free text pii masking failed",
            {"field": "question", "error_type": "MaskingError"},
        )
    ]
    assert "860312" not in _dump([out, recorder.records])
    assert "1948571" not in _dump([out, recorder.records])


async def test_load_context_does_not_swallow_missing_ner_dependency(monkeypatch) -> None:
    # Arrange: USE_NER=true인데 이미지에 transformers가 없다(설정 오류)
    _install_db(
        monkeypatch,
        _make_conn(question=_RAW_QUESTION, description=None, additional_information=None),
    )

    def _ner_unavailable(text: str) -> str:
        raise ModuleNotFoundError("No module named 'transformers'")

    monkeypatch.setattr(agents.guardrail, "mask_pii", _ner_unavailable)

    # Act
    out = await agents.load_context(_state())

    # Assert: 필드를 조용히 비우지 않는다 — worker가 재전달로 올리는 하드 실패로 드러난다
    # (node_trace는 safe_node가 붙이는 리포트 수치용 소요 시간이다)
    assert out == {
        "errors": ["load_context_failed:ModuleNotFoundError"],
        "node_trace": [{"node": "load_context", "ms": ANY, "status": "failed"}],
    }
    assert out["errors"][0].startswith(worker._HARD_FAILURE_PREFIXES)


@pytest.mark.parametrize(
    "claim_id",
    [pytest.param(None, id="single-document"), pytest.param("claim-1", id="claim-documents")],
)
async def test_load_context_does_not_remask_ocr_masked_text(
    monkeypatch, claim_id: str | None
) -> None:
    # Arrange: OCR이 이미 마스킹한 텍스트(단건 조회·청구 문서 병합 두 경로 모두)
    _install_db(
        monkeypatch,
        _make_conn(
            question=_RAW_QUESTION,
            description=None,
            additional_information=None,
            claim_docs=[{"doc_type": "diagnosis", "masked_text": _OCR_MASKED_TEXT, "entities": {}}],
        ),
    )
    mask_inputs = _spy_mask_pii(monkeypatch)

    # Act
    out = await agents.load_context(_state(claim_id))

    # Assert: OCR 텍스트는 손대지 않았고(입원일이 살아 있다), mask_pii는 자유입력 원문에만 걸렸다
    assert _OCR_MASKED_TEXT in out["masked_text"]
    assert mask_inputs == [_RAW_QUESTION]


# ── coverage_analysis: RAG 쿼리 ───────────────────────────────
async def test_coverage_query_omits_missing_question(monkeypatch) -> None:
    # Arrange: question이 NULL이거나 마스킹 실패로 비었다(None)
    apis = _install_external_apis(monkeypatch)
    state = {
        "case_info": {"question": None, "insurer": None, "product_name": None},
        "diagnosis": {"diagnosis": "요추 염좌", "icd_codes": ["S33.5"]},
    }

    # Act
    await agents.coverage_analysis(state)

    # Assert: "None" 글자가 쿼리(→ 임베딩 API·키워드 검색)에 들어가지 않는다
    assert apis.search_calls[0]["query"] == "요추 염좌 S33.5"


# ── 그래프 전체: 외부로 나가는 인자 전수 캡처 ─────────────────
async def test_graph_sends_no_raw_free_text_to_external_apis(monkeypatch) -> None:
    # Arrange: 청구 문서 경로(운영 기본), 세 자유입력 모두 PII 포함
    conn = _make_conn(
        question=_RAW_QUESTION,
        description=_RAW_DESCRIPTION,
        additional_information=_RAW_ADDITIONAL,
        claim_docs=[
            {
                "doc_type": "diagnosis",
                "masked_text": "진단명: 요추 염좌(S33.5) 환자 성명: [이름] 입원 5일",
                "entities": {"icd": "S33.5", "admission_days": 5},
            }
        ],
    )
    _install_db(monkeypatch, conn)
    apis = _install_external_apis(monkeypatch)
    mask_inputs = _spy_mask_pii(monkeypatch)

    # Act
    final = await build_graph().ainvoke(_state("claim-1"))

    # Assert: 외부 호출부가 전부 불렸다 — LLM 8곳(chat 1 + chat_json 7: 진단·특약·판례 필터·
    # 장해표·장해 감수·쟁점·Judge), 검색 3곳(특약·판례·장해). 수가 바뀌면 전수 점검을 다시 한다
    assert len(apis.chat_calls) == 1
    assert len(apis.chat_json_calls) == 7
    assert len(apis.search_calls) == 3
    # Assert: 원문 PII 조각이 외부로 나간 인자 어디에도 없다
    assert [f for f in _RAW_FRAGMENTS if f in apis.sent()] == []
    # Assert: question은 빠진 게 아니라 마스킹본으로 들어갔다(검색 순서: 특약 → 판례 → 장해)
    assert apis.search_calls[0]["query"] == f"요추 염좌 S33.5 {_MASKED_QUESTION}"
    assert f"질문: {_MASKED_QUESTION}\n" in apis.chat_calls[0][1]["content"]
    # Assert: mask_pii는 자유입력 원문 3개에 한 번씩만 걸렸다(OCR 텍스트·마스킹본 재적용 없음)
    assert sorted(mask_inputs) == sorted([_RAW_QUESTION, _RAW_DESCRIPTION, _RAW_ADDITIONAL])
    # Assert: 리포트는 정상 저장됐다
    assert final["errors"] == []
    assert any("status = 'AWAITING_ADOPTION'" in q for q, _ in conn.executed)


async def test_graph_keeps_generating_without_unmaskable_question(monkeypatch) -> None:
    # Arrange: question을 마스킹할 수 없다(점 구분 주민번호 → MaskingError)
    conn = _make_conn(
        question=_UNMASKABLE_QUESTION, description=_RAW_DESCRIPTION, additional_information=None
    )
    _install_db(monkeypatch, conn)
    apis = _install_external_apis(monkeypatch)

    # Act
    final = await build_graph().ainvoke(_state())

    # Assert: 차단 없이 끝까지 저장했고, 손해사정사가 볼 추가확인 섹션에 표식이 남는다
    assert final["errors"] == ["pii_mask_failed:question"]
    assert final["sections"]["7_추가확인필요"] == "pii_mask_failed:question"
    assert any("status = 'AWAITING_ADOPTION'" in q for q, _ in conn.executed)
    # Assert: question은 외부로 나가지 않았다(쿼리엔 진단·ICD만)
    assert "860312" not in apis.sent()
    assert "1948571" not in apis.sent()
    assert apis.search_calls[0]["query"] == "요추 염좌 S33.5"
