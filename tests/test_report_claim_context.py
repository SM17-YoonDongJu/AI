"""load_context의 청구(claim) 단위 문서 병합 테스트 (DB·LLM 페이크로 격리).

ocr_worker의 fan-in은 청구 1건당 ReportJob 1건을 발행하는데, 계약상 단일값인
``ocr_result_id``/``doc_type``에는 대표 문서(증권 우선) 하나만 실린다. 그래서
report_worker가 대표 문서 하나만 읽으면 진단서·영수증 내용이 리포트에 아예 반영되지
않았다. 여기서 고정하는 계약:
  (a) ``claim_id``가 있으면 청구의 전 문서를 doc_index 순으로 읽어 텍스트·엔티티를 병합한다.
  (b) ``claim_id``가 없거나 청구 문서가 0건이면 기존 ``ocr_result_id`` 단건 조회로 폴백한다.
  (c) 둘 다 비면 ``ocr_result_missing``(worker의 하드 실패 마커)이 남는다.
"""

import json
import uuid

from report_worker.nodes import agents

_REPORT_ROW = {
    "accident_type": "traffic",
    "treatment": "요추 염좌",
    "offered_amount": 0,
    "question": None,
    "claim_id": None,
}


class _FakeConn:
    def __init__(self, claim_docs: list[dict], ocr_row: dict | None) -> None:
        self.claim_docs = claim_docs
        self.ocr_row = ocr_row
        self.queries: list[tuple[str, tuple]] = []

    async def fetch(self, query: str, *args: object) -> list[dict]:
        self.queries.append((query, args))
        return list(self.claim_docs)

    async def fetchrow(self, query: str, *args: object) -> dict | None:
        self.queries.append((query, args))
        if "FROM ocr_results" in query:
            return self.ocr_row
        # user_insurances 쿼리는 상관 서브쿼리에 "FROM reports"를 포함하므로 먼저 거른다.
        if "FROM user_insurances" in query or "FROM user_claims" in query:
            return None
        if "FROM reports" in query:
            return dict(_REPORT_ROW)
        return None


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
    """페이크 DB가 돌려주는 컬럼은 전부 평문이라 실제 복호화는 일어나지 않는다."""
    return b"\x00" * 32


def _install(monkeypatch, claim_docs: list[dict], ocr_row: dict | None) -> _FakeConn:
    conn = _FakeConn(claim_docs, ocr_row)
    monkeypatch.setattr(agents.db, "get_pool", lambda: _FakePool(conn))
    monkeypatch.setattr(agents.crypto, "get_pii_dek", _fake_pii_dek)
    return conn


def _state(claim_id: str | None) -> dict:
    return {
        "report_id": str(uuid.uuid4()),
        "ocr_result_id": str(uuid.uuid4()),
        "claim_id": claim_id,
        "user_ref": "u-1",
        "doc_type": "policy",
    }


# ── 청구 전 문서 병합 ─────────────────────────────────────────
async def test_claim_merges_all_documents_into_masked_text(monkeypatch) -> None:
    # Arrange: 청구 1건에 증권·진단서·입퇴원확인서 3문서(대표는 증권 하나뿐)
    conn = _install(
        monkeypatch,
        claim_docs=[
            {
                "doc_type": "policy",
                "masked_text": "증권: 상해후유장해 특약 가입",
                "entities": {"insurer": "OO화재", "product": "행복보험"},
            },
            {
                "doc_type": "diagnosis",
                "masked_text": "진단: 요추 염좌 S33.5",
                "entities": {"diagnosis_name": "요추 염좌", "icd": "S33.5"},
            },
            {
                "doc_type": "hospitalization_cert",
                "masked_text": "입원 12일 수술 없음",
                "entities": {"admission_days": 12, "surgery": False},
            },
        ],
        ocr_row={"masked_text": "대표문서만", "entities": None},
    )

    # Act
    out = await agents.load_context(_state("claim-1"))

    # Assert: 세 문서 본문이 모두 들어가고 문서 경계가 표시된다
    text = out["masked_text"]
    assert "증권: 상해후유장해 특약 가입" in text
    assert "진단: 요추 염좌 S33.5" in text
    assert "입원 12일 수술 없음" in text
    assert "--- 문서 1: 보험증권 ---" in text
    assert "--- 문서 2: 진단서 ---" in text
    assert "--- 문서 3: 입퇴원확인서 ---" in text
    assert text.index("증권") < text.index("진단:") < text.index("입원 12일")
    # Assert: 대표 문서 단건 조회는 아예 타지 않는다
    assert not any("FROM ocr_results WHERE id = $1" in q for q, _ in conn.queries)
    assert not out["errors"]


async def test_claim_merges_entities_across_doc_types(monkeypatch) -> None:
    # Arrange: payment_calc가 읽는 키들이 서로 다른 문서에 흩어져 있다
    _install(
        monkeypatch,
        claim_docs=[
            {
                "doc_type": "diagnosis",
                "masked_text": "진단",
                "entities": {"diagnosis_name": "요추 염좌", "icd": "S33.5"},
            },
            {
                "doc_type": "hospitalization_cert",
                "masked_text": "입원",
                "entities": {"admission_days": 12, "surgery": True},
            },
        ],
        ocr_row=None,
    )

    # Act
    out = await agents.load_context(_state("claim-1"))

    # Assert
    assert out["entities"] == {
        "diagnosis_name": "요추 염좌",
        "icd": "S33.5",
        "admission_days": 12,
        "surgery": True,
    }


async def test_claim_entities_accept_json_string_column(monkeypatch) -> None:
    # Arrange: asyncpg가 jsonb를 문자열로 돌려주는 환경(코덱 미등록)
    _install(
        monkeypatch,
        claim_docs=[
            {
                "doc_type": "diagnosis",
                "masked_text": "진단",
                "entities": json.dumps({"icd": "S33.5"}),
            }
        ],
        ocr_row=None,
    )

    # Act
    out = await agents.load_context(_state("claim-1"))

    # Assert
    assert out["entities"] == {"icd": "S33.5"}


# ── 폴백(하위 호환) ───────────────────────────────────────────
async def test_single_document_report_without_claim_id_keeps_legacy_behavior(monkeypatch) -> None:
    # Arrange: claim에 묶이지 않은 기존 경로
    conn = _install(
        monkeypatch,
        claim_docs=[{"doc_type": "policy", "masked_text": "청구문서", "entities": {}}],
        ocr_row={"masked_text": "단일 문서 원문", "entities": {"icd": "S82.1"}},
    )

    # Act
    out = await agents.load_context(_state(None))

    # Assert: 단건 원문 그대로(문서 헤더 없음) + 청구 조회 미수행
    assert out["masked_text"] == "단일 문서 원문"
    assert out["entities"] == {"icd": "S82.1"}
    assert "--- 문서" not in out["masked_text"]
    assert not any("WHERE claim_id = $1" in q for q, _ in conn.queries)
    assert not out["errors"]


async def test_claim_with_no_documents_falls_back_to_single_lookup(monkeypatch) -> None:
    # Arrange: claim_id는 있는데 청구 문서 조회가 0건(예외적)
    conn = _install(
        monkeypatch,
        claim_docs=[],
        ocr_row={"masked_text": "대표 문서 원문", "entities": {"icd": "S33.5"}},
    )

    # Act
    out = await agents.load_context(_state("claim-1"))

    # Assert: 안전망으로 대표 문서 단건 조회를 탄다
    assert out["masked_text"] == "대표 문서 원문"
    assert out["entities"] == {"icd": "S33.5"}
    assert any("WHERE claim_id = $1" in q for q, _ in conn.queries)
    assert not out["errors"]


async def test_missing_everything_records_ocr_result_missing(monkeypatch) -> None:
    # Arrange: 청구 문서도 대표 문서 행도 없다
    _install(monkeypatch, claim_docs=[], ocr_row=None)

    # Act
    out = await agents.load_context(_state("claim-1"))

    # Assert: worker가 하드 실패로 승격하는 마커가 정확히 남는다
    assert out["errors"] == ["ocr_result_missing"]
    assert out["masked_text"] == ""
    assert out["entities"] == {}


# ── 순수 병합 로직 ────────────────────────────────────────────
def test_merge_entities_does_not_overwrite_value_with_none() -> None:
    # Arrange: 진단서 2장 — 뒤 장은 ICD 추출 실패(None)
    docs = [
        {"entities": {"icd": "S33.5", "diagnosis_name": "요추 염좌"}},
        {"entities": {"icd": None, "diagnosis_name": "경추 염좌"}},
    ]

    # Act
    merged = agents._merge_claim_entities(docs)

    # Assert: 앞 문서의 성공값은 유지, 실제 값이 있는 키는 뒤 문서가 덮어쓴다
    assert merged == {"icd": "S33.5", "diagnosis_name": "경추 염좌"}


def test_merge_entities_keeps_none_when_no_document_extracted_the_key() -> None:
    # Arrange
    docs = [{"entities": {"surgery": None}}]

    # Act / Assert: 단건 경로와 같은 모양(키 존재, 값 None)을 유지한다
    assert agents._merge_claim_entities(docs) == {"surgery": None}


def test_merge_texts_labels_unknown_doc_type_with_raw_value() -> None:
    # Arrange: DocType이 늘어나 라벨 매핑에 없는 값이 와도 죽지 않아야 한다
    docs = [{"doc_type": "new_type", "masked_text": "본문"}]

    # Act / Assert
    assert agents._merge_claim_texts(docs) == "--- 문서 1: new_type ---\n본문"
