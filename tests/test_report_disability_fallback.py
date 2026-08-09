"""disability_rag 표준 장해분류표(level) 폴백 테스트 (LLM/RAG 페이크로 격리).

가입 약관(terms)에 장해분류표가 없을 때 금감원 표준 장해분류표(level namespace)로 폴백하는
경로를 검증한다:
  (a) terms에 schedule 없고 level에 있으면 폴백이 타고 caveat·신뢰도 캡(medium)이 적용된다.
  (b) enrolled_at(가입일)이 level 검색의 contract_date로 패스스루된다.
  (c) terms·level 둘 다 비면 기존 disability_schedule_missing 동작을 유지한다.
"""

import datetime

from report_worker.nodes import agents


def _make_fake_search(calls: list[dict], terms_chunks: list[dict], level_chunks: list[dict]):
    """namespace별로 다른 결과를 주고 호출 인자를 기록하는 페이크 hybrid.search."""

    async def _fake_search(
        query: str,
        insurance_type: str | None = None,
        namespaces: list[str] | None = None,
        top_k: int = 8,
        *,
        insurer: str | None = None,
        product: str | None = None,
        contract_date: datetime.date | None = None,
    ) -> dict:
        calls.append({"namespaces": namespaces, "contract_date": contract_date})
        if namespaces == ["level"]:
            return {"ranked_chunks": level_chunks, "citations": ["[별표2, 표준]"]}
        return {"ranked_chunks": terms_chunks, "citations": ["[제5조, 가입상품]"]}

    return _fake_search


async def _fake_chat_json_one_item(messages: list) -> dict:
    """검증 가능한 장해 항목 1건을 반환(rate 숫자가 원문에 존재 → verified)."""
    return {
        "items": [
            {
                "injury": "한 팔의 손목관절 심한 장해",
                "body_region": "팔",
                "category_label": "한 팔의 3대 관절 중 1관절의 기능을 완전히 잃음",
                "rate": 20,
                "rate_quote": "지급률 20%",
                "temporary": False,
                "temporary_years": None,
                "citation": "[별표2, 표준]",
            }
        ],
        "uncertain": False,
    }


async def test_fallback_to_standard_schedule_caps_confidence(monkeypatch) -> None:
    # Arrange: terms엔 schedule 청크 없음, level엔 표준 장해분류표 청크 있음
    calls: list[dict] = []
    terms_chunks = [
        {"text": "일반 보상 조항", "source_ref": "[제5조, 가입상품]", "chunk_type": "clause"}
    ]
    level_chunks = [
        {
            "text": "한 팔의 3대 관절 중 1관절의 기능을 완전히 잃음 … 지급률 20%",
            "source_ref": "[별표2, 표준]",
            "chunk_type": "schedule_rows",
        }
    ]
    monkeypatch.setattr(
        agents.hybrid, "search", _make_fake_search(calls, terms_chunks, level_chunks)
    )
    monkeypatch.setattr(agents.ai_client, "chat_json", _fake_chat_json_one_item)

    state = {
        "case_info": {"diagnosis": "손목관절 장해", "enrolled_at": datetime.date(2020, 3, 1)},
        "diagnosis": {"diagnosis": "손목관절 장해", "icd_codes": []},
    }

    # Act
    out = await agents.disability_rag(state)

    # Assert: 폴백 마커·표준표 caveat·신뢰도 medium 캡
    assert "disability_fallback_standard_schedule" in out["errors"]
    da = out["disability_analysis"]
    assert da["caveat"] == agents._STANDARD_CAVEAT
    assert da["confidence"] == "medium"  # 원래 high였어야 하나 표준표라 medium으로 캡
    assert da["items"] and da["items"][0]["verified"]


async def test_fallback_passes_enrolled_at_as_contract_date(monkeypatch) -> None:
    # Arrange
    calls: list[dict] = []
    level_chunks = [
        {"text": "… 지급률 20%", "source_ref": "[별표2, 표준]", "chunk_type": "schedule_rows"}
    ]
    monkeypatch.setattr(agents.hybrid, "search", _make_fake_search(calls, [], level_chunks))
    monkeypatch.setattr(agents.ai_client, "chat_json", _fake_chat_json_one_item)

    enrolled = datetime.date(2018, 7, 12)
    state = {
        "case_info": {"diagnosis": "손목관절 장해", "enrolled_at": enrolled},
        "diagnosis": {"diagnosis": "손목관절 장해", "icd_codes": []},
    }

    # Act
    await agents.disability_rag(state)

    # Assert: level 검색 호출에 contract_date=enrolled_at 전달
    level_calls = [call for call in calls if call["namespaces"] == ["level"]]
    assert level_calls and level_calls[0]["contract_date"] == enrolled


async def test_no_schedule_anywhere_keeps_missing(monkeypatch) -> None:
    # Arrange: terms·level 둘 다 schedule 없음
    calls: list[dict] = []
    monkeypatch.setattr(agents.hybrid, "search", _make_fake_search(calls, [], []))
    monkeypatch.setattr(agents.ai_client, "chat_json", _fake_chat_json_one_item)

    state = {
        "case_info": {"diagnosis": "손목관절 장해", "enrolled_at": None},
        "diagnosis": {"diagnosis": "손목관절 장해", "icd_codes": []},
    }

    # Act
    out = await agents.disability_rag(state)

    # Assert: 기존 disability_schedule_missing 유지, 폴백 마커 없음
    assert "disability_schedule_missing" in out["errors"]
    assert "disability_fallback_standard_schedule" not in out["errors"]
    assert out["disability_analysis"]["items"] == []
