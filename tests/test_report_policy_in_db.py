"""policy_in_db 분기 — 가입 약관이 코퍼스에 있는지 **존재만** 묻는다(성능 실험 G3).

count(*)는 보험사 한 곳의 청크(dev 메리츠화재 38만 행)를 전부 훑어 콜드 캐시에서 44초가 걸렸다.
분기에 필요한 건 "한 건이라도 있나"뿐이라 EXISTS로 묻는다 — 이 계약과 분기 결과를 고정한다.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import pytest

from report_worker.nodes import agents


class _Conn:
    def __init__(self, found: Any) -> None:
        self._found = found
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self.queries.append((sql, args))
        return self._found


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self) -> Any:
        yield self._conn


def _install(monkeypatch: pytest.MonkeyPatch, found: Any) -> _Conn:
    conn = _Conn(found)
    monkeypatch.setattr(agents.db, "get_pool", lambda: _Pool(conn))
    return conn


def _state(insurer: str | None, product: str | None) -> dict[str, Any]:
    return {"case_info": {"insurer": insurer, "product_name": product}}


@pytest.mark.parametrize(("found", "route"), [(True, "coverage_parse"), (False, "terms_parse")])
async def test_routes_by_existence_of_subscribed_policy(
    monkeypatch: pytest.MonkeyPatch, found: bool, route: str
) -> None:
    # Arrange
    conn = _install(monkeypatch, found)

    # Act
    result = await agents.policy_in_db(_state("메리츠화재", "다모아상해보험"))

    # Assert: 보험사·상품으로 존재만 묻는다(건수를 세지 않는다)
    assert result == route
    [(sql, args)] = conn.queries
    assert sql.startswith("SELECT EXISTS")
    assert "count(" not in sql
    assert args == ("메리츠화재", "다모아상해보험")


async def test_insurer_only_when_product_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    conn = _install(monkeypatch, True)

    # Act
    result = await agents.policy_in_db(_state("메리츠화재", None))

    # Assert
    assert result == "coverage_parse"
    [(sql, args)] = conn.queries
    assert sql.startswith("SELECT EXISTS")
    assert args == ("메리츠화재",)


async def test_no_insurer_skips_db(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    conn = _install(monkeypatch, True)

    # Act
    result = await agents.policy_in_db(_state(None, "다모아상해보험"))

    # Assert: 보험사를 모르면 조회 없이 런타임 파싱 경로
    assert result == "terms_parse"
    assert conn.queries == []
