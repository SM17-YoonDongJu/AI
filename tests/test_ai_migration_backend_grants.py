"""AI 마이그레이션의 backend 읽기 권한(self-grant) 대상 롤 테스트 — 파일 내용만 검사한다.

dev·prod 롤(app_owner·app_owner_prod)이 같은 RDS 인스턴스에 함께 있다. 마이그레이션이 받는 롤을
'app_owner'로 박아 두면 prod(ai_owner_prod로 접속)에서 dev 롤이 prod 테이블 권한을 받고, prod
backend(app_owner_prod)는 못 받아 조회가 permission denied로 실패한다. 그래서 받는 롤은 접속 롤에서
끌어낸다(ai_owner → app_owner, ai_owner_prod → app_owner_prod). 실제 GRANT 결과는 실 PG에서
deploy/schema_split.prod.sql 검증과 함께 확인했다 — 여기선 그 규칙이 파일에서 빠지지 않게 고정한다.
"""

import re
from pathlib import Path

import pytest

_MIGRATIONS_AI = Path(__file__).resolve().parents[1] / "migrations" / "ai"
_DERIVE_BACKEND_ROLE = (
    "backend_role text := CASE WHEN current_user ~ '^ai_owner'\n"
    "                              THEN regexp_replace(current_user, '^ai_owner', 'app_owner')\n"
    "                              ELSE 'app_owner' END;"
)
_BACKEND_GRANTS = {
    "007_report_drafts.sql": "ai.report_drafts",
    "008_ocr_job_failures.sql": "ai.ocr_job_failures",
    "011_ocr_results_app_owner_readonly.sql": "ai.ocr_results",
    "012_report_job_failures.sql": "ai.report_job_failures",
}


def _sql_code(sql: str) -> str:
    """주석을 걷어낸 SQL(주석 속 단어가 검사에 걸리지 않게)."""
    return re.sub(r"--[^\n]*", "", sql)


def test_no_ai_migration_grants_to_a_hardcoded_backend_role() -> None:
    # Arrange
    paths = sorted(_MIGRATIONS_AI.glob("*.sql"))

    # Act
    hardcoded = [
        path.name
        for path in paths
        if re.search(r"\bTO\s+app_owner\w*\b", _sql_code(path.read_text(encoding="utf-8")))
    ]

    # Assert: 롤 이름을 박은 GRANT가 없다 — 새 마이그레이션도 backend_role을 끌어내 써야 한다
    assert paths
    assert hardcoded == []


@pytest.mark.parametrize(("name", "table"), list(_BACKEND_GRANTS.items()))
def test_backend_grant_targets_role_derived_from_connecting_role(name: str, table: str) -> None:
    # Arrange
    sql = _sql_code((_MIGRATIONS_AI / name).read_text(encoding="utf-8"))

    # Act
    grants = re.findall(rf"EXECUTE format\('GRANT SELECT[^']*ON {re.escape(table)} TO %I'", sql)

    # Assert: 접속 롤에서 끌어낸 롤에, 그 롤이 있을 때만 SELECT를 준다
    assert _DERIVE_BACKEND_ROLE in sql
    assert "IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = backend_role) THEN" in sql
    assert len(grants) == 1
