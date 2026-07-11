# migrations

Hybrid RAG(04)용 PostgreSQL 스키마. **마이그레이션 프레임워크 없이** 순서 있는 평문 SQL
파일로 구성한다. 파일명 숫자 접두사 순서대로 적용하면 된다(멱등하게 작성됨).

## 적용 순서

| 순서 | 파일 | 내용 |
|------|------|------|
| 001 | `001_extensions.sql` | `vector`(pgvector), `pg_trgm` 확장 |
| 002 | `002_policy_chunks.sql` | `policy_chunks`(약관, namespace=terms) + HNSW(halfvec)·tsvector·필터 인덱스 |
| 003 | `003_case_chunks.sql` | `case_chunks`(판례·금감원 분쟁조정례, namespace=case) + HNSW(halfvec)·tsvector·메타·태그 인덱스 |
| 004 | `004_search_terms.sql` | `search_terms`(정규 용어 사전) + trigram GIN |
| 005 | `005_schedule_chunks.sql` | `schedule_chunks`(후유장해분류표, namespace=level) + HNSW(halfvec)·tsvector·버전(applies_from,applies_to)·body_part 인덱스 |

`namespace`는 물리 컬럼이 아니라 검색한 소스 테이블로 부여하는 파생값이다
(`policy_chunks` -> `terms`, `case_chunks` -> `case`).

## 정본(canonical) 관계

`002`/`003`은 **실DB 초기화 스크립트 `tempVectorDB/init/`(01_schema.sql·03_case_chunks.sql)를
정본으로 삼아 동일 스키마**를 재현한다. 적재기(`tempVectorDB/load_cases.py`)와 검색기
(`src/rag/search.py`)가 이 스키마에 맞춰 있으므로, 두 소스는 항상 일치해야 한다.

- `embedding`은 두 테이블 모두 `halfvec(1024)`이고 HNSW는 `halfvec_cosine_ops`를 쓴다.
- `003`의 구버전 스키마(`case_number`/`block_type` enum/`court_level` 등)는 폐기됐다.
  구버전 테이블이 이미 있는 DB는 `case_chunks`(및 `case_outcome`·`case_block_type` enum)를
  `DROP` 후 재적용해야 한다 — 파일 상단 주석 참조.

## 적용 방법

### A. psql (권장)

```bash
# DSN은 src/core/config.py 의 db_dsn 과 동일(기본: 로컬 docker-compose.dev)
export DSN="postgresql://postgres:postgres@localhost:5432/ai_engine"

for f in migrations/0*.sql; do
  echo ">> $f"
  psql "$DSN" -v ON_ERROR_STOP=1 -f "$f"
done
```

### B. asyncpg로 순차 실행

```python
import asyncio, pathlib, asyncpg
from core.config import settings

async def main() -> None:
    conn = await asyncpg.connect(settings.db_dsn)
    try:
        for f in sorted(pathlib.Path("migrations").glob("0*.sql")):
            await conn.execute(f.read_text(encoding="utf-8"))
    finally:
        await conn.close()

asyncio.run(main())
```

> 확장 생성(`CREATE EXTENSION`)에는 superuser 또는 그에 준하는 권한이 필요하다.
> 모든 파일은 `IF NOT EXISTS` / `DO $$ ... $$` 가드로 **재실행 안전(멱등)** 하다.

## 키워드 검색 전략 (tsvector)

04 문서의 "앱단 토큰화 -> `content_tokens` 저장 -> simple tsvector" 전략을 따른다.
각 청크 테이블은 `content_tokens` 위에 **함수식 GIN 인덱스**
( `gin (to_tsvector('simple', coalesce(content_tokens, '')))` )를 둔다. 검색기
(`src/rag/search.py`)가 바로 이 함수식으로 `@@` 매칭하므로 인덱스 표현식과 정확히 일치한다.
형태소 분석은 적재/쿼리 시 앱단(`kiwipiepy`)에서 수행해 `content_tokens`(공백 구분 토큰)에
저장하고, DB는 `'simple'` 구성으로 단순 토큰 매칭만 한다.

## 버전 매칭 (schedule_chunks / level)

후유장해분류표는 시행세칙 개정(2018.4 대개정 등)마다 판이 갈린다. 계약 체결일이 속하는
버전만 검색해야 하므로 `(version_label, applies_from, applies_to)`로 유효기간을 표현한다
(`applies_to IS NULL` = 현행판). 검색기(`src/rag/search.py`)는 `level` namespace에 한해
`contract_date` 인자로 `applies_from <= contract_date AND (applies_to IS NULL OR
contract_date < applies_to)` 필터를 건다(`contract_date=None`이면 현행판만).

## 향후 확장

`medical`(HIRA 수가·KCD)은 테이블 미존재 -> 별도 마이그레이션으로 추가.
