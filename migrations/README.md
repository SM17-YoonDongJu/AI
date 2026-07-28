# migrations

Hybrid RAG(04)용 PostgreSQL 스키마. **마이그레이션 프레임워크 없이** 순서 있는 평문 SQL
파일로 구성한다. 파일명 숫자 접두사 순서대로 적용하면 된다(멱등하게 작성됨).

## 적용 순서

| 순서 | 파일 | 내용 |
|------|------|------|
| 001 | `001_extensions.sql` | `vector`(pgvector), `pg_trgm` 확장 |
| 002 | `002_policy_chunks.sql` | `policy_chunks`(약관, namespace=terms) + HNSW·tsvector 인덱스 |
| 003 | `003_case_chunks.sql` | `case_outcome`/`case_block_type` enum + `case_chunks`(분쟁조정, namespace=case) + HNSW·tsvector·필터 인덱스 |
| 004 | `004_search_terms.sql` | `search_terms`(정규 용어 사전) + trigram GIN |

`namespace`는 물리 컬럼이 아니라 검색한 소스 테이블로 부여하는 파생값이다
(`policy_chunks` -> `terms`, `case_chunks` -> `case`).

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
각 청크 테이블은 STORED generated 컬럼 `content_tsv`
( `to_tsvector('simple', coalesce(content_tokens, ''))` )를 두고 그 위에 GIN 인덱스를 건다.
형태소 분석은 적재/쿼리 시 앱단(`kiwipiepy`)에서 수행해 `content_tokens`(공백 구분 토큰)에
저장하고, DB는 `'simple'` 구성으로 단순 토큰 매칭만 한다.

## 향후 확장

`level`(후유장해 분류표), `medical`(HIRA 수가·KCD)은 테이블 미존재 -> 별도 마이그레이션으로 추가.
