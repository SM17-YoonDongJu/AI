# migrations

PostgreSQL 스키마. **마이그레이션 프레임워크 없이** 순서 있는 평문 SQL 파일로 구성한다.
파일명 숫자 접두사 순서대로 적용하면 된다(멱등하게 작성됨).

## 디렉터리 = 소유 스키마 = 실행 워커

```
migrations/
  ai/      — ai_owner 전용. ocr_worker(src/ocr_worker/__main__.py)가 실행.
  corpus/  — corpus_owner 전용. corpus_worker(src/corpus_worker/__main__.py)가 실행.
```

**중요(2026-08-09, #48~#52 사건 배경):** 예전엔 `migrations/` 폴더 하나를 모든 워커가
`run_migrations()`로 통째로 실행했다. `core`/`ai`/`corpus` 스키마 분리(role별 전용 스키마)
이후 이 구조가 깨졌다 — Postgres의 `CREATE SCHEMA`/`CREATE TABLE`/`CREATE INDEX`는
`IF NOT EXISTS`가 있어도 **소유권 검사를 존재 여부 검사보다 먼저** 한다. 그래서
`ai_owner`가 `corpus.*`를, `corpus_owner`가 `ai.*`를 서로 건드리려다 두 워커가 동시에
`permission denied`로 다운됐다(실측). 지금은 디렉터리 자체를 워커별로 분리해 각자
**자기 스키마 오브젝트만** 만들도록 강제한다 — 새 마이그레이션을 추가할 때도 반드시
소유 워커에 맞는 디렉터리에 넣을 것(`ai.*` 테이블이면 `ai/`, `corpus.*`면 `corpus/`).

CI 배포 필터(`.github/workflows/deploy.yml`)도 `migrations/ai/**` → ocr 재배포,
`migrations/corpus/**` → corpus 재배포로 나뉘어 있다. `migrations/`는 Dockerfile에서
`COPY migrations ./migrations`로 이미지에 박히므로, 마이그레이션만 바꿔도 해당 워커
이미지 재빌드가 필요하다(재빌드 없인 컨테이너 재시작해도 옛 마이그레이션 그대로 — 자동으로
안 되면 `src/core/config.py`의 "CD 스모크... 재트리거" 카운터를 올려 강제 재배포).

report_worker·chatbot은 마이그레이션을 실행하지 않는다(`run_migrations()` 호출 없음) —
`core` 스키마는 backend Flyway가, `ai`/`corpus`는 위 두 워커만 관리한다.

## `ai/` 적용 순서 (ai_owner)

| 순서 | 파일 | 내용 |
|------|------|------|
| 000 | `000_extensions.sql` | `vector`/`pg_trgm` 확장 + `ai` 스키마 선행 보장 |
| 001 | `001_ocr_results.sql` | `ai.ocr_results` |
| 002 | `002_corpus_catalog.sql` | `ai.corpus_source`/`corpus_file`/`corpus_file_part`(Notion 코퍼스 카탈로그 미러) |
| 003 | `003_ocr_results_doctype_expand.sql` | `doc_type` CHECK 확장 |
| 004 | `004_ocr_results_quality.sql` | `ocr_quality` 컬럼 |
| 005 | `005_ocr_results_id_default.sql` | `id` 기본값 복구 |
| 006 | `006_ocr_results_original_delete_outbox.sql` | 원본 S3 삭제 outbox 컬럼 |
| 007 | `007_report_drafts.sql` | `ai.report_drafts`(리포트 초안, report_worker 출력) |

## `corpus/` 적용 순서 (corpus_owner)

| 순서 | 파일 | 내용 |
|------|------|------|
| 000 | `000_extensions.sql` | `vector`/`pg_trgm` 확장(ai와 중복 적용돼도 안전) + `corpus` 스키마 선행 보장 |
| 001 | `001_policy_chunks.sql` | `corpus.policy_chunks`(약관, namespace=terms) + HNSW(halfvec)·tsvector·필터 인덱스. `ai_owner`에 SELECT 자동 부여 |
| 002 | `002_case_chunks.sql` | `corpus.case_chunks`(판례·금감원 분쟁조정례, namespace=case) + HNSW·tsvector·메타·태그 인덱스. `ai_owner`에 SELECT 자동 부여 |
| 003 | `003_search_terms.sql` | `corpus.search_terms`(정규 용어 사전) + trigram GIN. `ai_owner`에 SELECT 자동 부여 |
| 004 | `004_schedule_chunks.sql` | `corpus.schedule_chunks`(후유장해분류표, namespace=level) + HNSW·tsvector·버전·body_part 인덱스. `ai_owner`에 SELECT 자동 부여 |

`namespace`는 물리 컬럼이 아니라 검색한 소스 테이블로 부여하는 파생값이다
(`policy_chunks` -> `terms`, `case_chunks` -> `case`, `schedule_chunks` -> `level`).
report_worker·chatbot(`ai_owner`)이 RAG 검색 시 이 4개 테이블을 읽는다 — 각 파일 끝의
`GRANT SELECT ... TO ai_owner`(role 존재 체크 포함, corpus_owner가 자기 소유 오브젝트에
직접 부여하므로 권한 문제 없음)가 이를 보장한다.

## 정본(canonical) 관계

`corpus/001`·`002`는 **실DB 초기화 스크립트 `tempVectorDB/init/`(01_schema.sql·03_case_chunks.sql)를
정본으로 삼아 동일 스키마**를 재현한다. 적재기(`tempVectorDB/load_cases.py`)와 검색기
(`src/rag/search.py`)가 이 스키마에 맞춰 있으므로, 두 소스는 항상 일치해야 한다.

- `embedding`은 세 청크 테이블 모두 `halfvec(1024)`이고 HNSW는 `halfvec_cosine_ops`를 쓴다.
- `case_chunks`의 구버전 스키마(`case_number`/`block_type` enum/`court_level` 등)는 폐기됐다.
  구버전 테이블이 이미 있는 DB는 `case_chunks`(및 `case_outcome`·`case_block_type` enum)를
  `DROP` 후 재적용해야 한다 — `corpus/002_case_chunks.sql` 상단 주석 참조.
- `corpus.policy_chunks`/`search_terms`는 Policy-Chunker(`db/schema.sql`)의 정의와 컬럼이
  일부 다르다(예: `search_terms`가 Policy-Chunker는 1컬럼, 여긴 3컬럼). 지금은 둘 다 데이터가
  없어 충돌이 없지만, Policy-Chunker가 이 DB에 실제 ingest하기 전에 팀 정리가 필요하다.

## 적용 방법

### A. psql (권장, 워커별로 따로 실행)

```bash
# ai_owner로 접속해 ai/ 만
export DSN="postgresql://ai_owner:<password>@<host>:5432/<db>"
for f in migrations/ai/0*.sql; do
  echo ">> $f"
  psql "$DSN" -v ON_ERROR_STOP=1 -f "$f"
done

# corpus_owner로 접속해 corpus/ 만
export DSN="postgresql://corpus_owner:<password>@<host>:5432/<db>"
for f in migrations/corpus/0*.sql; do
  echo ">> $f"
  psql "$DSN" -v ON_ERROR_STOP=1 -f "$f"
done
```

로컬 개발(docker-compose)처럼 단일 superuser(`postgres`)로 두 디렉터리를 다 적용해도
무방하다 — 소유권 문제가 애초에 없는 환경이라서다.

### B. asyncpg로 순차 실행 (워커 코드가 실제로 쓰는 방식)

```python
import asyncio, asyncpg
from core.config import settings
from core.db import run_migrations

async def main() -> None:
    pool = await asyncpg.create_pool(settings.database_url)
    try:
        # ocr_worker는 "migrations/ai", corpus_worker는 "migrations/corpus"
        applied = await run_migrations(pool, "migrations/ai")
        print(applied)
    finally:
        await pool.close()

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

`medical`(HIRA 수가·KCD)은 테이블 미존재 -> 별도 마이그레이션으로 추가(`corpus/`).
