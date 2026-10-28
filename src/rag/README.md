# rag — Hybrid RAG 검색 파이프라인 (04)

`report_worker`가 **함수 호출**로 쓰는 공용 모듈이다. 독립 서비스가 아니다. 공개 진입점은 `search()` 하나이고, 결과 모델(`Chunk`·`Citation`·`RagResult`)은 `core.contracts`가 단일 출처다.

## 사용

```python
from rag import search

result = await search(
    "골절 진단비 지급 조건",
    namespaces=["terms", "case"],       # None이면 라우터가 결정
    top_k=8,
    insurer="OO생명", product="OO건강보험",  # terms 메타 필터(선택)
    contract_date=date(2021, 3, 1),      # level: 계약일이 속한 분류표 버전만
)
result.ranked_chunks  # list[Chunk] — text·namespace·score(RRF)·source_ref·article_number·product_name
result.citations      # list[Citation] — clause_no(조항/사례 번호)·exhibit(별표·분류표 버전)
```

## 파이프라인

| 단계 | 모듈 | 내용 |
|---|---|---|
| 1. 쿼리 라우터 | `router.py` | namespace 조합 결정. 비신체보험(자동차·화재 등)은 범위 밖이라 빈 결과 |
| 2. 오타 보정 | `typo.py` | kiwipiepy로 내용어 추출 → `search_terms`의 pg_trgm에서 `similarity > 0.4`인 정규 용어로 치환 |
| 3. 병렬 검색 | `search.py` | tsvector 키워드 검색과 pgvector 벡터 검색을 namespace별로 동시에 실행 |
| 4. RRF 통합 | `fusion.py` | `score = Σ weight / (60 + rank)`, 키워드:벡터 = 0.5:0.5 (순수 함수) |
| 5. 인용 역추적 | `search.py` | 상위 청크의 메타데이터로 `Citation` 생성 |

## Namespace

| namespace | 테이블 | 내용 | 비고 |
|---|---|---|---|
| `terms` | `policy_chunks` | 신체보험 약관 | 기본 대상. `insurer`/`product` 필터 적용 |
| `case` | `case_chunks` | 분쟁조정 사례 | 기본 대상 |
| `level` | `schedule_chunks` | 후유장해 분류표 | 명시할 때만 검색. `contract_date`로 버전 매칭(없으면 현행판) |

`medical`(수가·KCD)은 테이블이 아직 없어 향후 확장 대상이다. 테이블 스키마는 [`migrations/corpus/`](../../migrations/corpus)에 있다.

## 임베딩과 장애 시 동작

- 기본 임베딩은 `core.ai_client.embed()`이고(qwen3 embedding, 1024d), 쿼리에는 instruction 프리픽스를 붙인다.
- 기본 임베딩이 실패하면 **BGE-M3**(`embeddings` extra, 1024d)로 폴백한다.
- 둘 다 실패하면 **키워드 검색만으로 degrade**하고, 완료 로그(`rag.search.completed`)에 `degraded=True`를 남긴다. 검색 자체는 실패시키지 않는다.

## 의존

- `core.db`(asyncpg + pgvector·tsvector·pg_trgm), `core.ai_client`, `core.contracts`
- `kiwipiepy`(base), `sentence-transformers`(선택, `embeddings` extra)
- 이 모듈을 바꾸면 report 서비스가 재배포된다(CI paths-filter).

## 참고

- [Notion 04 Hybrid RAG](../../.claude/docs/04_hybridRag.md) · [아키텍처: rag](../../docs/architecture/rag.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
- 점검 스크립트: `scripts/rag_check.py`
