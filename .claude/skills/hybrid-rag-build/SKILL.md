---
name: hybrid-rag-build
description: Hybrid RAG 검색 파이프라인(04번)을 구현할 때 사용. 쿼리 라우터(namespace 조합), pg_trgm trigram 오타보정, tsvector 키워드 검색, pgvector 벡터 검색(qwen3:embedding 1024d), RRF 통합, 메타데이터 역추적(인용 생성)을 다룬다. RAG·하이브리드 검색·재순위화·임베딩 검색·인용 근거 작업 시 사용.
---

# Hybrid RAG Build

노션 04번 파이프라인. 리포트(05)·챗봇(12)이 **함수 호출**로 쓰는 공용 모듈(`src/rag/`). 순수 함수형 인터페이스(in/out 명확)로 만들어 호출자가 조립하기 쉽게 한다. DB는 `core/db.py`, 임베딩은 `core/ai_client.py`를 쓴다.

## 파이프라인 (순서)
1. **쿼리 라우터** — 쿼리 + 보험 유형 힌트로 검색할 namespace 조합과 top-k 결정. namespace: `terms`(약관)·`level`(후유장해 분류표)·`case`(분쟁·판례)·`medical`(HIRA 수가·KCD). 비신체보험(자동차·화재 등)은 범위 외 안내.
2. **trigram 오타보정** — `search_terms`의 pg_trgm 인덱스로 `similarity(input, term) > SIMILARITY_THRESHOLD(0.4)` 매칭, 가장 유사한 정규 용어로 치환. 보정 쿼리가 이후 단계 입력.
3. **tsvector 키워드 검색** — 보정 쿼리로 tsvector 인덱스 top-k. 도메인 용어("상해후유장해" 등) 정밀 매칭에 강점.
4. **pgvector 벡터 검색** — 보정 쿼리 임베딩(qwen3:embedding, **1024d**) → HNSW 코사인 top-k. 의미 유사성에 강점.
5. **RRF 통합** — `score = Σ 1/(RRF_K(60) + rank_i)`. 기본 가중치 0.5:0.5, namespace별 조정 가능.
6. **메타데이터 역추적** — 상위 청크에서 조항번호·별표·출처 URL을 역추적해 인용 근거 생성.

## 구현 원칙
- **3·4단계는 병렬** — `asyncio.gather`로 tsvector·벡터 검색 동시 실행. 지연을 줄인다.
- 상수는 명명: `RRF_K = 60`, `SIMILARITY_THRESHOLD = 0.4`, `EMBEDDING_DIM = 1024`.
- 임베딩 실패 시 BGE-M3 폴백, 둘 다 실패면 키워드 검색만으로 degrade하고 경고 로깅(완전 실패 회피).
- 반환은 `ranked_chunks + citations` 구조의 명확한 pydantic/dataclass. 호출자가 그대로 LLM 컨텍스트로 쓴다.
- 차원 불일치(≠1024)는 즉시 에러 — 인덱스와 어긋나면 검색이 조용히 망가진다.

## 검증
- 오타 쿼리가 정규 용어로 보정되는지(trigram).
- tsvector·벡터 결과가 RRF로 합쳐져 순위가 바뀌는지.
- 인용에 조항번호·출처가 실제 청크에서 역추적되는지.
- namespace 라우팅: 신체/비신체 분기.

## 산출물
`src/rag/*` + 테스트. 공개 함수 시그니처를 `_workspace/02_aicore_api.md`에 기록해 `agent-engineer`가 참조하게 한다.
