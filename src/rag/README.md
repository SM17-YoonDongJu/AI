# rag — Hybrid RAG 검색 파이프라인 (04)

`report_worker`·`chatbot`이 **함수 호출**로 공유하는 공용 모듈. 독립 서비스 아님. 순수 함수형 인터페이스(입력→출력 명확)로 만들어 호출자가 조립하기 쉽게 한다.

## 파이프라인

1. **쿼리 라우터** — namespace 조합(terms·level·case·medical) + top-k 결정 (비신체보험은 범위 외)
2. **trigram 오타보정** — `search_terms` pg_trgm, `similarity > 0.4`로 정규 용어 치환
3. **tsvector 키워드 검색** + **pgvector 벡터 검색**(임베딩 1024d) **병렬 실행**
4. **RRF 통합** — `score = Σ 1/(60 + rank)`
5. **메타데이터 역추적** — 조항번호·별표·출처 URL 인용 생성

## 입력 / 출력 (계약)

- **입력**: 쿼리 텍스트 + 보험유형/namespace 힌트
- **출력**: `ranked_chunks` + `citations` (호출자가 LLM 컨텍스트로 사용)

## 의존

- `core.db`(asyncpg, pgvector·tsvector·pg_trgm) · `core.ai_client`(임베딩, **모델 미정**)

## 참고

- [Notion 04 Hybrid RAG](../../.claude/docs/04_hybridRag.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
- 상수 명명: `RRF_K=60`, `SIMILARITY_THRESHOLD=0.4`, `EMBEDDING_DIM=1024`
