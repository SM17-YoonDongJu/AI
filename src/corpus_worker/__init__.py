"""Corpus Worker (이슈 #35) — 약관 코퍼스 S3 스테이징 파이프라인.

P1(현재 범위)은 **읽기 전용 카탈로그 동기화 계층**만 구현한다: Notion(공식 REST API)의
"데이터 출처 카탈로그"·"보험 약관 파일" 두 DB를 PostgreSQL(corpus_source·corpus_file·
corpus_file_part)로 미러링한다. 다운로드/S3 업로드/청킹/임베딩은 P2 이후 범위다.

모듈 구성:
- ``notion_source``: 공식 Notion REST 클라이언트(async·레이트리밋·재시도)와 property 파싱.
- ``repository``: asyncpg upsert(멱등)·파트 동기화·아카이브·증분 커서(순수 I/O 경계).
- ``priority``: 우선순위 점수 계산 seam(P1은 tier_base만).
- ``sync``: 한 사이클 오케스트레이션(부수효과와 순수 매핑 로직 분리).
"""
