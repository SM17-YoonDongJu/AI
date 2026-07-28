"""Corpus Worker (이슈 #35) — 약관 코퍼스 S3 스테이징 파이프라인.

**상시 데몬**(``python -m corpus_worker``)으로, Notion 카탈로그를 PG로 미러링하고(P1)
우선순위 큐에서 약관 첨부를 S3로 스테이징한다(P2 — Notion 첨부 → S3까지). 청킹·임베딩·
OCR은 범위 밖이다.

모듈 구성:
- ``notion_source``: 공식 Notion REST 클라이언트(async·레이트리밋·재시도), property 파싱,
  업로드 직전 신선 첨부 URL 재조회(``file_urls``).
- ``repository``: asyncpg 미러 upsert(멱등)·파트 동기화·아카이브·증분 커서,
  우선순위 큐 claim(SKIP LOCKED)·파트 진행·완료/실패 전이·좀비 회수(순수 I/O 경계).
- ``priority``: 우선순위 점수(카테고리 base + tier + 수요도 + 긴급도, 순수 함수).
- ``sync``: 카탈로그 동기화 한 사이클 오케스트레이션(부수효과와 순수 매핑 분리).
- ``downloader``: httpx 스트리밍 다운로드 + 증분 SHA256(대용량 메모리 미상주).
- ``s3``: 내용주소 dedup HeadObject + 멀티파트 ``upload_file``(스레드 격리).
- ``pipeline``: 문서 1건 스테이징(멱등 재개·dedup·로컬 미잔류).
- ``__main__``: sync·drain 두 태스크를 도는 상시 워커(우아한 종료).
"""
