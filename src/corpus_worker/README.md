# corpus_worker — 약관 코퍼스 S3 스테이징 워커

Notion에서 관리하는 약관 카탈로그를 PG에 미러링하고, 약관 원문 PDF를 S3에 스테이징하는 **상시 데몬**이다. GPU가 필요 없는 경량 워커이며, RDS·S3·Notion으로의 아웃바운드만 있으면 된다.

범위는 **Notion 첨부를 S3에 올리는 데까지**다. 청킹·임베딩·`policy_chunks` 적재는 이 워커 범위가 아니다.

## 실행

```bash
uv sync --extra corpus               # base + boto3
uv run python -m corpus_worker       # NOTION_TOKEN, S3_BUCKET, DATABASE_URL 필요
```

기동 순서: 로깅 → DB 풀 → **`migrations/corpus/` 적용**(corpus_owner) → `in_progress` 좀비 회수(1회) → Notion·httpx·S3 클라이언트 → `sync`와 `drain`을 동시에 실행. SIGTERM/SIGINT를 받으면 새 claim을 멈추고, 진행 중인 문서를 마친 뒤 종료한다.

## 두 개의 루프

| 루프 | 주기 | 하는 일 |
|---|---|---|
| **sync** | `NOTION_SYNC_INTERVAL_SECONDS`(기본 3600초) | Notion 카탈로그 DB와 약관 DB를 PG 미러(`corpus_source`·`corpus_file`·`corpus_file_part`)에 증분 반영. `CORPUS_FULL_RECONCILE_EVERY_CYCLES`(기본 24)마다 삭제까지 반영하는 full 동기화 |
| **drain** | 상시 | 우선순위 큐에서 문서를 claim해 파트 순서대로 S3에 업로드. 동시 처리 수는 `CORPUS_MAX_CONCURRENT_UPLOADS`(기본 4) |

## 문서 1건 처리 (`pipeline.py`)

1. **PDF 검증** (`filetype.py`): 파일명이 `.pdf`가 아니면 바로 실패 처리. HWP·zip 등은 올리지 않는다.
2. **신선 URL 조회**: Notion 첨부 URL은 만료되므로 업로드 직전에 다시 받는다.
3. **스트리밍 다운로드** (`downloader.py`): 임시 파일에 청크 단위로 쓰면서 SHA256을 계산한다. 큰 파일도 메모리에 올리지 않는다.
4. **dedup 업로드** (`s3.py`): 내용 주소 키 `corpus/{category}/{sha256}.pdf`. 같은 키가 이미 있으면 건너뛴다. SSE-S3로 암호화한다.
5. **상태 전이**: 파트는 `pending → uploaded | failed`, 문서는 `pending → in_progress → done | failed`. `CORPUS_MAX_ATTEMPTS`(기본 5)를 넘기면 `failed`로 둔다.

## 우선순위 (`priority.py`, 순수 함수)

점수 = 카테고리 기본값(약관 우선) + 출처 등급(P0~P3) + 수요도(상품 종류) + 긴급도(시행일 신선도 반감기). 현재 MVP 범위는 `CORPUS_CATEGORIES=terms`다.

## 운영 스크립트 (모두 dry-run 먼저)

| 스크립트 | 용도 |
|---|---|
| `scripts/purge_non_pdf_corpus.py [--apply]` | PDF 전용 정책 이전에 올라간 비-PDF 객체를 S3와 DB에서 정리. **영구 삭제** |
| `scripts/backfill_corpus_content_type.py [--apply]` | `.pdf`로 잘못 라벨링된 과거 객체의 키와 Content-Type 재지정 |
| `scripts/check_corpus_deploy.sh` | 배포 상태 확인 |

## 의존 / 배포

- `core.db`·`core.config`·`core.logging` · httpx(Notion REST) · boto3(S3)
- DB: 카탈로그 미러 테이블은 `migrations/ai/002_corpus_catalog.sql`, 검색용 청크 테이블은 `migrations/corpus/`
- 배포: `src/corpus_worker/Dockerfile` → ECR → ETL EC2. `deploy/docker-compose.corpus.yml`, SSM으로 재시작
- `NOTION_TOKEN`은 시크릿이다. SSM이나 `.env`로 주입하고 커밋하지 않는다.

## 참고

- [migrations/README.md](../../migrations/README.md)(스키마 소유권) · [deploy/README.md](../../deploy/README.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
