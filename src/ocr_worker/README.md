# ocr_worker — OCR 워커 (02)

`ocr-job-queue` SQS 메시지를 소비해 문서를 OCR·분류·추출·마스킹하고 `ai.ocr_results`에 저장하는 워커다. 청구 단위로 fan-in을 판정해 `report-job`을 발행한다. **surya-ocr(PyTorch)를 프로세스 안에서 실행하므로 GPU 노드에 배포**한다.

## 실행

```bash
uv sync --extra ocr                  # surya-ocr·pymupdf·pillow·boto3 (Linux + CUDA)
uv run python -m ocr_worker
```

기동 순서: 로깅 → DB 풀 → **`migrations/ai/` 적용**(ai_owner) → `SqsProducer` → 원본 삭제 스윕 task와 `SqsConsumer.run()`을 함께 실행. SIGTERM/SIGINT를 받으면 우아하게 종료한다.

## 처리 흐름 (`pipeline.py`)

| # | 단계 | 모듈 | 비고 |
|---|---|---|---|
| 0 | 멱등 단락 | `pipeline.py` | 이미 저장된 `job_id`면 OCR을 건너뛰고 발행·판정만 재개 |
| 1 | S3 원본 다운로드 | `storage.py` | 메모리로만 받는다(로컬 디스크에 남기지 않음) |
| 2 | 렌더 + OCR | `ocr.py` | PDF/이미지 → 페이지 이미지 → surya. 라인 단위 `OcrLine(text·bbox·polygon·confidence)` |
| 3 | 분류 | `classify.py` | 규칙 기반 `DocType` 판정. 업로드 힌트를 반영하고, 단서가 약하면 `other` |
| 4 | 엔티티 추출 | `extract.py` | KCD·진단명·보험사·상품·금액 등 **비-PII 값만** 뽑는다 |
| 5 | VLM 보조 | `vlm_client.py` | 표 문서이거나 평균 신뢰도가 0.90 미만일 때만. 표를 Markdown으로 전사하고, surya 원문과 겹침(groundedness)을 검증한 뒤 채택. 전사 결과도 6단계와 같은 마스킹을 거친다 |
| 6 | 텍스트 마스킹 | `masking/` | 정규식(항상) ∪ NER(이름, `USE_NER=true`일 때). 마스킹 후 잔류 스캔을 하고, 남아 있으면 `MaskingError`(fail-closed) |
| 7 | 이미지 마스킹 | `masking/image_masker.py` | PII 라인을 검은 블록으로 가린 비식별 사본을 S3에 올린다. 저신뢰도면 VLM grounding으로 보강 |
| 8 | 품질 판정 | `pipeline.py` | 신뢰도 < 0.90 이면서 (이름 미검출 또는 도메인 정보 없음)이면 `needs_reupload` |
| 9 | 저장 | `repository.py` | `ai.ocr_results`에 `job_id` 업서트. 원본 삭제 outbox 상태도 같은 문에서 기록 |
| 10 | 청구 fan-in | `pipeline.py` | 종결 카운트를 원자적으로 올리고, 마지막 문서면 판정 후 발행(아래) |
| 11 | 원본 삭제 | `storage.py` | 비식별 사본 검증(bbox 커버리지 + 재OCR 잔류)을 **통과한 경우에만**. 백그라운드로 실행 |

## 청구 fan-in (`ai.claim_readiness`)

- **종결 = 성공 + 확정 실패 + poison 소진**. 실패 문서도 세야 청구가 영영 멈추지 않는다.
- 종결 카운트는 `job_id` 집합 기반이라 문서별로 멱등하다. 중복 전달돼도 수가 늘지 않는다.
- 마지막 문서를 처리한 워커만 판정한다. 무엇이 인식됐는지는 카운터가 아니라 `ocr_results`를 다시 조회해 확인한다.

| 판정 | 상태 | 발행 |
|---|---|---|
| 필수 유형(증권과 진단서) 충족 | `published` | `ReportJob(ocr_quality=ok)` 청구당 1건 |
| 필수 유형 누락 또는 인식 불완전 | `blocked` | `ReportJob(ocr_quality=needs_reupload)` |
| 단독 문서(`claim_id`·`doc_total` 없음) | — | 문서별로 즉시 발행 |

## 실패 처리

| 실패 종류 | 예 | 처리 |
|---|---|---|
| 결정적 (`NonRetryableError`) | 마스킹 잔류, 파일 디코드 실패 | 저널에 `terminal=True`로 기록하고 **첫 시도에 ack**. 청구 종결 카운트에 반영 |
| 일시적 | S3·DB·VLM 오류 | 저널에 `terminal=False`로 기록하고 재전달 |
| poison | 수신 횟수 상한 초과 | `_poison_journal` 훅이 확정 실패로 기록하고 종결 카운트 반영 후 스킵 |

실패 저널은 `ai.ocr_job_failures`이고, 이후 성공하면 해당 행을 지운다. 원본 삭제가 실패하면 `sweep_pending_deletes`가 최대 `OCR_DELETE_MAX_ATTEMPTS`회 재시도하고, 그래도 안 되면 `exhausted`로 종결한다. 최종 백스톱은 S3 라이프사이클이다.

## 입력 / 출력 (계약)

- **입력**: `core.contracts.OcrJob` (`ocr-job-queue`)
- **출력**: `ai.ocr_results` 행, S3 비식별 사본, `core.contracts.ReportJob` (`report-job`)

## 의존 / 배포

- `core.sqs`·`core.db`·`core.contracts` · S3 · Ollama VLM(`VLM_BASE_URL`·`VLM_MODEL`·`VLM_GROUNDING_MODEL`)
- `.[ocr]` extra: surya-ocr `0.17.x`로 고정(0.18+는 별도 추론 서버가 필요), transformers `<5`
- 무거운 의존성(surya·pymupdf·PIL·boto3)은 **lazy import**한다. GPU 없이도 오케스트레이션을 테스트할 수 있다.
- 배포: `src/ocr_worker/Dockerfile`(CUDA 베이스) → GPU EC2. `deploy/docker-compose.ocr.yml`, SSM으로 재시작
- 배포 확인: `scripts/check_ocr_deploy.sh`

## 원칙

- **외부 OCR API 사용 금지**(개인정보보호). 로컬 GPU와 자체 호스팅 VLM만 쓴다.
- 마스킹 전 텍스트는 저장하거나 로그에 남기거나 발행하지 않는다.

## 참고

- [Notion 02 OCR](../../.claude/docs/02_ocr.md) · [OCR v2 설계](../../.claude/docs/ocr-pipeline-v2-design.md) · [아키텍처: ocr](../../docs/architecture/ocr.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
