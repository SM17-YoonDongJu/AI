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
| 5 | VLM 보조(쪽 전사) | `vlm_client.py` | 표 문서이거나 평균 신뢰도가 0.90 미만일 때만. 클러스터 vLLM의 PaddleOCR-VL-1.6에 쪽 전체를 `OCR:`로 보내고(쪽끼리 동시, 상한 `VLM_CONCURRENCY`, 단계 예산 `VLM_PAGE_BUDGET_SECONDS`), HTML/OTSL 표는 pipe Markdown으로 바꾼다. `max_tokens`에서 잘린 응답·반복 루프는 버리고, surya 원문과 겹침(groundedness)을 검증한 뒤 채택. 전사 결과도 6단계와 같은 마스킹을 거친다. 실패한 쪽만 surya로 폴백 |
| 6 | 텍스트 마스킹 | `masking/` | 정규식(항상) ∪ NER(이름, `USE_NER=true`일 때). 마스킹 후 잔류 스캔을 하고, 남아 있으면 `MaskingError`(fail-closed) |
| 7 | 이미지 마스킹 | `masking/image_masker.py` | PII 라인을 검은 블록으로 가린 비식별 사본을 S3에 올린다. 저신뢰도면 VLM **라인 재판독**으로 보강한다: surya 라인 bbox crop을 `OCR:`로 다시 읽고, 텍스트만 바꾼 페이지에 같은 검출을 돌려 찾은 PII 라인을 검은 블록에 더한다(라인 인덱스로 합쳐 11단계 커버리지 검증이 그 라인도 잰다). 실패한 라인은 건너뛴다(기존 판정은 그대로). **한계**: surya가 라인을 만든 곳만 다시 읽는다. 예전 Qwen grounding은 surya 라인이 없는 영역의 PII도 픽셀 bbox로 가릴 수 있었는데, 지금은 그 영역과 단계 예산으로 건너뛴 라인을 메우지 못한다. "후퇴 없음"은 surya 단독과 비교할 때만 맞다(후속: `Spotting:` 형식 확인 후 평가) |
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
| 일시적 | S3·DB 오류 | 저널에 `terminal=False`로 기록하고 재전달 |
| VLM 실패 | 호출 오류·잘린 출력·반복 루프·예산 초과 | 작업 실패가 아니다. 쪽 전사는 그 쪽만 surya로 폴백, 라인 재판독은 그 라인만 건너뛴다(사유별 수를 로그로 남김) |
| poison | 수신 횟수 상한 초과 | `_poison_journal` 훅이 확정 실패로 기록하고 종결 카운트 반영 후 스킵 |

실패 저널은 `ai.ocr_job_failures`이고, 이후 성공하면 해당 행을 지운다. 원본 삭제가 실패하면 `sweep_pending_deletes`가 최대 `OCR_DELETE_MAX_ATTEMPTS`회 재시도하고, 그래도 안 되면 `exhausted`로 종결한다. 최종 백스톱은 S3 라이프사이클이다.

## 입력 / 출력 (계약)

- **입력**: `core.contracts.OcrJob` (`ocr-job-queue`)
- **출력**: `ai.ocr_results` 행, S3 비식별 사본, `core.contracts.ReportJob` (`report-job`)

## 의존 / 배포

- `core.sqs`·`core.db`·`core.contracts` · S3 · 클러스터 vLLM의 PaddleOCR-VL-1.6(`VLM_BASE_URL`·`VLM_MODEL`, OpenAI 호환 `/v1/chat/completions`). 타임아웃·토큰 상한·동시도·단계 예산과 SQS 가시성 600초 안의 최악 경로 계산은 `src/core/config.py` VLM 섹션에 있다. 기본 예산은 쪽 전사 210초·라인 재판독 90초이고, VLM 외 단계 몫 240초(실측 전 가정)와 안전 여유 60초를 더해 600초다. 이 합이 `SQS_VISIBILITY_TIMEOUT`을 넘으면 워커가 기동을 거부한다. 쪽 210초는 Paddle 실측 속도(35.7 tok/s)로 3,072토큰 ≈ 86초라 넉넉하다. 장기적으로는 처리 중 가시성 연장(heartbeat)이 필요하다(예산은 surya·재OCR 시간을 묶지 못한다)
- VLM 한계(공식 문서 기준): 쪽 전체 이미지를 서버에 직접 보내는 건 공식 권장 경로가 아니고(레이아웃 → 영역별 인식 권장), 쪽 입력은 약 1MP로 줄어 작은 글씨가 뭉개질 수 있다. 그래서 쪽 전사는 검증을 통과할 때만 채택한다. 라인 재판독은 문서화된 element-level 입력과 맞는다. VLM 출력엔 신뢰도가 없어 신뢰도 기준은 계속 surya다
- `.[ocr]` extra: surya-ocr `0.17.x`로 고정(0.18+는 별도 추론 서버가 필요), transformers `<5`
- 무거운 의존성(surya·pymupdf·PIL·boto3)은 **lazy import**한다. GPU 없이도 오케스트레이션을 테스트할 수 있다.
- 배포: `src/ocr_worker/Dockerfile`(python:3.12-slim + torch 번들 CUDA). 현재는 GPU EC2의 `deploy/docker-compose.ocr.yml`(SSM으로 재시작, VLM은 이 호스트에 없고 클러스터 vLLM을 부른다). k8s brbs-etl(g4dn.xlarge T4 — vLLM과 GPU를 타임슬라이스로 나눔)로 옮길 예정이고, 그때 surya 배치 크기(`RECOGNITION_BATCH_SIZE`·`DETECTOR_BATCH_SIZE`)를 env로 낮춘다
- 배포 확인: `scripts/check_ocr_deploy.sh`

## 원칙

- **외부 OCR API 사용 금지**(개인정보보호). 로컬 GPU와 자체 호스팅 VLM(클러스터 vLLM)만 쓴다.
- 마스킹 전 텍스트는 저장하거나 로그에 남기거나 발행하지 않는다. VLM 전사·재판독 텍스트도 같다(로그엔 개수·사유만).

## 참고

- [Notion 02 OCR](../../.claude/docs/02_ocr.md) · [OCR v2 설계](../../.claude/docs/ocr-pipeline-v2-design.md) · [아키텍처: ocr](../../docs/architecture/ocr.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
