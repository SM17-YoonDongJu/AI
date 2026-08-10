# ocr_worker — OCR 워커 (02)

`ocr-job-queue` SQS 메시지를 소비해 문서를 OCR·분류·마스킹하는 워커. **surya-ocr(PyTorch)를 인프로세스로 실행하므로 GPU 노드에 배포**한다(Ollama/vLLM 노드와 co-location 가능).

## 처리 흐름

1. `ocr-job-queue` 소비 (`OcrJob`)
2. S3에서 파일 읽기 (GetObject)
3. **surya-ocr**(전용 OCR 엔진, 로컬 GPU, 한국어) — 라인 단위 `(text, bbox, polygon, confidence)` 추출
4. 문서 유형 분류 → 엔티티 추출(진단명 KCD·보험사·상품·지급금액)
5. **PII 마스킹**(정규식+NER) — 마스킹 텍스트만 downstream으로
6. `ocr_results` 저장 → `report-job` 발행

## 입력 / 출력 (계약)

- **입력**: `core.contracts.OcrJob` (consume `ocr-job-queue`)
- **출력**: `ocr_results` DB 저장 + `core.contracts.ReportJob` 발행 (`report_worker`가 소비)

## 의존 / 배포

- `core.sqs`·`core.db` · S3 · surya-ocr(`.[ocr]` extra, **PyTorch/CUDA**)
- 배포: `src/ocr_worker/Dockerfile` (CUDA 베이스) → **GPU 노드**
- PII 마스킹 규칙은 `guardrail` 입력단과 정렬

## 참고

- [Notion 02 OCR](../../.claude/docs/02_ocr.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
- 외부 OCR API 사용 금지(개인정보보호) — 로컬 GPU만
