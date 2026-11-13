# guardrail — 가드레일 (06)

`report_worker`가 **함수 호출**로 쓰는 공용 모듈이다. 개인정보보호와 법적 안전성이 이 도메인의 핵심 제약이라 **필수 경로**다. 단계별 함수로 나뉘어 있어 호출자가 필요한 단계만 조립한다. 공개 진입점은 `guard_input`·`guard_generation`·`guard_output`과, 외부 API로 보내기 전 자유입력을 가리는 `mask_pii`, 마스킹 토큰 건수를 세는 `count_masked_pii`다. 결과 모델은 `core.contracts`(`InputGuardResult`·`OutputGuardResult`)가 단일 출처다.

## 사용

```python
import guardrail

# 마스킹은 문서 본문에, 도메인 검사는 사용자 질문에만 건다(#103). question은 필수(None 가능)
inp = await guardrail.guard_input(document_text, question=user_question)  # → InputGuardResult
if inp.blocked:
    ...                                             # 리포트: persist_blocked → reports.status=BLOCKED

draft = guardrail.guard_generation(llm_text)        # → str (동기 함수)
out = await guardrail.guard_output(draft, run_judge=True, chunks=evidence)  # 근거: 약관·장해분류표·판례
out.final_text             # 고지문이 삽입된 최종 텍스트
out.judge_failures         # LLM Judge가 근거 없다고 판정한 문장 목록(판정 불가면 [JUDGE_UNAVAILABLE])
out.judge_checked_chars    # Judge가 실제로 본 리포트 글자 수(판정 못 받았으면 0)
out.judge_total_chars      # final_text 전체 글자 수(항상) → 검증 범위 = checked / total
out.judge_evidence_chunks  # Judge가 본 근거 청크 수(판정 못 받았으면 0)

# 외부 LLM·임베딩 API(Claude·Voyage)로 보내기 전, 복호화한 자유입력 원문을 가린다(§13)
try:
    question = guardrail.mask_pii(raw_question)      # → str (동기 함수). None은 받지 않는다
except guardrail.MaskingError:
    ...                                             # 고민감 PII가 남았다 — 이 텍스트는 보내지 않는다

guardrail.count_masked_pii(masked_text)             # → {"이름": 2, "주민등록번호": 1} (동기·순수)
```

## 3단계 (현재 구현 기준)

| 단계 | 함수 | 하는 일 |
|---|---|---|
| **입력** | `guard_input` (async) | 문서 본문(`text`)에 정규식 PII 마스킹(주민번호 **앞 6자리만 보존**, 전화, 계좌). 사용자 질문(`question`)에만 키워드 기반 보험·법률 외 주제 차단(부동산·주식·코인 등, 회사 표기 `주식회사`는 제외). 문서 본문과 사고 경위·추가 정보에는 이 단어들이 정상적으로 나와서(증권의 "○○보험 주식회사", 사고 경위의 "요리하다가") 도메인 검사를 걸지 않는다 |
| **생성** | `guard_generation` (sync) | "OO원을 받습니다/지급됩니다" 같은 단정적 금액 표현을 "참고 추정 범위(약 OO원 내외, 약관·근거 기준)"로 치환 |
| **출력** | `guard_output` (async) | 법적 고지문(`DISCLAIMER`) 삽입. `run_judge=True`이고 `chunks`가 있으면 **LLM Judge**(`core.ai_client.chat_json`)로 리포트 주장이 근거 원문(약관·장해분류표·판례)과 맞는지 검증하고, Judge가 본 범위를 함께 돌려준다 |

### LLM Judge (`guard_output`)

LLM Judge는 **리포트 전용**이다. 결과는 `judge_failures`로 돌려줄 뿐 본문을 고치지 않는다. 실패 문장을 어떻게 처리할지는 호출자가 정한다.

```python
async def guard_output(
    text: str,
    *,
    run_judge: bool = True,
    chunks: list[Any] | None = None,                  # 근거. dict면 "text" 값, 아니면 str()
    max_report_chars: int = JUDGE_MAX_REPORT_CHARS,   # 1 미만이면 ValueError(호출자 버그)
) -> OutputGuardResult
```

**Judge가 보는 범위.** 프롬프트에 넣는 양을 명명 상수로 정한다(`guardrail`에서 바로 import된다).

| 상수 | 값 | 뜻 |
|---|---|---|
| `JUDGE_MAX_REPORT_CHARS` | 12,000 | 리포트 앞부분 글자 수. 고지문이 붙은 `final_text` 기준이다. `max_report_chars`로 바꿀 수 있다 |
| `JUDGE_MAX_EVIDENCE_CHUNKS` | 20 | 근거 청크 수. 약관 8 + 장해분류표 최대 8 + 판례 4가 다 들어가는 크기다 |
| `JUDGE_EVIDENCE_CHARS` | 800 | 청크당 글자 수. 장해 추출 ctx와 같다(지급률 숫자가 잘리지 않게) |

- 예전에는 리포트 앞 2,000자와 근거 6건(각 500자)만 봤다. 그래서 본문 뒷부분, 약관이 6건을 넘으면 밀려나는 장해분류표, 판례가 검증 밖이었다.
- 근거는 **앞에서부터** 20건만 들어간다. 중요한 근거를 앞에 둔다. report는 약관 → 장해분류표 → 판례 순으로 넘긴다.
- 빈 `judge_failures`만으로는 "전부 검증해 통과"인지 "일부만 봤다"인지 알 수 없다. 그래서 Judge가 실제로 본 양을 함께 돌려준다. 검증 범위는 `judge_checked_chars / judge_total_chars`다. 리포트가 12,000자 이하면 1.0이다. 근거가 잘렸는지는 넘긴 건수와 `judge_evidence_chunks`를 비교해 안다.

**판정 결과별 반환값.** 어떤 경우에도 고지문은 붙어 있고 예외를 올리지 않는다(`max_report_chars` 오류만 예외). 예외가 올라가면 report의 `safe_node`가 삼켜 고지문 없는 원본이 저장되기 때문이다.

| 경우 | `judge_failures` | `judge_checked_chars` | `judge_evidence_chunks` | 로그 |
|---|---|---|---|---|
| `run_judge=False`, 또는 `chunks`가 비었거나 None (Judge 호출 없음) | `[]` | 0 | 0 | 없음 |
| 판정 받음 (`{"failures": [문장, ...]}`) | 판정 그대로 | `min(len(final_text), max_report_chars)` | `min(len(chunks), 20)` | 없음 |
| 호출 실패 (`AiClientError` — 거절·429·5xx·타임아웃·출력 상한 잘림) | `[JUDGE_UNAVAILABLE]` | 0 | 0 | `guardrail.judge.failed` (`error_type`) |
| 판정 불가 응답 (아래) | `[JUDGE_UNAVAILABLE]` | 0 | 0 | `guardrail.judge.unusable_verdict` (`reason`) |

`judge_total_chars`는 모든 경우에 `len(final_text)`다.

**판정 불가 응답.** `chat_json`은 JSON 파싱에 실패하면 `{}`를 돌려준다. 이것을 예전처럼 "failures 없음 = 통과"로 읽으면 검증하지 못한 리포트가 통과로 남는다. 그래서 프롬프트가 요구한 `{"failures": [문장, ...]}` 형식이 아니면 전부 호출 실패와 똑같이 처리한다. 로그 `reason`은 분류값만 담는다. 응답 본문은 남기지 않는다.

| `reason` | 응답 예 |
|---|---|
| `failures_missing` | `{}` (파싱 실패), `failures` 키 없음 |
| `failures_not_list` | `{"failures": null}`, `{"failures": "없음"}` |
| `verdict_not_object` | `[...]`, `"..."`, `null` |
| `failure_item_not_str` | `{"failures": [{"문장": ...}]}`. 그대로 넣으면 `judge_failures: list[str]` 검증에서 예외가 나 고지문 없는 원본이 저장됐다 |

`JUDGE_UNAVAILABLE` 문구("LLM Judge 호출 실패")는 저장된 초안에 그대로 남는 값이라 바꾸지 않았다. 판정 불가 응답도 이 표식을 쓴다.

**한계.**
- 프롬프트가 커졌다. 상한 기준 약 5,000자(리포트 2,000 + 근거 3,000)에서 약 28,000자(리포트 12,000 + 근거 16,000)다. 호출 1회의 입력 토큰과 지연이 그만큼 는다.
- `LLM_PROVIDER=openai_compat`(Ollama·vLLM 등)로 돌리면 프롬프트가 모델 컨텍스트를 넘을 수 있다. 로컬 `.env.example`이 이 설정이다. vLLM은 `max_model_len`을 넘으면 400으로 거절한다. 그러면 `AiClientError`가 나고 결과는 `JUDGE_UNAVAILABLE`이다. Ollama는 `num_ctx`를 넘는 입력을 오류 없이 잘라 처리할 수 있다. 그러면 Judge가 실제로 읽은 양보다 `judge_checked_chars`가 크게 나온다. `AI_TIMEOUT_SECONDS`(기본 60초) 안에 못 끝내도 `JUDGE_UNAVAILABLE`이 된다. 운영 기본은 Claude(`anthropic`, 코드 기본값)라 해당하지 않는다.
- 판정 품질(Judge가 환각을 실제로 잡는지)은 테스트하지 않는다. `tests/test_guardrail.py`는 페이크 `chat_json`으로 범위와 응답 처리만 고정한다.

## 외부 전송 전 마스킹 — `mask_pii`

`mask_pii(text: str) -> str`는 Spring DB에서 복호화한 자유입력 필드(`reports.question`·`user_claims.description`·`user_claims.additional_information`)를 프롬프트·검색 쿼리에 넣기 전에 쓴다([컨벤션 §13](../../.claude/CODE_CONVENTIONS.md)).

규칙을 복제하지 않는다. OCR 입력단이 쓰는 `ocr_worker.masking`을 그대로 호출한다.

1. `ocr_worker.masking.mask(text)`: 정규식은 항상 적용한다. `settings.use_ner`가 켜져 있으면 NER 결과도 합친다.
2. `ocr_worker.masking.assert_no_residual(masked)`: OCR `_analyze`와 같은 잔류 게이트다. 고민감 PII(주민·외국인등록번호·카드)가 남아 있으면 `MaskingError`를 던진다.

그래서 같은 텍스트면 OCR이 저장하는 `masked_text`와 결과가 같다. OCR이 거절하는 텍스트는 여기서도 거절된다. `MaskingError`는 `guardrail.MaskingError`로도 내보낸다(`ocr_worker.masking.MaskingError`와 같은 클래스). `NonRetryableError`라서 같은 입력을 다시 넣어도 결과가 같다. 메시지에는 분류와 건수만 담긴다.

### `guard_input`과의 차이

| | `mask_pii` | `guard_input` (`_mask_pii`) |
|---|---|---|
| 용도 | 복호화한 자유입력 **원문**을 외부 API로 보내기 전 | OCR이 **이미 마스킹한** `masked_text`에 한 번 더 |
| 규칙 | OCR 입력단과 같음(정규식 + 설정 시 NER + 잔류 게이트) | 노션 06 명세 3종(주민번호·휴대폰·하이픈 계좌) |
| 주민번호 | `[주민등록번호]`로 통째 치환(생년월일도 안 나감) | 앞 6자리 보존(`860312-*******`) |
| 결과 형식 | `[분류]` 토큰(`[이름]`·`[주소]`·`[이메일]`…) | `***` 부분 가림 |
| 실패 | 잔류 고민감 PII → `MaskingError` | 없음(`question`이 도메인 외 주제면 `blocked=True`) |

`core.logging.mask_pii`는 이름만 같다. 그쪽은 로그 렌더링 직전의 부분 마스킹 방어선이라 외부 전송용으로 쓰지 않는다.

### 한계 — 운영 report에는 NER이 없다

운영 report는 `USE_NER=false`이고, report 이미지(`uv sync --no-dev --extra report`)에는 transformers도 없다. 그러니 정규식만 돈다. 아래 표는 report extra만 설치한 venv에서 정규식 전용으로 직접 확인한 결과다(합성 데이터, 2026-11-01).

**가려진다**
- 주민·외국인등록번호(`860312-1948571`, 13자리 붙여쓰기, 공백 1칸 구분)
- 휴대폰·유선(`010-1234-5678`, `01012345678`, `010.1234.5678`, `02-123-4567`), 이메일
- 카드(4-4-4-4, 16자리 붙여쓰기, Amex 4-6-5), 사업자번호, 여권, 운전면허
- 키워드(`계좌`·`계좌번호`·`예금주`·`예금`·`가상계좌`·`입금`·`이체`·`account`) 뒤 25자 안의 계좌번호
- 시·도 + 시·군·구로 시작하는 주소(`서울특별시 강남구 테헤란로 123 101동 202호`, `서울 강남구 역삼동`)
- 라벨 뒤 이름: `환자 성명:`·`성명`·`피보험자`·`계약자`·`예금주`·`보호자 성명`·의사명 라벨·`OOO (서명)`·담당자. 라벨 바로 뒤 한글 덩어리가 2~4글자일 때만 잡는다. 조사·호칭이 붙어도 합쳐 4글자 이하면 덩어리째 가린다(`피보험자 홍길동이` → `피보험자 [이름]`). 5글자 이상이면 통째로 놓친다(아래)
- 증권번호·면허번호·병실·승인번호·환자번호(각 라벨 뒤)
- 점으로 구분한 주민번호·카드(`860312.1948571`, `1234.5678.9012.3456`)는 디텍터가 놓친다. 잔류 게이트가 잡아 `MaskingError`로 거절한다

**가려지지 않는다(그대로 외부로 나간다)**
- **라벨 없는 자유문 속 이름**: `홍길동입니다`, `저는 홍길동이고 어머니는 김영희입니다`, `홍길동 씨가`, `아버지 김철수가`. 이건 NER의 몫인데, report에서는 NER을 켤 수 없다(아래 "주의" 참고). `tests/test_guardrail.py`의 `test_mask_pii_without_ner_keeps_unlabeled_names`가 이 한계를 고정한다
- 라벨 뒤라도 조사가 붙거나 이름+접미사가 4글자를 넘는 경우: `성명은 홍길동입니다`, `피보험자는 홍길동입니다`, `예금주는 홍길동`, `피보험자 홍길동님이`, `환자 성명 남궁민수입니다`
- 앵커 키워드가 없는 계좌: `신한은행 110-234-567890`, `국민 123456-01-123456`. `은행`은 키워드가 아니다. 이전 `_mask_pii`의 하이픈 계좌 패턴은 이런 계좌를 가렸지만, 날짜(`2024-01-05`)까지 가렸다
- 시·도가 빠진 주소: `강남구 역삼동 123-45`, `역삼동 아파트 101동 202호`
- 구분자 앞뒤에 공백이 있는 번호: `860312 - 1948571`(잔류 게이트도 못 잡는다), `010 - 1234 - 5678`. 그 밖에 `(010)1234-5678`, `+82 10-1234-5678`, 전각 숫자 휴대폰
- 차량번호(`12가 3456`, `123가4567`), 생년월일만 따로 적은 값(`1986년 3월 12일`, `860312`), 메신저 ID, 풀어 쓴 이메일(`hong at example dot com`)

**과하게 가려진다(유출은 아니지만 리포트 품질에 영향)**
- 키워드 뒤 25자 안의 8자리 이상 숫자는 금액이어도 계좌로 본다: `입금된 금액 12000000원` → `입금된 금액 [계좌번호]원`. 콤마가 있으면(`1,200,000원`) 괜찮다
- 공백으로 띄운 16자리 숫자열(`기간 2024 01 05 2024 01 20`)은 잔류 게이트가 카드로 읽어 `MaskingError`가 난다

정규식 규칙은 `ocr_worker.masking`(ocr-engineer 소관) 한 곳에만 있다. 위 누락을 고치려면 그쪽 패턴을 고친다. 그러면 OCR과 report에 같이 반영된다.

### 호출 시 주의

- **원문에 한 번만 건다.** 멱등이 아니다. `[계좌번호]`·`[환자등록번호]`·`[면허번호]`·`[병실번호]`·`[승인번호]`·`[증권번호]` 토큰은 그 자체가 앵커 키워드를 품는다. 그래서 다시 걸면 토큰 뒤의 날짜·금액까지 가린다. 예: `환자등록번호: [환자등록번호] 입원일 2024-01-05` → `… 입원일 [환자등록번호]5`. OCR이 이미 마스킹한 `masked_text`에는 걸지 않는다. 대표 유형(이메일·카드·라벨 이름·주소·주민번호·휴대폰·키워드 계좌)만 있는 텍스트에서는 다시 걸어도 그대로다(테스트로 고정).
- **`MaskingError`가 나면 그 텍스트를 보내지 않는다.** 원문으로 폴백하지 않는다. 필드를 빼고 진행할지, 리포트를 막을지는 호출자가 정한다. 예외가 나면 마스킹본이 없으니, 원문 변수를 대신 쓰는 폴백만 없으면 된다.
- **`USE_NER=true`로 두지 않는다.** report 이미지에서 켜면 transformers가 없어, 비어 있지 않은 입력을 처음 넣을 때 `ModuleNotFoundError: No module named 'transformers'`가 난다. 빈 문자열은 통과한다. `.env.example`은 OCR 기준이라 `USE_NER=true`다. 그대로 복사해 transformers 없는 로컬 venv에서 report를 돌려도 같은 오류가 난다.
- 동기 함수다. 정규식만이면 짧은 필드는 금방 끝난다. NER을 켜면 CPU 추론이 끼므로 async 코드에서는 `asyncio.to_thread`로 뗀다.
- 입력은 `str`만 받는다. 복호화 결과가 `None`이면 호출 전에 거른다.

### 배포 결합

- `mask_pii`는 import 시점에 `ocr_worker.masking` 패키지 전체(`image_masker`→`ocr_worker.ocr`→`ocr_worker.storage`, `image_verify`, `verify` 등)를 불러온다. 지금 이 체인은 최상위에서 표준 라이브러리와 `core`만 import한다. OCR 엔진(paddleocr)·PIL·pymupdf·numpy·transformers·boto3는 함수 안에서 lazy import한다. 그래서 report 이미지에서 동작한다. report extra만 설치한 venv에서 import·호출·`report_worker.graph`/`worker` import를 확인했다. `test_mask_pii_imports_and_runs_without_report_absent_packages`는 하위 프로세스에서 PIL·torch·transformers·paddle·paddleocr·paddlex·fitz·cv2·sentence_transformers(옛 엔진 surya 포함) import를 막고 같은 호출을 돌린다. 체인에 이 중 하나가 최상위 import로 들어오면 이 테스트가 깨진다.
- report Dockerfile은 `COPY src ./src`로 `src/ocr_worker/`까지 이미지에 넣는다. 복사 범위를 report 관련 패키지로 좁히면 `import guardrail`부터 실패한다.
- CI 배포 필터(`.github/workflows/deploy.yml`)의 `report`는 `src/report_worker/**`·`src/rag/**`·`src/guardrail/**`만 본다. `src/ocr_worker/masking/**`만 바뀌면 OCR만 재배포된다. 그러면 운영의 두 워커가 다른 규칙으로 돈다. report 필터에 `src/ocr_worker/masking/**`(그리고 import 체인의 `src/ocr_worker/{__init__,ocr,storage}.py`)를 넣는 후속 작업이 필요하다.

## 마스킹 건수 — `count_masked_pii`

`count_masked_pii(text: str | None) -> dict[str, int]`는 OCR 마스커가 남긴 `[분류]` 토큰(`[이름]`·`[주민등록번호]` 등)을 분류별로 센다. 리포트 수치(가린 PII 건수)용 순수 함수다. 동기 함수이고 아무것도 로깅하지 않는다.

- 반환은 `{라벨값: 개수}`다. 키는 한국어 라벨값(`"이름"`)이고, 개수가 1 이상인 라벨만 담는다. None이나 빈 문자열이면 `{}`다.
- 토큰 정의를 복제하지 않는다. `ocr_worker.masking.PiiLabel`을 순회해 `label.token`을 센다. OCR에 라벨이 늘면 여기도 자동으로 따라간다. `[면허번호]`와 `[운전면허번호]`처럼 이름이 겹쳐도 대괄호 덕분에 섞이지 않는다.
- OCR 규칙으로 가린 텍스트에 쓴다(OCR `masked_text`, `mask_pii` 결과). `guard_input`의 `***` 부분 가림은 토큰이 아니라 세지 않는다.
- 탐지 건수가 아니라 **가린 자리 수**다. 겹친 탐지는 마스커가 토큰 하나로 합친다. 토큰 문자열을 세므로 원문에 처음부터 `[이름]` 같은 글자가 있었으면 그것도 센다.

## 설계 대비 미구현 항목

[Notion 06 설계](../../.claude/docs/06_guardrail.md)와 비교해 아직 구현되지 않은 부분이다.

- `guard_input`의 PII 마스킹에 **NER이 없다**(정규식 3종만). OCR이 이미 마스킹한 텍스트에 거는 2차 방어선이라서다. 자유입력 원문은 `mask_pii`가 OCR 규칙으로 가린다. 다만 운영 report에서는 NER을 켤 수 없어, 라벨 없는 이름은 `mask_pii`로도 막지 못한다(위 "한계").
- 생성 단계의 `[조항][판례]` **인용 강제**가 없다.
- LLM Judge 불일치 섹션의 **자동 치환**이 없다.
- `guard_input`(`_mask_pii`)의 규칙은 여전히 `ocr_worker.masking`과 **별도**다. 노션 06 명세(주민번호 앞 6자리 보존)를 따르기 때문이다. 외부 전송 경로(`mask_pii`)는 OCR 규칙을 그대로 공유한다.

## 주의

- 마스킹 전 원문을 로그에 남기지 않는다(`core.logging`도 출력 직전 한 번 더 마스킹한다). `mask_pii`는 아무것도 로깅하지 않는다.
- 이 모듈을 바꾸면 report 서비스가 재배포된다(CI paths-filter). `ocr_worker.masking`을 바꿨을 때는 재배포되지 않는다(위 "배포 결합").

## 참고

- [Notion 06 가드레일](../../.claude/docs/06_guardrail.md) · [아키텍처: guardrail](../../docs/architecture/guardrail.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md) · [OCR 마스킹](../ocr_worker/masking/__init__.py)
