# guardrail — 가드레일 (06)

`report_worker`가 **함수 호출**로 쓰는 공용 모듈이다. 개인정보보호와 법적 안전성이 이 도메인의 핵심 제약이라 **필수 경로**다. 단계별 함수로 나뉘어 있어 호출자가 필요한 단계만 조립한다. 결과 모델은 `core.contracts`(`InputGuardResult`·`OutputGuardResult`)가 단일 출처다.

## 사용

```python
import guardrail

inp = await guardrail.guard_input(user_text)        # → InputGuardResult(masked_text, blocked, reason)
if inp.blocked:
    ...                                             # 리포트: persist_blocked → reports.status=BLOCKED

draft = guardrail.guard_generation(llm_text)        # → str (동기 함수)
out = await guardrail.guard_output(draft, run_judge=True, chunks=rag_chunks)
out.final_text       # 고지문이 삽입된 최종 텍스트
out.judge_failures   # LLM Judge가 근거 없다고 판정한 문장 목록
```

## 3단계 (현재 구현 기준)

| 단계 | 함수 | 하는 일 |
|---|---|---|
| **입력** | `guard_input` (async) | 정규식 PII 마스킹(주민번호 **앞 6자리만 보존**, 전화, 계좌). 키워드 기반으로 보험·법률 외 질문 차단(부동산·주식·코인 등) |
| **생성** | `guard_generation` (sync) | "OO원을 받습니다/지급됩니다" 같은 단정적 금액 표현을 "참고 추정 범위(약 OO원 내외, 약관·근거 기준)"로 치환 |
| **출력** | `guard_output` (async) | 법적 고지문(`DISCLAIMER`) 삽입. `run_judge=True`이고 `chunks`가 있으면 **LLM Judge**(`core.ai_client.chat_json`)로 리포트 주장이 약관 원문과 맞는지 검증 |

LLM Judge는 **리포트 전용**이다. 결과는 `judge_failures`로 돌려줄 뿐 본문을 고치지 않는다. 실패 문장을 어떻게 처리할지는 호출자가 정한다.

## 설계 대비 미구현 항목

[Notion 06 설계](../../.claude/docs/06_guardrail.md)와 비교해 아직 구현되지 않은 부분이다.

- 입력 PII 마스킹에 **NER이 없다**(정규식만). 이름 같은 비정형 PII는 걸러지지 않는다.
- 생성 단계의 `[조항][판례]` **인용 강제**가 없다.
- LLM Judge 불일치 섹션의 **자동 치환**이 없다.
- PII 규칙이 `ocr_worker.masking`과 **별도 구현**이다. 설계 원칙은 "두 곳의 규칙이 같아야 한다"인데, 지금 `ocr_worker` 쪽이 더 넓다(이메일·카드·사업자·여권·운전면허·주소 + NER). 정의를 공유하도록 통합하는 것이 후속 과제다.

## 주의

- 마스킹 전 원문을 로그에 남기지 않는다(`core.logging`도 출력 직전 한 번 더 마스킹한다).
- 이 모듈을 바꾸면 report 서비스가 재배포된다(CI paths-filter).

## 참고

- [Notion 06 가드레일](../../.claude/docs/06_guardrail.md) · [아키텍처: guardrail](../../docs/architecture/guardrail.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
