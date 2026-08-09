# 가드레일 시스템 (입력·생성·출력 3단계)

> 출처: AI 엔진 아키텍처 문서 세트 · 최종 점검일 2026-07-15 · 브랜치 `11-feature-langgraph-멀티에이전트-구현`
> 상위: [README](./README.md) · 원본 코드 정독 + 적대적 교차검증(코드 재대조) 완료

## 🎯 한 문장 요약

가드레일은 AI가 만든 보험·법률 답변에서 개인정보를 가리고, 위험한 단정 표현을 완화하고, 법적 고지문을 붙이고, 근거 없는 주장(환각)을 점검하는 3단계 안전장치 모듈이다.

## 🌱 쉽게 말하면

가드레일은 식당 주방에서 요리(=AI 답변)가 손님에게 나가기 전에 거치는 **위생·안전 검사대**라고 생각하면 된다.

- 재료가 들어올 때(**입력**) 먼저 손님의 민감한 개인정보(주민번호·전화번호·계좌번호)를 지워 가리고, 보험·법률과 무관한 엉뚱한 주문("코인 사도 돼요?")은 아예 되돌려 보낸다.
- 요리를 만드는 중에(**생성**) "무조건 500만원 받습니다" 같은 위험한 단정 표현을 "약 500만원 내외로 추정" 같은 조심스러운 말로 바꾼다.
- 접시가 나가기 직전(**출력**) "이건 참고용입니다"라는 안내문(고지문)을 붙이고, AI가 근거 없이 지어낸 말은 없는지 다른 AI(검증관)에게 한 번 더 확인시킨다.

중요한 점은 가드레일이 **혼자 돌아가는 별도 프로그램이 아니라, 다른 프로그램이 필요할 때 불러 쓰는 도구 상자(함수 모음)**라는 것이다. 그리고 뒤에서 솔직하게 밝히듯이, 이 검사대는 아직 **기본 검사(개인정보 가리기·문구 치환·고지문)는 잘 하지만, 고급 검사(이름·주소 인식, 환각 문장 자동 삭제)는 아직 미완성**이다.

---

## 1. 3단계 개요

가드레일은 **독립 서비스가 아니라 공용 Python 모듈**이다. `report_worker`(05, Kafka 워커)와 `chatbot`(12, FastAPI)이 **함수 호출**로 공유하며, 모듈 자체는 Frontend·Spring Boot·Kafka와 직접 통신하지 않는다(`.claude/docs/06_guardrail.md:9`, `src/guardrail/README.md:3`). 개인정보보호·법적 안전성이 도메인 핵심 제약이라 반드시 거쳐야 하는 경로로 설계됐고, 단계별 함수로 잘게 쪼개 두어 호출하는 쪽이 필요한 것만 골라 조립한다(`README.md:3`).

> 쉽게 말하면: 가드레일은 "스스로 켜져 있는 서버"가 아니라 "필요할 때 꺼내 쓰는 앱 안의 부품"이다. 리포트 워커와 챗봇이라는 두 프로그램이 이 부품을 각자 가져다 쓴다. 부품이므로 카프카(메시지 우체통)나 스프링 같은 외부 시스템과 직접 대화하지 않는다.

실제 구현은 `src/guardrail/guards.py` **단 하나의 파일(96줄)**에 3개 진입 함수로 담겨 있다. 모듈 상단 docstring(파일 맨 위에 붙는 설명문)이 3단계를 요약한다(`guards.py:1-9`):

```python
"""가드레일 3단계 구현 — 입력/생성/출력.

- 입력: 정규식 PII 마스킹(주민번호 앞 6자리 보존) + 보험·법률 외 도메인 차단
- 생성: 단정적 금액 표현 → "참고 추정 범위"로 치환
- 출력: 법적 고지문 삽입 + (리포트 한정) LLM Judge 인용 검증

결과 모델은 `core.contracts`(InputGuardResult/OutputGuardResult)가 단일 출처다.
PII 마스킹 규칙은 `ocr_worker` 입력단과 동일해야 한다(어긋나면 한쪽이 PII 유출).
"""
```

여기서 몇 가지 용어를 짚어 두면 아래 표가 훨씬 잘 읽힌다.

- **PII**(Personally Identifiable Information): 개인을 특정할 수 있는 정보. 주민번호·전화번호·계좌번호 같은 것.
- **정규식**(regular expression): 글자 패턴을 찾아내는 규칙. "숫자 6개 뒤에 하이픈, 그 뒤 숫자 7개" 같은 모양을 컴퓨터에게 알려주는 문법이다.
- **마스킹**(masking): 찾은 정보를 `*` 같은 기호로 덮어 가리는 것.

| 단계 | 진입 함수(guards.py) | 동기/비동기 | 반환 | 핵심 동작 |
|---|---|---|---|---|
| **입력** | `guard_input` (`:36`) | `async` | `InputGuardResult` | 정규식 PII 마스킹 + 도메인외 키워드 차단 |
| **생성** | `guard_generation` (`:53`) | **동기(sync)** | `str` | 단정적 금액 표현 → 추정 범위 문구 치환 |
| **출력** | `guard_output` (`:61`) | `async` | `OutputGuardResult` | 법적 고지문 삽입 + (리포트 한정) LLM Judge 인용검증 |

> 표 읽는 법: "비동기(async)"는 안에서 시간이 걸리는 작업(예: 다른 AI를 기다리는 일)을 할 수 있게 열어 둔 함수, "동기(sync)"는 즉시 끝나는 함수라는 뜻이다. "반환"은 각 함수가 일을 마치고 돌려주는 결과 꾸러미의 이름이다.

모듈이 의존하는(가져다 쓰는) 다른 코드는 최소한이다(`guards.py:11-14`): 표준 `re`(정규식 도구), `core.ai_client`(LLM Judge를 부를 때 쓰는 AI 호출기), 그리고 결과 꾸러미 모델 `core.contracts.InputGuardResult/OutputGuardResult`. 결과 스키마(데이터의 정해진 형태)의 유일한 출처는 `core.contracts`이며(`guards.py:7`), 가드레일 모듈은 **데이터 모델을 직접 정의하지 않고 가져다 쓰기만 한다**(`contracts.py:1-5` — "이 모듈은 데이터 모델만 정의한다").

> 쉽게 말하면: 결과의 "그릇 모양"은 `core.contracts`라는 한 곳에서만 정해 두고, 가드레일은 그 그릇을 빌려 담기만 한다. 이렇게 하면 여러 곳에서 제각각 그릇을 만들다 어긋나는 사고를 막는다.

문서(`06_guardrail.md`, `README.md`)와 실제 코드 사이에는 아직 구현되지 않은 격차가 여럿 있는데, 이는 6절에서 정직하게 정리한다(요약: NER 미탑재, 인용 강제 미구현, 인용 실패 섹션 치환 미구현).

---

## 2. 입력 가드레일 — `guard_input`

### 2.1 PII 마스킹 `_mask_pii` (정규식 3종, verbatim)

개인정보 가리기는 별도 도우미 함수 `_mask_pii`가 맡는데, **순수 정규식 3개**로만 이뤄진다(`guards.py:26-33`):

```python
def _mask_pii(text: str) -> str:
    # 주민번호 6-7: 앞 6자리 보존, 뒤 7자리 마스킹
    text = re.sub(r"(\d{6})[- ]?\d{7}", r"\1-*******", text)
    # 전화번호
    text = re.sub(r"01[016789][- ]?\d{3,4}[- ]?\d{4}", "***-****-****", text)
    # 계좌번호(연속 10자리 이상 숫자, 하이픈 포함)
    text = re.sub(r"\b\d{2,6}-\d{2,6}-\d{2,8}\b", "****-****-****", text)
    return text
```

정규식별로 무슨 일을 하는지 풀어 보면:

1. **주민등록번호** (`:28`) — `(\d{6})[- ]?\d{7}`. 앞 6자리를 캡처그룹(정규식이 따로 기억해 두는 조각) `\1`로 **남겨 두고** 뒤 7자리만 `*******`로 바꿔서 `123456-*******` 형태로 만든다. 하이픈이나 공백 구분자는 있어도 되고 없어도 된다(`[- ]?`). 이 "앞 6자리만 보존" 정책은 docstring·README·docs가 모두 똑같이 명시한 규칙이다(`guards.py:3`, `README.md:9`, `06_guardrail.md:17`).
2. **휴대전화번호** (`:30`) — `01[016789][- ]?\d{3,4}[- ]?\d{4}`. 010/011/016/017/018/019로 시작하는 번호를 잡아 전체를 `***-****-****`로 통째 가린다(남기는 부분 없음).
3. **계좌번호** (`:32`) — `\b\d{2,6}-\d{2,6}-\d{2,8}\b`. 하이픈으로 나뉜 숫자 3덩이를 `****-****-****`로 바꾼다. 다만 주석에는 "연속 10자리 이상"이라고 적혀 있지만 실제 정규식은 **하이픈이 들어간 3덩이 형태만** 잡는다. 즉 하이픈 없이 쭉 이어진 숫자열은 걸러내지 못한다 → 주석과 코드가 서로 맞지 않는 지점이다.

**치환하는 순서가 서로에게 영향을 준다.** 주민번호가 먼저 `123456-*******`로 바뀌기 때문에, 그다음 실행되는 계좌번호 정규식(하이픈 3덩이) 입장에서는 가운데 덩이가 `*`뿐이라 다시 걸리지 않는다. 한편 계좌 정규식은 `2020-01-01` 같은 **날짜 문자열을 실수로 잡을(false positive, 오탐)** 수 있다(4자리-2자리-2자리 → `****-****-****`).

> 쉽게 말하면: 이 정규식은 "혹시 개인정보일까 싶으면 일단 가린다"는 안전 우선 성향이다. 그래서 날짜처럼 개인정보가 아닌 것까지 가끔 가려 버리는데, 덜 가려서 정보가 새는 것보다는 안전한 쪽이라 그대로 둔 것이다.

### 2.2 도메인 외 질문 차단 + 진입 함수(verbatim)

차단할 분야는 코드에 미리 박아 둔 **7개 키워드 묶음**이다(`guards.py:21-22`):

```python
# 보험·법률 외 도메인 차단 키워드(간이)
_OFF_DOMAIN = ("부동산", "주식", "코인", "비트코인", "연애", "요리", "게임")
```

진입 함수는 먼저 개인정보를 가린 뒤, 원문에 위 키워드가 들어 있는지 검사한다(`guards.py:36-43`):

```python
async def guard_input(text: str) -> InputGuardResult:
    masked = _mask_pii(text or "")
    for kw in _OFF_DOMAIN:
        if kw in (text or ""):
            return InputGuardResult(
                masked_text=masked, blocked=True, reason=f"보험·법률 외 질문({kw})"
            )
    return InputGuardResult(masked_text=masked, blocked=False, reason=None)
```

동작을 순서대로 보면:

- `text or ""`로 입력이 비어 있어도(`None`이어도) 오류가 안 나게 막아 둔다(`:37`, `:39`).
- **개인정보 가리기는 차단 여부와 상관없이 항상 먼저 한다**(`:37`). 그래서 질문이 차단되더라도 `masked_text`에는 이미 가려진 안전한 값이 들어 있다.
- 키워드 검사는 `_OFF_DOMAIN`을 하나씩 훑다가 **처음 걸리는 순간 바로 결과를 돌려준다**(`:38-42`). `reason`에는 `f"보험·법률 외 질문({kw})"` 형태로 어떤 키워드 때문에 막혔는지 적힌다.
- 걸리는 키워드가 하나도 없으면 `blocked=False, reason=None`으로 통과시킨다(`:43`).

주의할 점: 이 함수는 `async`(비동기)로 선언돼 있지만 **안에서 실제로 기다리는(`await`) 지점이 하나도 없다**(정규식·문자열 비교만 하니까). docs가 말한 "정규식+NER" 중 NER(문장에서 이름·지명 같은 개체를 알아내는 AI 기술) 호출을 나중에 끼워 넣으려고 미리 비동기 형태로 만들어 둔 것으로 보이며, 지금은 사실상 즉시 끝나는 동기 로직이다(6절 참조).

### 2.3 결과 모델 `InputGuardResult`

`core.contracts`에 정의된 pydantic 모델(입력값의 형태와 타입을 자동 검증해 주는 파이썬 데이터 클래스)이다(`contracts.py:131-136`):

```python
class InputGuardResult(BaseModel):
    """입력 가드레일 결과. PII 마스킹 + 도메인 외 질문 차단."""

    masked_text: str  # PII 마스킹된 텍스트(주민번호 앞 6자리만 보존 등)
    blocked: bool  # 도메인 외 질문 차단 여부
    reason: str | None = None  # 차단 사유
```

- `masked_text`: 항상 채워진다(가리기 결과).
- `blocked`: 도메인 밖 질문이라 막혔는지를 나타내는 참/거짓 표시.
- `reason`: 막혔을 때는 사유 문구가, 통과했을 때는 `None`(값 없음)이 들어간다.

---

## 3. 생성 가드레일 — `guard_generation`

### 3.1 단정적 금액 표현 감지·치환(verbatim)

"무조건 얼마 받는다" 식의 단정적 금액 표현은 모듈 상단에 미리 컴파일해 둔(정규식을 미리 준비해 둔) 정규식으로 찾는다(`guards.py:48-50`):

```python
# 단정 금액 표현 → 참고 추정 범위로 치환
_ABS_AMOUNT = re.compile(
    r"(\d[\d,]*\s*(?:만\s*)?원)\s*(?:을|를)?\s*(?:받습니다|지급됩니다|지급합니다|입니다)"
)
```

정규식을 조각내 보면:

- `(\d[\d,]*\s*(?:만\s*)?원)` — 금액 부분을 캡처그룹 1로 잡는다. 숫자와 콤마(`1,000` 등), 선택적으로 `만`, 그리고 `원`으로 끝나는 형태다. `"500만원"`, `"1,000원"` 등을 잡는다.
- `\s*(?:을|를)?\s*` — 조사 `을/를`이 붙어도 되고 안 붙어도 된다.
- `(?:받습니다|지급됩니다|지급합니다|입니다)` — **단정 서술어 4종**. 이 서술어가 붙어야만 "단정적"이라고 판단한다.

치환 함수(`guards.py:53-57`):

```python
def guard_generation(text: str) -> str:
    def _repl(m: re.Match) -> str:
        return f"참고 추정 범위(약 {m.group(1)} 내외, 약관·근거 기준)"

    return _ABS_AMOUNT.sub(_repl, text or "")
```

- 잡아낸 금액(`m.group(1)`)은 그대로 살리고, 그 앞뒤를 `참고 추정 범위(약 500만원 내외, 약관·근거 기준)` 같은 부드러운 표현으로 바꾼다(`:55`).
- 예: `"보험금 500만원을 받습니다"` → `"보험금 참고 추정 범위(약 500만원 내외, 약관·근거 기준)"`.
- `text or ""`로 빈 입력을 막는다(`:57`). 이 함수만 **동기(sync)**다(AI를 부르지 않아서 기다릴 일이 없다).

> 쉽게 말하면: "이만큼 받습니다"라는 확정 어투를 "이 정도로 추정됩니다"라는 조심스러운 어투로 자동 순화하는 자동 교정기다. 보험금은 실제로 확정 지급을 장담하면 안 되기 때문에 이런 완충 장치를 둔다.

### 3.2 인용 강제 로직 — **가드레일 모듈에는 없음**

docs·README는 생성 단계에서 "모든 사실 주장에 `[조항번호][판례번호]` 형식의 인용을 반드시 넣도록 프롬프트로 강제한다"고 적어 놓았다(`06_guardrail.md:21`, `README.md:10`). 그러나 **`guard_generation`에는 인용을 강제하는 코드가 전혀 없다.** 이 함수는 오직 금액 표현만 바꾼다.

실제 "인용 강제"는 가드레일 모듈이 아니라 **호출하는 쪽(report_worker)의 프롬프트 수준**에서 일어난다. `report_compose`의 시스템 프롬프트(AI에게 역할과 규칙을 지시하는 첫 지시문)가 인용을 넣으라고 말한다(`agents.py:504`):

> `"너는 보험 손해사정 리포트 작성자다. 사실 주장에는 약관 조항 인용을 포함하고, 금액은 단정하지 말고 범위로 쓴다."`

따라서 인용 강제는 "가드레일이 검사해서 막는 규칙"이 아니라 "AI에게 이렇게 써 달라고 부탁하는 지침"으로만 존재한다(강제력 없음). 더 자세한 판별은 6절에서 다룬다.

> 쉽게 말하면: 인용을 넣으라는 건 규정으로 막는 게 아니라 AI에게 "가급적 이렇게 써 줘" 하고 당부하는 수준이다. AI가 안 지켜도 걸러내는 장치는 아직 없다.

### 3.3 report_compose에서의 사용

`report_compose` 노드가 AI로 리포트 본문을 만들어 낸 직후 딱 한 번 호출한다(`agents.py:500-521`):

```python
    body = await ai_client.chat(
        [
            {
                "role": "system",
                "content": "너는 보험 손해사정 리포트 작성자다. 사실 주장에는 약관 조항 인용을 포함하고, 금액은 단정하지 말고 범위로 쓴다.",
            },
            ...
        ]
    )
    body = guardrail.guard_generation(body)
```

여기서 중요한 범위 한정이 있다. `guard_generation`은 **AI가 서술한 본문 `body`에만** 적용된다(`agents.py:521`). 그 뒤에 `sections` 딕셔너리(항목별로 값을 담는 사전 형태의 자료구조)로 조립되는 다른 필드(`5_추정보상범위` = `str(estimated_range)`, `5b_장해지급률` 등, `agents.py:541-555`)나 `issues`는 이 치환을 거치지 않는다. 즉 숫자가 노출되는 다른 섹션들은 생성 가드레일의 손이 닿지 않는 곳에 있다. `estimated_range`가 애초에 범위(`{"min":.., "max":..}`) 형태로 만들어져 단정 서술어가 붙을 일이 없으니 문제 소지가 적다는 설계 전제로 보인다.

---

## 4. 출력 가드레일 — `guard_output`

### 4.1 전체 코드(verbatim)

```python
DISCLAIMER = (
    "본 분석은 참고용이며 법적 효력이 없습니다. "
    "정확한 보험금 지급 여부는 담당 손해사정사의 검토 후 확정됩니다."
)
```
(`guards.py:16-19`)

```python
async def guard_output(
    text: str, *, run_judge: bool = True, chunks: list | None = None
) -> OutputGuardResult:
    final = text or ""
    if DISCLAIMER not in final:
        final = f"> {DISCLAIMER}\n\n{final}"

    judge_failures: list[str] = []
    if run_judge and chunks:
        # LLM Judge: 리포트의 인용·주장이 검색 청크 원문과 부합하는지 검증
        ctx = "\n---\n".join(
            (c.get("text", "") if isinstance(c, dict) else str(c))[:500] for c in chunks[:6]
        )
        verdict = await ai_client.chat_json(
            [
                {
                    "role": "system",
                    "content": (
                        "너는 보험 리포트 검증관이다. 리포트의 사실 주장이 제공된 약관 원문으로 "
                        "뒷받침되는지 검증한다. JSON만 출력."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"[약관 원문]\n{ctx}\n\n[리포트]\n{final[:2000]}\n\n"
                        '근거 없는(환각) 주장이 있으면 {"failures": ["문장1", ...]}, '
                        '없으면 {"failures": []} 형식으로만 답하라.'
                    ),
                },
            ]
        )
        if isinstance(verdict, dict):
            judge_failures = list(verdict.get("failures", []) or [])

    return OutputGuardResult(final_text=final, judge_failures=judge_failures)
```
(`guards.py:61-96`)

### 4.2 법적 고지문 삽입

- 고지문 `DISCLAIMER`는 모듈 상수(`guards.py:16-19`)로, docs에 적힌 문구와 정확히 일치한다(`06_guardrail.md:25`).
- 삽입 방식은 **멱등(idempotent, 같은 걸 여러 번 실행해도 결과가 한 번 한 것과 똑같음)**하다: `if DISCLAIMER not in final`일 때만 앞에 붙이므로(`:65-66`) 이미 고지문이 있으면 또 붙이지 않는다.
- 붙는 형식은 Markdown 인용블록: `> {DISCLAIMER}\n\n{final}`(`:66`).
- `text or ""`로 빈 입력을 막는다(`:64`).

> 쉽게 말하면: 고지문 붙이기를 실수로 두 번 돌려도 안내문이 두 줄로 겹치지 않는다. "이미 있으면 안 붙인다"는 조건 덕분이다.

### 4.3 LLM Judge 인용검증 (`run_judge`)

여기서 **LLM Judge**란, AI가 만든 리포트를 또 다른 AI가 심사관처럼 검사하는 것이다. "리포트가 근거 자료(약관 원문)에 실제로 나오는 내용만 말했는지, 아니면 없는 얘기를 지어냈는지(환각)"를 판정한다.

- **실행 조건**: `run_judge`와 `chunks`가 **둘 다** 참이어야 검사가 돌아간다(`:69`). 기본값은 `run_judge=True`(`:62`)지만 `chunks`(근거로 쓸 검색 조각들)가 비어 있으면(`None`이거나 빈 리스트) **검사를 건너뛴다** → `judge_failures=[]`. 즉 RAG 검색이 아무것도 못 찾아 `retrieved_clauses`가 비어 있는 리포트는 환각 검사를 아예 받지 못한다.
- **컨텍스트(심사관에게 보여줄 근거) 구성**(`:71-73`): 검색 조각을 **최대 6개**(`chunks[:6]`)까지만, 각 조각 텍스트를 **500자에서 잘라(`[:500]`)** `\n---\n`로 이어 붙인다. 조각이 dict면 `c.get("text","")`로, 아니면 `str(c)`로 안전하게 꺼낸다.
- **프롬프트**(`:76-90`): 시스템 역할은 "보험 리포트 검증관", 유저 메시지에는 `[약관 원문]`(ctx)과 `[리포트]`(**앞 2000자만**, `final[:2000]`)를 넣고, 근거 없는(환각) 주장을 `{"failures": ["문장1", ...]}` 형식으로 답하라고 요구한다.
- **호출**: `core.ai_client.chat_json`(`:74`). 이 도우미는 AI가 돌려준 답에서 코드펜스(백틱 표시)를 떼고 중괄호를 찾아내는 식으로 JSON을 너그럽게 해석하며, **해석에 실패하면 빈 `{}`를 돌려준다**(`ai_client.py:101-132`). HTTP 오류(`AiClientError`)는 위로 그대로 전달되는데 `guard_output`은 이를 직접 잡지 않으므로 → 더 상위의 `safe_node` 데코레이터가 처리한다(5절).
- **결과 파싱**(`:93-94`): `if isinstance(verdict, dict): judge_failures = list(verdict.get("failures", []) or [])`. 심사관 AI가 형식을 어겨 `{}`를 주거나 `failures` 키가 없으면 → **빈 리스트(통과로 간주)**. 즉 심사관 답변이 망가지면 조용히 "실패 없음"으로 넘어간다.

> 쉽게 말하면: 이 검사는 실패했을 때 막는 게 아니라 통과시키는 쪽(fail-open)으로 기운다. 심사관 AI가 답을 이상하게 주면 "문제없음"으로 처리해 버린다는 뜻이다. 안전하게 막기보다 흐름을 끊지 않는 쪽을 택한 설계다.

- 통신은 Ollama HTTP(EXAONE, 로컬에서 돌리는 오픈 LLM)로 이뤄지도록 설계됐고(`06_guardrail.md:50`), 실제 접속 주소와 모델은 `core.config.settings`가 넣어 준다(`ai_client.py:82`).

### 4.4 결과 모델 `OutputGuardResult`

`core.contracts`에 정의(`contracts.py:139-143`):

```python
class OutputGuardResult(BaseModel):
    """출력 가드레일 결과. 법적 고지문 삽입 + (리포트 한정) LLM Judge 인용 검증."""

    final_text: str  # 고지문 삽입된 최종 텍스트
    judge_failures: list[str]  # 인용 검증 실패 섹션(리포트만, run_judge=True)
```

- `final_text`: 고지문이 붙은 최종 텍스트. **심사관이 환각을 찾아내더라도 `final_text`에서 그 문장을 지우거나 바꾸지 않는다** — 원문에 고지문만 얹은 채 그대로 나간다.
- `judge_failures`: 환각으로 지목된 문장들의 목록. **모으기만 하고 텍스트에는 반영하지 않는다.** docs가 말한 "불일치가 발견되면 그 섹션을 `[인용 검증 실패 — 삭제됨]`으로 바꾼다"(`06_guardrail.md:25`)는 **아직 구현되지 않았다**(6절 참조).

> 쉽게 말하면: 심사관이 "이 문장 근거가 없어요" 하고 표시는 하지만, 실제로 그 문장을 삭제하거나 고치지는 않는다. 지적 사항을 메모로 남길 뿐, 최종 결과물은 손대지 않는 "관찰 전용" 상태다.

---

## 5. report_worker 연동

`report_worker`는 LangGraph의 `StateGraph`(작업 단계를 노드로 잇는 흐름도)로 노드를 조립하며(`graph.py:14-72`), 가드레일 3단계가 서로 다른 3개 노드에서 호출된다. 모든 노드는 `@safe_node` 데코레이터로 감싸져 있어서 **오류가 나도 예외를 삼켜 `errors`에 적어 두고, 만들 수 있는 부분결과로 계속 진행한다**(`agents.py:30-44`).

> 쉽게 말하면: 데코레이터는 함수에 덧씌우는 "겉포장"이다. `@safe_node`라는 포장이 각 단계를 감싸서, 한 단계가 넘어져도 전체가 멈추지 않고 "여기서 이런 오류가 났다"는 메모만 남긴 채 다음으로 넘어가게 해 준다.

### 5.1 입력 가드레일 노드 → 차단 라우팅 → `persist_blocked`

**(1) `input_guardrail` 노드**(`agents.py:123-129`):

```python
@safe_node
async def input_guardrail(state: ReportState) -> dict[str, Any]:
    g = await guardrail.guard_input(state.get("masked_text", ""))
    out: dict[str, Any] = {"masked_text": g.masked_text}
    if g.blocked:
        out["errors"] = _err(state, f"input_blocked:{g.reason}")
    return out
```

- 입력으로 쓰는 `state["masked_text"]`는 바로 앞 단계 `load_context`가 DB의 `ocr_results.masked_text`에서 읽어온 값이다(`agents.py:113-119`, `state.py:24`). 즉 OCR 워커가 이미 한 번 가린 텍스트를 **여기서 또 한 번 가린다**(이중 방어). README·docstring이 강조하는 "PII 규칙은 ocr_worker 입력단과 동일해야 한다"(`guards.py:8`, `README.md:16`)는 이 이중 적용이 어긋나지 않게 하는 전제다.
- 반환하는 dict가 `masked_text`를 다시 덮어쓴다(`:126`).
- 차단됐을 때만 `errors`에 `f"input_blocked:{g.reason}"` 문자열을 덧붙인다(`:127-128`). `_err`은 기존 errors 뒤에 메시지를 이어 붙이는 도우미(`agents.py:26-27`). 통과할 때는 `errors` 키를 돌려주지 않아 기존 errors가 그대로 보존된다.

> 쉽게 말하면: 개인정보를 OCR 단계에서 한 번, 여기서 또 한 번 가린다. 자물쇠를 두 개 채우는 셈이라, 두 곳의 가리기 규칙이 똑같아야 한쪽이 빠뜨려도 다른 쪽이 잡아 준다.

**(2) 차단 분기 `route_after_input`**(`agents.py:195-199`):

```python
def route_after_input(state: ReportState) -> str:
    """input_guardrail이 도메인외/차단을 표시하면 LLM 파이프라인을 건너뛴다."""
    if any(str(e).startswith("input_blocked") for e in state.get("errors", [])):
        return "blocked"
    return "diagnosis"
```

- `errors` 안에 `"input_blocked"`로 시작하는 항목이 하나라도 있으면 `"blocked"`, 없으면 `"diagnosis"`를 돌려준다. 이 값이 다음에 어디로 갈지를 정한다.

**(3) 그래프 조건부 엣지**(`graph.py:37-41`):

```python
    g.add_conditional_edges(
        "input_guardrail",
        agents.route_after_input,
        {"blocked": "persist_blocked", "diagnosis": "diagnosis"},
    )
```

- `"blocked"`면 `persist_blocked` 노드로, `"diagnosis"`면 정상 파이프라인으로 간다. 주석대로 "차단되면 LLM 파이프라인을 건너뛰어 비용과 잘못된 출력을 막는다"(`graph.py:35-36`).

> 쉽게 말하면: 엉뚱한 질문으로 판정되면 값비싼 AI 처리를 아예 돌리지 않고 곧장 "차단 기록" 단계로 빠진다. 헛돈과 헛수고를 아끼는 지름길이다.

**(4) `persist_blocked` 노드**(`agents.py:634-658`):

```python
@safe_node
async def persist_blocked(state: ReportState) -> dict[str, Any]:
    """입력 가드레일 차단 시 reports.status만 'BLOCKED'로 갱신한다(초안 없음).
    ...
    TODO(spring-contract): reports.status enum에 'BLOCKED'를 추가해 정렬해야 한다
    (현재 계약: AWAITING_INSPECTION|AWAITING_ADOPTION|...). BLOCKED는 여기서 신설한 값이다.
    """
    reasons = [str(e) for e in state.get("errors", []) if str(e).startswith("input_blocked")]
    logger.info(
        "report blocked by input guardrail", report_id=state.get("report_id"), reasons=reasons
    )

    rid = uuid.UUID(state["report_id"])
    pool = db.get_pool()
    async with pool.acquire() as c:
        await c.execute(
            "UPDATE reports SET status = 'BLOCKED', updated_at = now() WHERE id = $1",
            rid,
        )
    return {"errors": state.get("errors", [])}
```

- `input_blocked` 사유만 추려서 **구조화 로그로만 남기고**(초안=리포트 내용물이라 차단됐을 땐 아예 안 만든다), `report_drafts`는 생성하지 않는다(`:645-649`).
- `reports.status`를 `'BLOCKED'`로만 바꾼다(`:654-657`). docstring이 밝히듯 `'BLOCKED'`는 **여기서 새로 만든 값**이라, Spring 쪽 status 목록(enum) 계약에는 아직 반영되지 않은 TODO 상태다(`:642-643`) — 아직 서로 맞지 않는 지점임을 정직하게 표시.
- 이 노드가 필요한 이유: 없으면 status가 영영 갱신되지 않아 사용자나 Spring 쪽에서 "처리중"으로 무한히 표시된다(`:639-641`, `graph.py:35-36`).
- 이 뒤로 `persist_blocked → END`(`graph.py:70`).

### 5.2 출력 가드레일 노드 → `judge_failures`를 state에 남기는 방식

**`output_guardrail` 노드**(`agents.py:561-566`):

```python
@safe_node
async def output_guardrail(state: ReportState) -> dict[str, Any]:
    g = await guardrail.guard_output(
        state.get("report", ""), run_judge=True, chunks=state.get("retrieved_clauses", [])
    )
    return {"report": g.final_text, "judge_failures": g.judge_failures}
```

- 입력: `state["report"]`(바로 앞 `report_compose`가 만든 통합 Markdown, `agents.py:556-557`)와 `chunks=state["retrieved_clauses"]`(RAG가 채운 검색 조각, `state.py:32`).
- `run_judge=True`로 고정 → **리포트 경로에서는 항상 심사관을 켠다**(챗봇은 이 노드를 쓰지 않으므로 LLM Judge 미적용, `06_guardrail.md:25`·`:41`, `README.md:11`).
- 결과로 `report`(고지문 붙은 최종본)와 `judge_failures`를 state에 합쳐 넣는다(`state.py:48`의 `judge_failures: list[str]` 칸).
- 그래프 순서: `report_compose → output_guardrail → persist → END`(`graph.py:66-69`).

**`persist`가 judge_failures를 어떻게 처리하나**(`agents.py:570-592`):

```python
    draft = {
        "sections": state.get("sections", {}),
        "estimated_range": state.get("estimated_range", {}),
        "disclaimer": guardrail.DISCLAIMER,
        "judge_failures": state.get("judge_failures", []),
        ...
    }
```

- `judge_failures`는 `report_drafts.draft`(jsonb 컬럼, JSON을 통째로 담는 DB 자료형)에 그대로 **보관만** 된다(`agents.py:584`, `:599-604`).
- 중요: `judge_failures`가 비어 있지 않아도(=환각이 지목돼도) **리포트 저장을 막거나 status를 바꾸지 않는다.** `persist`는 `reports.status`를 `'AWAITING_ADOPTION'`으로 평소처럼 갱신한다(`agents.py:609`). 즉 환각 지목 결과는 "기록으로만 남고 관문 역할은 하지 않는다"(fail-open, 나중에 사람이 검토할 때 참고하는 신호).
- 고지문 문구도 draft에 별도 필드로 한 번 더 저장된다(`draft["disclaimer"] = guardrail.DISCLAIMER`, `agents.py:583`).

> 쉽게 말하면: 심사관이 "이 문장 의심스럽다"고 표시해도 리포트는 그대로 저장되고 다음 단계로 넘어간다. 그 표시는 통과를 막는 문지기가 아니라, 나중에 사람이 볼 때 "여기 한 번 보세요" 하는 포스트잇에 가깝다.

### 5.3 생성 가드레일 노드

앞서 3.3에서 본 대로 `report_compose` 노드 안에서 `body = guardrail.guard_generation(body)`로 호출된다(`agents.py:521`). 별도 노드가 아니라 리포트 조립 노드 안의 한 단계다.

### 5.4 연동 흐름 요약

```
START → load_context → input_guardrail
        └─(route_after_input)─┬─ blocked ───────────────→ persist_blocked → END
                              └─ diagnosis → ... → report_compose(guard_generation)
                                     → output_guardrail(guard_output: 고지문+Judge)
                                     → persist(judge_failures를 draft에 보존) → END
```
(`graph.py:32-72`)

> 표 읽는 법: 위 그림은 리포트가 거치는 길이다. 입력 검사에서 엉뚱한 질문이면 위쪽 갈래(blocked)로 빠져 곧장 끝나고, 정상이면 아래쪽 갈래(diagnosis)로 내려가 진단→작성→출력 검사→저장을 차례로 거쳐 끝난다.

---

## 6. 구현 상태 — 실제 구현 vs 스텁/단순 규칙 (정직 판별)

`guards.py`가 96줄로 작은 것은 우연이 아니라, **문서가 설명한 기능 중 상당수가 아직 단순 규칙이거나 미구현**이기 때문이다. 항목별로 정직하게 구분한다.

> 참고: 여기서 "스텁(stub)"은 자리만 잡아 둔 임시·간이 구현을, "미구현"은 문서엔 있지만 코드엔 아직 없는 기능을 뜻한다.

| 문서(docs/README) 서술 | 실제 guards.py | 판정 |
|---|---|---|
| 입력: "정규식 **+ NER**로 PII 마스킹", "이름·주소 탐지"(`06_guardrail.md:17`, `README.md:9`) | `_mask_pii`는 **정규식 3종뿐**. NER 모델 import·호출 전혀 없음. 이름/주소 마스킹 없음 | **NER 미구현** — `guard_input`이 `async`인데 `await` 없는 이유는 향후 NER 자리 예약으로 추정 |
| 생성: "모든 사실 주장에 `[조항][판례]` 인용 **강제**"(`06_guardrail.md:21`, `README.md:10`) | `guard_generation`은 금액 치환만. 인용 관련 코드 없음 | **인용 강제 미구현** — 강제성은 `report_compose` 프롬프트 지침(`agents.py:504`)뿐, 검증/차단 없음 |
| 출력: "불일치 발견 시 해당 섹션을 `[인용 검증 실패 — 삭제됨]`으로 치환"(`06_guardrail.md:25`, `README.md:11`) | `guard_output`은 `judge_failures` 수집만. `final_text`에서 삭제·치환 없음 | **섹션 치환 미구현** — 결과는 기록만 되고 텍스트에 미반영 |
| 도메인 차단 | `_OFF_DOMAIN` 7개 키워드 substring 매칭(`guards.py:22`, `:38-39`) | **단순 규칙(스텁성)** — 오탐/누락 가능(예: "주식형 보험" 오차단, 미등록 도메인 통과) |

> 표 읽는 법: 왼쪽은 "문서가 하겠다고 적어 둔 것", 가운데는 "코드가 실제로 하는 것", 오른쪽은 그 둘을 대조한 결론이다. 요약하면 문서의 약속 중 고급 기능 세 가지가 아직 코드에 없다.

**정상 구현으로 판정되는 것:**

- 주민번호 앞 6자리 보존 마스킹, 전화·계좌 마스킹 — 실제로 동작한다(`guards.py:26-33`). 문서 규칙과 일치.
- 단정 금액 표현 치환 — 동작은 하지만 서술어 4종(`받습니다/지급됩니다/지급합니다/입니다`)과 `원` 단위에만 걸리는 **좁은 규칙**이다(`guards.py:48-50`). `"원"` 없이 쓴 `"5백만 지급"`이나 `"~로 산정됩니다"` 같은 표현은 놓친다.
- 법적 고지문 멱등 삽입 — 실제로 동작한다(`guards.py:64-66`).
- LLM Judge — **실제 LLM 호출까지 구현돼 있다**(`guards.py:69-94`). 다만 (a) `chunks`가 없으면 건너뛰고, (b) 리포트 앞 2000자와 청크 6개×500자만 검사하며, (c) 심사관 응답이 깨지면 통과로 처리(fail-open)하고, (d) 결과가 텍스트나 저장 관문에 반영되지 않는다 — 즉 "찾아내기는 하되 손은 대지 않는" 관찰 전용 단계다.

**추가 격차/주의:**

- 계좌번호 마스킹은 주석("연속 10자리 이상")과 실제 정규식(하이픈 3덩이)이 서로 맞지 않고(`guards.py:31-32`), 하이픈 없이 쭉 이어진 계좌 숫자열은 가려지지 않는다.
- `reports.status = 'BLOCKED'`는 Spring status enum 계약에 아직 반영되지 않은 신설값이다(`agents.py:642-643` TODO).
- 챗봇(12) 연동 코드는 이번 조사 범위(`report_worker`)에 들어 있지 않다. docs상 챗봇은 입력·생성·출력은 쓰되 LLM Judge만 빼는 것으로 설계됐지만(`06_guardrail.md:37-41`), 실제 챗봇 호출부는 본 문서가 다루는 파일에 포함되지 않아 확인할 수 없다.

**총평:** 가드레일은 "3단계 뼈대 + 결정론 규칙(마스킹·치환·고지문) + LLM Judge 관찰"까지는 실제로 작동한다. 하지만 문서가 약속한 고급 기능(NER 기반 PII 탐지, 인용 강제 검증, 환각 섹션 자동 삭제)은 아직 프롬프트 지침에 머물거나 미구현으로 남아 있다.

> 쉽게 말하면: 정해진 패턴대로 처리하는 부분(개인정보 가리기·문구 바꾸기·고지문 붙이기)은 믿고 써도 될 만큼 확실하다. 반면 AI가 똑똑하게 판단해야 하는 부분(엉뚱한 질문 걸러내기, 지어낸 말 조치하기)은 아직 초기·간이 수준이라는 점을 감안하고 사용해야 한다.
