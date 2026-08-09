# 후유장해 지급률 산정 파이프라인 — 심층

> 관련 코드: `src/report_worker/nodes/agents.py`, `src/report_worker/disability_rules.py`,
> `src/rag/search.py`
> 상위 문서: [README.md](./README.md) · 원문 데이터: [schedule-data.md](./schedule-data.md)

이 문서는 리포트 워커에서 **후유장해 지급률을 어떻게 산정하는가**를 코드 흐름 그대로 설명한다.

---

## 0. 설계 원칙 — "분류는 LLM, 합산은 결정론"

후유장해 산정은 두 단계로 엄격히 분리돼 있다.

1. **분류·추출 (`disability_rag`)** — 사고가 장해분류표의 어느 항목에 해당하고 지급률이 몇 %인지
   찾는다. 이건 자연어 이해가 필요해 **LLM**이 한다. 단, **지급률 숫자는 원문 표에서만** 나오도록
   결정론 백스톱으로 검증한다.
2. **합산 (`disability_calc` → `combine_disability_rate`)** — 여러 장해의 지급률을 총칙 규칙에 따라
   합산한다. 이건 **LLM·DB·IO가 전혀 없는 순수 함수**라 재현·감사·단위테스트가 된다.

이렇게 나눈 이유: 지급률은 **돈에 직결**되므로, 확률적으로 흔들리는 LLM이 최종 숫자를 만들면 안 된다.
LLM은 "어느 항목인가"까지만, 최종 계산은 검증 가능한 코드가 한다.

---

## 1. 진입 조건 — 언제 장해 분기를 타나

`case_search` 다음의 조건 분기 `route_after_case` (`agents.py:279`):

```python
def route_after_case(state):
    if (state.get("diagnosis") or {}).get("requires_disability_review"):
        return "disability"        # → disability_rag → disability_calc → payment_calc
    return "payment_calc"          # 장해 건너뛰고 직행
```

`requires_disability_review`는 `diagnosis` 노드에서 **LLM이 판단**한다 (`agents.py:151`).
진단서에 "영구 후유장해가 예상되며 장해지급률 평가가 필요" 같은 소견이 있으면 True.

> **라우팅 불변식** (`scripts/battery.py:212`가 검증): `requires_disability_review`가 True면
> 반드시 장해 분기가 실행돼 `disability_analysis`에 결과를 남겨야 한다.

---

## 2. disability_rag 노드 — 분류·지급률 추출

`agents.py:382`. 4단계로 진행한다.

### 2-1. 쿼리 구성 + 1차 검색 (가입 약관)

```python
query = f"{dx_name} {icd} 후유장해 장해분류표 지급률"
res = await hybrid.search(query, namespaces=["terms"], top_k=8,
                          insurer=ci.get("insurer"), product=ci.get("product_name"))
sched = _select_schedule(res.get("ranked_chunks", []))
```

먼저 **가입 약관(`terms`)** 안에 장해분류표가 있는지 찾는다. `_select_schedule` (`agents.py:292`)가
`chunk_type == "schedule"`인 청크를 우선 고르고, 없으면 "장해의 분류" 헤더 휴리스틱으로 폴백.

### 2-2. 폴백 — 표준 장해분류표(level)

가입 약관에 분류표가 없으면(대부분의 경우) **금감원 표준 장해분류표**로 재검색:

```python
if not sched:
    res = await hybrid.search(query, namespaces=["level"], top_k=8,
                              contract_date=enrolled_at)     # ★ 계약일 전달
    sched = res.get("ranked_chunks", [])
    is_fallback = bool(sched)
```

- `namespaces=["level"]` → `schedule_chunks` 테이블 검색 (금감원 시행세칙 원문).
- `contract_date=enrolled_at` → **계약 체결일이 속하는 개정판만** 반환 (버전 매칭).
  `enrolled_at`은 `user_insurances.enrolled_at`에서 온다 (`load_context`, `agents.py:109`).
  버전 필터 상세는 아래 §5, 데이터 출처는 [schedule-data.md](./schedule-data.md).

### 2-3. 둘 다 비면 — 빈 결과 + 마커

`terms`·`level` 모두 실패하면 기존 동작 유지 (`agents.py:409`):
`disability_analysis`를 `combined_rate: 0.0`, `confidence: "low"`, `caveat: "장해분류표 미검색"`로
두고 `errors`에 `disability_schedule_missing` 기록.

### 2-4. LLM 추출 + 결정론 백스톱 (`_extract_schedule_items`, `agents.py:300`)

찾은 장해분류표 원문을 LLM에 주고 항목을 추출한다. **핵심은 지급률 검증.**

LLM에 요구하는 항목별 필드:
```
injury          부상/장해명
body_region     신체부위 (눈·귀·코·씹기말하기·척추·체간골·팔·다리·손가락·발가락·흉복부장기·신경계정신)
category_label  원문 항목 텍스트 그대로 복사
rate            지급률 % (number)
rate_quote      rate 숫자가 등장한 원문 구절 그대로 복사
temporary       한시장해 여부 (bool)
temporary_years 존속기간 (number|null)
citation        원문 source_ref
```

**결정론 백스톱** (`agents.py:351`) — LLM이 지어낸 지급률을 거르는 장치:
```python
verified = bool(quote) and (str(int(rate_f)) in sched_text)
```
지급률 숫자가 실제 원문(`sched_text`)에 존재해야 `verified=True`. 미검증 항목은
`rule_notes`에 "미검증 지급률 제외"로 남기고 합산에서 뺀다.

> ⚠️ **이 백스톱은 현재 헐겁다 — [known-issues.md](./known-issues.md) 버그 3 참조.**
> (1) `rate_quote`가 원문에 있는지 검사하지 않고 비었는지만 봄. (2) `int()`가 소수를 버리고
> 부분 문자열 매칭이라 "제12조"·"12개월"에도 걸림. 두 자리 숫자면 사실상 무조건 통과.

**신뢰도 산정** (`agents.py:370`):
- `high`: 모든 항목이 verified + LLM이 uncertain=false
- `medium`: 일부만 verified
- `low`: verified 0건

### 2-5. 폴백 시 신뢰도 캡 + 캐비앗

표준표 폴백이면 (`agents.py:427`):
- `caveat = "표준 장해분류표 기준(가입 약관 미확보) — 개별 약관 확인 필요"`
- `enrolled_at`이 None이면 `" · 가입일 미상 — 현행판 기준"` 추가
- **신뢰도 high → medium으로 캡** (표준표는 개별 약관과 다를 수 있으므로)
- `errors`에 `disability_fallback_standard_schedule` 마커 (운영 추적용)

---

## 3. disability_calc 노드 — 결정론 합산

`agents.py:456`. 단순하다 — verified 항목만 골라 순수 함수에 넘긴다:

```python
da = state.get("disability_analysis", {})
verified = [i for i in da.get("items", []) if i.get("verified")]
result = combine_disability_rate(verified)
# → disability_analysis에 combined_rate, normalized_items, rule_notes 병합
```

**verified=True인 항목만 합산에 산입**한다. §2-4의 백스톱을 통과하지 못한 지급률은 여기서 배제된다.

---

## 4. combine_disability_rate — 합산 규칙 (`disability_rules.py:36`)

MVP 4대 규칙. **각 규칙이 금감원 총칙 원문과 어떻게 대응하는지** 함께 표기한다
(원문: `tempVectorDB/schedule_data/b3_rev2018.txt`).

### 규칙 1 — 한시장해 환산 (`disability_rules.py:23`)

> **총칙 1-4항 원문:** "영구히 고정된 증상은 아니지만 치료 종결 후 한시적으로 나타나는 장해에
> 대하여는 그 기간이 **5년 이상인 경우 해당 장해지급률의 20%**를 장해지급률로 한다."

- `temporary=True` + `temporary_years >= 5` → 지급률 × 0.20
- `temporary_years < 5` 또는 미상 → **미산입(0%)**
- 상수: `_TEMPORARY_MIN_YEARS = 5.0`, `_TEMPORARY_FACTOR = 0.20`

### 규칙 2 — 동일 신체부위는 최고값만 (`disability_rules.py:60`)

> **총칙 3-2항 원문:** "동일한 신체부위에 2가지 이상의 장해가 발생한 경우에는 **합산하지 않고
> 그중 높은 지급률**을 적용함을 원칙으로 한다."

- `body_region`으로 그룹핑 → 각 그룹에서 `effective_rate` 최고값만 인정
- `rule_notes`에 "동일부위(X) N건 중 최고 Y%만 인정" 기록

### 규칙 3 — 서로 다른 부위는 합산 (`disability_rules.py:72`)

> **총칙 2항 원문:** 13개 신체부위 정의 + "다만, **좌·우의 눈, 귀, 팔, 다리, 손가락, 발가락은
> 각각 다른 신체부위로 본다.**"

- 부위별 최고값들을 **합산**
- `rule_notes`에 "서로 다른 N개 부위 합산 = Z%" 기록

> ⚠️ **좌우 구분 미구현 — [known-issues.md](./known-issues.md) 버그 1.** 원문은 좌팔/우팔을
> 다른 부위로 보지만 `body_region`에 좌우 개념이 없어 같은 "팔"로 묶여 **과소산정**된다.

### 규칙 4 — 상한 100% (`disability_rules.py:78`)

- `combined > 100` → 100으로 캡, `rule_notes`에 기록
- 상수: `_MAX_RATE = 100.0`

### 반환 형태

```python
{"combined_rate": float, "rule_notes": list[str], "normalized_items": list[dict]}
```

### 미구현 규칙 (코드 주석에 "후속 단계"로 명시)

> **총칙 3-3항 (파생장해):** "하나의 장해로 둘 이상의 파생장해가 발생하는 경우 각 파생장해의
> 지급률을 합산한 지급률과 최초 장해의 지급률을 비교하여 그 중 높은 지급률을 적용."

기존장해 공제·파생장해 세부·부위별 상한도 미구현. [known-issues.md](./known-issues.md) 참조.

### 단위테스트 (`tests/test_disability_combine.py`)

6개 케이스로 규칙 1~4 커버 (부위 합산·동일부위 흡수·상한·한시장해 5년/미만·빈 입력).
**LLM/DB 없이 순수 함수만** 테스트하므로 Docker 없이도 통과한다.

---

## 5. 계약일 → 개정판 버전 매칭 (`src/rag/search.py:95`)

표준 장해분류표는 개정판마다 지급률이 다를 수 있어, **계약 체결일이 속하는 판**만 검색해야 한다.

```python
def _version_filter(namespace, args, contract_date):
    if namespace not in _VERSION_FILTER_NS:      # level namespace에만 적용
        return ""
    if contract_date is None:
        return " AND applies_to IS NULL"          # 현행판만
    args.append(contract_date)
    idx = len(args)
    return f" AND applies_from <= ${idx} AND (applies_to IS NULL OR ${idx} < applies_to)"
```

**반열림 구간 `[applies_from, applies_to)`.** `applies_to`가 exclusive라 3개 개정판이
겹침·빈틈 없이 이어진다:

| 개정판 | 적용 구간 |
|--------|-----------|
| 2005 계열 | `[2005-04-01, 2018-04-01)` |
| 2018.4 개정 | `[2018-04-01, 2025-06-30)` |
| 2025.6 개정(현행) | `[2025-06-30, NULL)` |

예: 계약일 2020-05-01 → 2018.4 판. 계약일 2018-04-01(경계) → 2018.4 판(2005의 `< 2018-04-01`은
배제, 2018의 `applies_from <= 2018-04-01`은 포함).

> ⚠️ **`enrolled_at`이 NULL이면** `contract_date=None`이 돼 **조용히 현행판(2025.6)**으로 검색.
> 구계약인데 가입일이 안 잡히면 잘못된 버전 지급률이 나온다. 캐비앗으로 고지는 되지만 값은 틀림.
> [known-issues.md](./known-issues.md) 참조.

---

## 6. 하류 소비 — 어떻게 리포트에 반영되나

### payment_calc (`agents.py:472`)

장해지급률을 보상 추정 범위 산정에 반영:
```python
rate = float((state.get("disability_analysis") or {}).get("combined_rate") or 0.0)
factor_hi = 1.0 + min(rate, 100.0)/100.0 * 0.8 if rate > 0 else 1.8
# 0%→×1.0, 100%→×1.8. 가입금액 미보유 → 절대 보험금 불가, 모두 '추정'
```

### report_compose (`agents.py:492`)

장해 결과를 리포트 섹션 `5b_장해지급률`에 서술:
```
추정 합산 장해지급률 {combined_rate}% (신뢰도 {confidence}, 근거 {citations}) —
규칙 {rule_notes}. ※ {caveat}
```
`items`가 없으면 "해당 없음(후유장해 미검토)".

### persist (`agents.py:590`)

`disability_analysis` 전체를 `report_drafts.draft.disability`에 JSON으로 보존.
장해 인용(`da_cites`)은 약관 인용·판례 근거와 합쳐 `basis_terms_precedents`에 저장.

---

## 7. 한눈 요약

```
requires_disability_review (LLM 판단)
   │ True
   ▼
disability_rag
   ├─ terms 검색 → 장해분류표 있나?
   │     └─ 없으면 level(표준표) 폴백, 계약일로 개정판 매칭
   ├─ LLM 추출 (injury/body_region/rate/rate_quote/...)
   ├─ 결정론 백스톱: rate가 원문에 있나? → verified
   └─ 신뢰도 산정 (+ 폴백 시 medium 캡)
   ▼
disability_calc
   └─ verified 항목만 → combine_disability_rate
         규칙1 한시장해 20%/미산입
         규칙2 동일부위 최고값
         규칙3 부위 간 합산
         규칙4 상한 100%
   ▼
combined_rate → payment_calc(보상범위) → report_compose(5b섹션) → persist(draft.disability)
```

**돈에 직결되는 최종 숫자(`combined_rate`)는 검증 가능한 순수 함수가 만든다.** LLM은 분류까지만.
단, 현재 백스톱·좌우 구분에 결함이 있어 [known-issues.md](./known-issues.md)의 버그 1·3을
우선 수정해야 신뢰할 수 있다.
