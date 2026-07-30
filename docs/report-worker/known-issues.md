# 알려진 이슈 — 버그·미구현·TODO

> 점검일: 2026-07-15 (브랜치 `11-feature-langgraph-멀티에이전트-구현`)
> 상위 문서: [README.md](./README.md) · [disability-pipeline.md](./disability-pipeline.md) ·
> [schedule-data.md](./schedule-data.md)

원본 코드·원문 데이터와 직접 대조해 확인한 결함 목록. **우선순위는 "사용자 돈에 직접 영향 +
틀린 걸 맞다고 말하는 정도" 기준.**

---

## 우선순위 요약

| # | 이슈 | 심각도 | 영향 | 위치 |
|---|------|:---:|------|------|
| 1 | 좌우 신체부위 미구분 → 과소산정 | 🔴 높음 | 지급률이 실제보다 낮게 | `agents.py:327` + `disability_rules.py:57` |
| 2 | 검증 백스톱 무력화 → 환각 통과 | 🔴 높음 | 없는 지급률이 verified로 | `agents.py:351` |
| 3 | 신체부위 목록에 "외모" 누락 | 🟡 중간 | 흉터·추상 장해 오분류 | `agents.py:327` |
| 4 | `enrolled_at` NULL → 조용히 현행판 | 🟡 중간 | 구계약 버전 오매칭 | `agents.py:404` |
| 5 | 파생장해 규칙 미구현 | 🟡 중간 | 총칙 3-3항 미반영 | `disability_rules.py` |
| 6 | test_config가 로컬 `.env` 오염 | 🟢 낮음 | 로컬서 테스트 1건 상시 실패 | `tests/test_config.py:21` |
| 7 | ruff가 `scripts/`·`tempVectorDB/`서 실패 | 🟢 낮음 | CI 게이트 불가 | (린트 설정) |

---

## 버그 1 — 좌우 신체부위 미구분 (과소산정) 🔴

### 증상
좌팔 30% + 우팔 30% 사고가 `combine_disability_rate`에서 같은 `"팔"` 그룹으로 묶여
**최고값 30%만 인정**된다. 총칙대로면 다른 신체부위라 합산 60%.

### 원문 근거
> **총칙 2항:** "다만, **좌·우의 눈, 귀, 팔, 다리, 손가락, 발가락은 각각 다른 신체부위로 본다.**"

이 규칙은 적재 md 총칙에 그대로 보존돼 있다([schedule-data.md §5](./schedule-data.md)). **데이터엔
있는데 소비 계층이 못 쓴다.**

### 원인
- `agents.py:327` LLM 프롬프트의 `body_region`이 좌우 개념 없는 단일 라벨.
- `disability_rules.py:57` region 그룹핑이 그 라벨을 그대로 써서 좌우를 합쳐버림.

### 왜 나쁜가
이 서비스는 "보험금이 적게 나온 것 같다"는 사용자를 돕는 게 목적인데, 엔진이 **과소산정 방향**으로
틀린다. 양팔·양다리 사고는 교통사고에서 드물지 않다.

### 수정 방향
LLM 항목에 `laterality`(left|right|none) 필드 추가 → region 키를 `f"{body_region}:{laterality}"`
로 구성(좌우 구분 대상 6부위만). 회귀 테스트 추가.

---

## 버그 2 — 검증 백스톱 무력화 (환각 통과) 🔴

### 증상
LLM이 지어낸 지급률이 `verified=True`를 달고 합산에 산입될 수 있다. 신뢰도도 `high`로 뜬다.

### 원인 (`agents.py:351`)
```python
verified = bool(quote) and (str(int(rate_f)) in sched_text)
```
두 군데가 샌다:
1. **`rate_quote` 미검증** — "이 숫자가 등장한 원문 구절을 복사하라"고 받은 값인데, 코드는
   `bool(quote)`(비었는지)만 본다. `quote in sched_text`인지 확인 안 함 → 통째로 지어내도 통과.
2. **숫자 검사 헐거움** — `int(rate_f)`가 소수를 버려(12.5%→"12") 원문 전체에서 부분 문자열로
   찾는다. 원문엔 "제12조"·"12개월"·"120일"이 널려 있어 **두 자리 숫자면 사실상 무조건 매칭**.

### 왜 제일 위험한가
버그 1은 값이 틀리는 거지만, 이건 **틀린 걸 자신 있게(high confidence) 맞다고 말한다.**
"표에 없는 지급률은 절대 만들지 마라"는 설계 의도가 코드에서 안 지켜진다.

### 수정 방향
```python
quote_norm = re.sub(r"\s+", "", quote)
sched_norm = re.sub(r"\s+", "", sched_text)
rate_str = f"{rate_f:g}"  # 소수 보존
verified = bool(quote_norm) and quote_norm in sched_norm and rate_str in quote_norm
```
`_extract_schedule_items`의 검증 로직에 대한 단위테스트도 신설(현재 0개).

---

## 버그 3 — 신체부위 목록에 "외모" 누락 🟡

### 증상
`agents.py:327` LLM 프롬프트의 허용 `body_region`이 12개뿐 — **⑤외모**(흉터·추상 장해)가 빠짐.
화상·얼굴 흉터 케이스에서 부위를 `"기타"`로 떨구거나 엉뚱한 부위에 우겨넣음.

### 원문 근거
총칙 2항은 13개 부위이고 ⑤가 외모. 적재 md에도 `## 외모` 섹션이 실재
([schedule-data.md §5](./schedule-data.md)).

### 수정 방향
프롬프트 목록에 "외모" 추가(13개로). `disability_rules.py`는 라벨 무관하게 동작하므로 그대로.

---

## 버그 4 — `enrolled_at` NULL → 조용히 현행판 🟡

### 증상
`user_insurances.enrolled_at`이 NULL이면 `disability_rag`가 `contract_date=None`으로 검색
(`agents.py:404`) → 버전 필터가 **현행판(2025.6)만** 반환(`search.py:112`). 구계약인데 가입일이
안 잡히면 잘못된 개정판 지급률이 나온다.

### 완화 상황
`_STANDARD_NO_DATE_CAVEAT = "가입일 미상 — 현행판 기준"`으로 **고지는 된다.** 하지만 값 자체는
틀릴 수 있다(정신행동 2005=70 vs 2018=75, 귀 평형기능·흉복부 심장 등 버전 간 실차이 존재).

### 수정 방향
`enrolled_at` NULL을 명시적 에러/경고 마커로 승격하거나, 가입일 확보 전엔 장해 지급률을
"판정 보류"로 표기. 제품 정책 결정 필요.

---

## 버그 5 — 파생장해 규칙 미구현 🟡

### 내용
`disability_rules.py` 주석에 "후속 단계"로 명시된 알려진 부채:
> **총칙 3-3항:** "하나의 장해로 둘 이상의 파생장해가 발생하는 경우 각 파생장해의 지급률을
> 합산한 지급률과 최초 장해의 지급률을 비교하여 그 중 높은 지급률을 적용."

기존장해 공제·파생장해 세부·부위별 상한도 미구현.

### 상태
알려진 MVP 스코프 밖. 버그 1·2보다 발생 빈도 낮아 후순위. **별도 이슈로 관리 권장.**

---

## 버그 6 — test_config가 로컬 `.env` 오염 🟢

### 증상
`tests/test_config.py::test_model_names_default_empty_to_force_injection`이 로컬에서 상시 실패:
```
assert cfg.llm_model == ""
AssertionError: assert 'qwen3:8b' == ''
```

### 원인
`Settings`가 `env_file=".env"`(`config.py:23`)라 테스트가 `Settings()`를 부르면 로컬 `.env`를
읽어버린다. `.env`에 `LLM_MODEL=qwen3:8b`가 있어 "기본값 빈 문자열" 단언이 성립 불가.
→ **CI(`. env` 없음)에서만 통과, 개발자 로컬에선 항상 실패.** 방치하면 "원래 하나는 빨간색"이
돼 진짜 회귀를 놓친다.

### 수정 방향
`Settings(_env_file=None)`로 파일 로딩 끄고 `monkeypatch.delenv`로 프로세스 환경변수까지 격리.

---

## 버그 7 — ruff가 `scripts/`·`tempVectorDB/`에서 실패 🟢

### 증상
`ruff check .` → 31건 에러 + 포맷 2건. 단, `ruff check src tests migrations`는 **All checks
passed.** 걸린 건 전부 실험 스크립트·데이터 적재 코드(E501 긴 줄, SIM115 컨텍스트 매니저,
S101 assert, import 정렬 등).

### 영향
기능 무관하지만 `ruff check .`가 상시 빨간불이라 커밋 훅·CI 게이트로 못 쓴다.

### 수정 방향
둘 중 택1 — (a) `pyproject.toml`에서 `scripts/`·`tempVectorDB/` 제외, (b) 실제 정리.
일회성 ingest 도구 성격상 (a)가 현실적.

---

## 검증 못 한 범위 (환경 제약)

2026-07-15 점검 시 **Docker 미기동**으로 Kafka·PostgreSQL·Ollama 필요 경로는 검증 불가:
- RAG 실검색(terms/case/level namespace)
- 리포트 워커 E2E (`scripts/battery.py`, `scripts/kafka_smoke.py`)
- `load_schedule.py` 실적재

현재 초록불인 것은 **유닛 테스트 범위까지**다(70/71 통과, 실패 1건은 버그 6). 통합 검증은
`docker compose up -d` 후 `PYTHONPATH=src python scripts/battery.py`로 별도 수행 필요.

---

## 데이터 TODO (상세는 schedule-data.md §7)

- 2025.6 실제 시행일 확정 (매니페스트 잠정값)
- 2025.6 판 현행 HWP 재대조 (판정기준 문장부호)
- source_url 정밀화 (admRulSeq/flSeq)
- ADLs 보조표 정형화 (계산이 세부배점 파싱 시)

이 데이터 TODO들은 **지급률 값에는 영향 없음**(무변경 확인). 시행일 경계·인용 정밀도 과제.

---

## 권장 처리 순서

1. **버그 1(좌우) + 버그 2(백스톱)** — 사용자 돈에 직접 영향. 데이터가 이미 준비돼 있어
   원문 근거를 그대로 붙여 수정 가능. 회귀 테스트 필수.
2. **버그 3(외모)** — 프롬프트 한 줄. 함께 처리.
3. **버그 6(test_config)** — 테스트 격리. "상시 빨간불" 제거해 회귀 감지 복구.
4. **버그 7(ruff scope)** — CI 게이트 확보.
5. **버그 4(enrolled_at)·버그 5(파생장해)** — 제품 정책 결정 후 별도 이슈.
