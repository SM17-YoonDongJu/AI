---
name: ai-engine-orchestrator
description: 보험·법률 AI Python 엔진(OCR Worker·Hybrid RAG·LangGraph 리포트·가드레일·챗봇)의 구축·확장·수정을 조율하는 오케스트레이터. 에이전트 팀(platform/ocr/aicore/agent/qa)을 구성해 파운데이션→팬아웃→점진 QA로 진행한다. 이 프로젝트 기능 구현/수정/리팩터링, "다시 실행/재실행/업데이트/보완", "OCR/RAG/리포트/가드레일/챗봇 부분만" 등의 요청 시 사용.
---

# AI Engine Orchestrator

보험·법률 AI Python 엔진을 모듈별 전문 에이전트 팀으로 구축·유지보수한다. 실행 모드는 **에이전트 팀**(기본). 전 에이전트 `model: "opus"`로 호출한다.

## Phase 0: 컨텍스트 확인
1. `_workspace/` 존재 여부와 `.claude/agents/`·`src/` 현황을 본다.
2. 분기:
   - `_workspace/` 없음 → **초기 구축** (Phase 1 전체)
   - 있음 + 부분 수정 요청("OCR만 다시" 등) → **부분 재실행** (해당 에이전트만 재호출)
   - 있음 + 새 입력/대규모 변경 → **새 실행** (기존 `_workspace/`를 `_workspace_prev/`로 이동 후 진행)
3. `CLAUDE.md`의 아키텍처 요약과 `.claude/CODE_CONVENTIONS.md`를 로드해 팀에 공유한다.

## Phase 1: 팀 구성
`TeamCreate`로 다음 5인 팀을 만든다:
- `platform-engineer` — 토대(core·스캐폴딩·contracts·docker-compose·마이그레이션)
- `ocr-engineer` — OCR Worker(02)
- `aicore-engineer` — RAG(04) + 가드레일(06) 공용 모듈
- `agent-engineer` — 리포트(05) + 챗봇(12)
- `qa-engineer` — 경계면 교차 검증 (general-purpose 타입)

> 부분 재실행이면 해당 에이전트만 활성화한다.

## Phase 2: 작업 흐름 (파운데이션 → 팬아웃 → 점진 QA)
`TaskCreate`로 의존성을 명시해 할당한다:

1. **파운데이션 (선행, 차단)**: `platform-engineer`가 `contracts.py`·core·스캐폴딩을 먼저 완성. 완료 즉시 다른 에이전트에 SendMessage로 계약·import 경로 공지 → 차단 해소.
2. **팬아웃 (병렬)**:
   - `ocr-engineer` → OCR Worker (contracts·DB 의존)
   - `aicore-engineer` → RAG + 가드레일 (core·ai_client 의존). 공개 API 확정 즉시 `agent-engineer`에 공지.
3. **조립 (aicore 의존)**: `agent-engineer` → 리포트·챗봇. (05 그래프 설계는 먼저 `_workspace/`에 올려 확인받고 구현)
4. **점진 QA**: 각 모듈 완성 직후 `qa-engineer`가 해당 경계면을 즉시 교차 검증. 버그는 책임 에이전트에 반려·재검증.

## Phase 3: 종합
팀 산출물을 모아 최종 상태를 요약한다. QA 보고서(`_workspace/99_qa_report.md`)의 미해결 항목을 명시한다. 팀을 정리한다.

## 데이터 전달 프로토콜
- **태스크 기반**(조율): `TaskCreate`/`TaskUpdate`로 의존성·진행 추적.
- **메시지 기반**(실시간): `SendMessage`로 계약 공지·버그 반려·조율.
- **파일 기반**(산출물): 코드는 `src/`, 중간 산출물은 `_workspace/{NN}_{agent}_*.md`. 최종 코드만 정식 경로, `_workspace/`는 감사용 보존.

## 에러 핸들링
- 에이전트 작업 1회 재시도 후 재실패 → 해당 결과 없이 진행하고 최종 보고에 누락 명시.
- 계약 충돌 시 상충 정의를 삭제하지 않고 출처 병기, 책임 에이전트가 조정.
- 외부 의존(Ollama·GPU) 미가용으로 런타임 검증 불가 → 구조·정적 검증으로 대체하고 한계 명시.

## 테스트 시나리오
- **정상 흐름**: 초기 구축 요청 → 팀 생성 → platform이 contracts 확정·공지 → ocr/aicore 병렬 → agent 조립 → 각 단계 QA PASS → 종합 보고.
- **에러 흐름**: aicore RAG API가 agent 호출부와 시그니처 불일치 → QA가 FAIL 보고 → aicore에 반려 → 수정 → QA 재검증 PASS → 진행. (한쪽이 1회 재시도 후에도 실패하면 누락 표기 후 나머지 진행)
