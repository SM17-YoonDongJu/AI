## 하네스: 보험·법률 AI Python 엔진

**목표:** 보험·법률 문서 AI 처리 Python 엔진(OCR·Hybrid RAG·LangGraph 리포트·가드레일·챗봇)을 모듈별 전문 에이전트 팀으로 구축·유지보수한다.

**트리거:** OCR Worker·RAG·LangGraph 리포트·가드레일·챗봇 등 이 프로젝트 기능의 구현/수정/리팩터링 요청 시 `ai-engine-orchestrator` 스킬을 사용하라. 단순 질문이나 단일 파일 수정은 직접 응답 가능.

**아키텍처 요약 (확정):**
- OCR(02)·리포트(05) = Kafka 워커 — `src/ocr_worker` · `src/report_worker`
- 챗봇(12) = FastAPI WebSocket 직결, **비스트리밍**(완성 응답 1회) — `src/chatbot/app.py`
- RAG(04)·가드레일(06)·`ai_client` = 공용 모듈 — `src/rag` · `src/guardrail` · `src/core/ai_client.py`
- Spring Boot는 게이트웨이(업로드/JWT/S3/Kafka 발행) — **별도 범위**
- 노드 간 통신: OCR/리포트=Kafka, 챗봇=FastAPI WS 직결

**코드 컨벤션:** 모든 Python 코드는 `.claude/CODE_CONVENTIONS.md`를 따른다.

**변경 이력:**
| 날짜 | 변경 내용 | 대상 | 사유 |
|------|----------|------|------|
| 2026-06-15 | 초기 구성 (5 에이전트 + 6 스킬 + 오케스트레이터) | 전체 | - |
| 2026-06-15 | PR 작성 스킬 + 코드 컨벤션 문서 추가 | `skills/pr-writer`, `CODE_CONVENTIONS.md` | 사용자 요청 |
| 2026-06-15 | pr-writer를 백엔드용으로 수정 (API/메시지 계약·DB·동시성·보안·운영 강조 추가) | `skills/pr-writer` | 프론트 레포 원본 → 백엔드 적합화 |
| 2026-06-16 | docs-notion-sync 스킬 + SessionStart(startup) 훅 추가 | `skills/docs-notion-sync`, `settings.local.json` | 세션 시작 시 .claude/docs를 Notion과 동기화(Notion→docs) |
| 2026-06-16 | 코드 컨벤션 문서를 `.claude/` 하위로 이동, 참조 7곳 경로 갱신 | `.claude/CODE_CONVENTIONS.md`, agents·skills | 설정·문서를 .claude로 일원화 |
| 2026-06-16 | 공유 설정(훅·Notion 권한)을 `settings.json`으로 분리, `.gitignore` 작성 | `.claude/settings.json`, `.gitignore` | .claude 커밋 — 훅은 공유, local.json은 개인용 제외 |
| 2026-06-17 | 워커중심 구조로 변경 + 하네스 경로 동기화 | agents·skills·CLAUDE.md | 실제 구조(`src/ocr_worker`·`src/report_worker`·`src/chatbot/app.py`)와 정합 |
| 2026-06-25 | git-committer 스킬 추가 (변경 분석→한국어 커밋, ruff·시크릿 점검, push 안 함) | `skills/git-committer` | 사용자 요청 — 커밋 메시지 작성·커밋 자동화 |
| 2026-08-03 | 커밋·PR 스킬에서 AI 서명/트레일러 제거 (`Co-Authored-By`·`Generated with`) | `skills/git-committer`, `skills/pr-writer` | 사용자 요청 — 커밋/PR에 도구 흔적 미표기 |
