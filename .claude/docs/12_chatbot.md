## notion 링크: https://app.notion.com/p/12-37530798f08f81e8af55c6c27fd144f0

## 참여 컴포넌트

- **Frontend** (React Native / Next.js): WebSocket 연결, 메시지 송수신 UI
- **FastAPI**: WebSocket 연결 수락·세션 관리·JWT 검증(on-connect), 세션 생성·종료(REST), RAG 검색, LLM 추론(완성 응답 일괄 반환·비스트리밍), 가드레일 적용
- **RAG 모듈** (04번): 약관·판례·분쟁사례 검색
- **AI 라우터 (`ai_client`)**: Ollama EXAONE(**별도 GPU 노드**, `OLLAMA_BASE_URL` HTTP),
- **AWS RDS (PostgreSQL + pgvector)**: 세션·대화 이력 저장 및 RAG 벡터 검색(임베딩 1024차원 고정), asyncpg 접근
- **ElastiCache Redis**: WebSocket 세션 상태 공유 (다중 Pod 간 동기화), 챗봇 대화 컨텍스트 캐시

---

## 소프트웨어 레이어 구조

**[Frontend — 세션 생성]**

REST로 FastAPI의 를 호출하여 session_id와 WebSocket URL을 발급받는다.

**[FastAPI — WebSocket 연결]**

ALB를 통해 FastAPI가 WebSocket 연결을 직접 수락한다. on-connect 시 JWT(RS256)를 스테이트리스 검증하여 인증되지 않은 연결을 거부한다. 동일 session_id에 중복 연결 시 기존 연결을 해제하고 신규 연결을 수립한다. 수신 메시지는 FastAPI가 직접 처리한다. FastAPI Pod가 여러 인스턴스로 스케일아웃된 경우 Redis로 WebSocket 세션 상태를 공유하여, 재연결이 다른 Pod로 가도 대화 컨텍스트를 복구한다.

**[ElastiCache Redis — 세션·컨텍스트 캐시]**

WebSocket 세션 상태를 Redis에 저장하여 다중 FastAPI Pod 환경에서도 동일한 세션을 유지한다. FastAPI가 멀티턴 대화 컨텍스트(이전 N턴 요약)를 Redis에 캐시하여 PostgreSQL 조회 없이 빠르게 컨텍스트를 구성한다. 세션 만료(24시간) 시 Redis 키도 자동 삭제된다.

**[FastAPI — 챗봇 처리]**

WebSocket으로 사용자 메시지를 수신하면 다음 순서로 처리한다.

1. 입력 가드레일 — PII 마스킹, 도메인 외 질문 차단
2. RAG 모듈(04번) — 쿼리에 적합한 namespace 선택, 약관·판례·분쟁사례 검색, 멀티턴 컨텍스트 구성
3. AI 라우터(`ai_client`) — RAG 검색 결과와 대화 이력을 컨텍스트로 Ollama EXAONE 응답 생성 (완성까지 대기, 비스트리밍)
4. 출력 가드레일 — 단정표현 치환, 고지문 삽입
5. 완성된 응답을 WebSocket 메시지로 1회 전달

**[AWS RDS — 대화 이력]**

턴별 질문·응답·인용 출처·타임스탬프를 저장한다. 이력 보존 기간은 90일이며 컨텍스트 윈도우 초과 시 오래된 턴부터 요약·압축한다.

---

## 데이터 흐름 (순서)

1. Frontend가 REST로 FastAPI에 세션 생성 → session_id와 ws_url 수신
2. Frontend가 FastAPI와 WebSocket 연결 직접 수립 (on-connect JWT 검증)
3. Frontend가 메시지 전송
4. FastAPI가 WebSocket으로 메시지를 직접 수신 (Spring Boot 미경유)
5. FastAPI가 입력 가드레일 → RAG 검색 → LLM 생성(완성) → 출력 가드레일
6. FastAPI가 완성된 응답을 `message` 메시지로 WebSocket 1회 반환 (citations 포함)
7. (스트리밍 미사용 — 별도 `done` 신호 불필요)
8. PostgreSQL에 대화 이력 저장

---

## 컴포넌트 간 통신 방식

| 구간 | 방식 |
| --- | --- |
| Frontend → FastAPI | REST POST (세션 생성·종료), REST GET (이력 조회) |
| Frontend ↔ FastAPI | WebSocket (메시지 송수신, 비스트리밍·완성 응답 1회) |
| Frontend ⇒ FastAPI (WS 직결) | ALB path 라우팅 (/ws/chat → FastAPI 직접 연결) |
| FastAPI → RAG 모듈 | Python 함수 내부 호출 |
| FastAPI → AI 라우터(ai_client) | Ollama HTTP (별도 GPU 노드, EXAONE) |
| FastAPI → AWS RDS (PostgreSQL) | asyncpg (대화 이력 저장) |
| FastAPI → Redis (세션 공유) | Pub/Sub (WebSocket 세션 상태 공유) |
| FastAPI → Redis | GET/SET (대화 컨텍스트 캐시) |

!12_법률_보험_상담_챗봇.png
