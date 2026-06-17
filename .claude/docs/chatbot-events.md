# 챗봇 이벤트 명세서 (12)

`chatbot` 워커의 외부 인터페이스 정의 — **REST(세션 관리)** + **WebSocket(실시간 메시지)**. Frontend·`chatbot` 담당자가 이 명세에 맞춰 병렬 개발한다.

핵심 전제:
- FastAPI가 ALB(`/ws/chat`)를 통해 **WebSocket을 직접 수락**(Spring 미경유).
- **비스트리밍** — LLM 완성 응답을 **1회** 전달한다. 토큰 스트리밍·`stream`/`done` 청크 없음.
- 다중 Pod 환경 → 세션 상태·멀티턴 컨텍스트는 **Redis 공유**(24h 만료).

---

## 1. REST 엔드포인트

### `POST /sessions` — 세션 생성
- 인증: `Authorization: Bearer <JWT(RS256)>`
- 요청: `{}` (필요 시 메타)
- 응답 `201`:
```json
{ "session_id": "s_9f3a...", "ws_url": "wss://<host>/ws/chat?session_id=s_9f3a..." }
```

### `DELETE /sessions/{session_id}` — 세션 종료
- 응답 `204`. Redis 세션 키·WS 연결 정리.

### `GET /sessions/{session_id}/history` — 대화 이력 조회
- 응답 `200`:
```json
{
  "session_id": "s_9f3a...",
  "turns": [
    {"role": "user", "text": "...", "ts": "2026-06-17T05:40:00Z"},
    {"role": "assistant", "text": "...", "citations": ["[제3조]"], "ts": "2026-06-17T05:40:08Z"}
  ]
}
```
- 이력 보존 **90일**. 컨텍스트 윈도우 초과 시 오래된 턴부터 요약·압축.

---

## 2. WebSocket `/ws/chat`

### 연결(handshake)
- `wss://<host>/ws/chat?session_id=<id>` + JWT.
- **on-connect 시 JWT(RS256) 스테이트리스 검증** → 실패 시 거부(`4401`).
- 동일 `session_id` 중복 연결 시 **기존 연결 해제 후 신규 수립**.

### 2-1. 클라이언트 → 서버 이벤트

#### `message` — 사용자 메시지 전송
```json
{ "type": "message", "session_id": "s_9f3a...", "text": "교통사고 후유장해 보상 받을 수 있나요?" }
```
| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| `type` | string | ✓ | `"message"` 고정 |
| `session_id` | string | ✓ | 세션 식별자 |
| `text` | string | ✓ | 사용자 입력(서버에서 PII 마스킹됨) |

> (코드 계약: `core.contracts.ChatClientMessage`)

### 2-2. 서버 → 클라이언트 이벤트

#### `message` — 완성 응답 (비스트리밍, 1회)
처리 순서: **입력 가드레일 → RAG 검색 → LLM 생성(완성) → 출력 가드레일** 후 한 번에 전달.
```json
{
  "type": "message",
  "text": "참고 추정 범위: 후유장해 등급에 따라 ... [제3조][2021다1234]",
  "citations": ["[제3조]", "[2021다1234]"]
}
```
| 필드 | 타입 | 설명 |
|---|---|---|
| `type` | string | `"message"` 고정 |
| `text` | string | 완성된 응답(고지문·단정표현 치환 적용됨) |
| `citations` | string[] | 인용 근거(조항·판례 번호) |

> (코드 계약: `core.contracts.ChatServerMessage`)

#### `error` — 오류
```json
{ "type": "error", "code": "DOMAIN_BLOCKED", "message": "보험·법률 외 질문은 답변할 수 없습니다." }
```
| `code` | 의미 |
|---|---|
| `DOMAIN_BLOCKED` | 입력 가드레일 — 도메인 외 질문 차단 |
| `RATE_LIMITED` | 과도한 요청 |
| `INTERNAL` | LLM/RAG 등 내부 오류(안전 폴백 메시지 동반 가능) |

> 비스트리밍이므로 `stream`(토큰 청크)·`done`(완료 신호) 이벤트는 **없다**. 클라이언트는 요청 후 `message` 또는 `error`를 1회 기다린다(처리 중엔 "입력 중..." 인디케이터 표시 권장).

---

## 3. 종료(close) 코드

| 코드 | 의미 |
|---|---|
| `1000` | 정상 종료 |
| `4401` | 인증 실패(JWT 무효/만료) |
| `4408` | 세션 만료(24h) |
| `4409` | 동일 session_id 중복 연결로 기존 연결 해제 |

---

## 4. 시퀀스 (lifecycle)

```
Frontend                         chatbot (FastAPI)              Redis / RAG / ai_client
   │  POST /sessions ──────────────▶ JWT 검증, 세션 생성 ───────▶ Redis SET 세션
   │  ◀── 201 {session_id, ws_url}
   │  WS connect /ws/chat?session_id ▶ on-connect JWT 검증
   │  ── message{text} ────────────▶ 입력 가드레일(PII·도메인)
   │                                  └ RAG 검색 ───────────────▶ pgvector/tsvector
   │                                  └ LLM 생성(완성) ─────────▶ ai_client(EXAONE, HTTP)
   │                                  └ 출력 가드레일(고지문)
   │  ◀── message{text, citations}    (1회 전달, 비스트리밍)
   │                                  └ 이력 저장 ──────────────▶ PG(90일)
   │  ── (다음 메시지 반복) ...
   │  DELETE /sessions/{id} ────────▶ 세션·WS 정리 ─────────────▶ Redis DEL
```

## 5. 멀티턴·세션 노트
- 컨텍스트: 이전 N턴 요약을 **Redis 캐시**로 구성(PG 조회 최소화). 재연결이 다른 Pod로 가도 Redis로 컨텍스트 복구.
- LLM Judge는 **챗봇에 미적용**(리포트 전용). 챗봇 출력 가드레일은 고지문 삽입·단정표현 치환만.

---

> 상위 계약: [contracts.md](contracts.md) · 아키텍처: [.claude/docs/12_chatbot.md](12_chatbot.md)
