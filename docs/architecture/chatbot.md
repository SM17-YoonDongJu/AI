# 12번 챗봇 — FastAPI WebSocket 직결·비스트리밍

> 출처: AI 엔진 아키텍처 문서 세트 · 최종 점검일 2026-07-15 · 브랜치 `11-feature-langgraph-멀티에이전트-구현`
> 상위: [README](./README.md) · 원본 코드 정독 + 적대적 교차검증(코드 재대조) 완료

## 🎯 한 문장 요약
사용자와 실시간 채팅으로 보험·법률 질문에 답하는 챗봇의 설계 문서이며, 배포 골격과 상세 스펙은 잡혀 있지만 **본체 코드는 아직 만들어지지 않은 "설계도" 단계**임을 정직하게 정리한 글이다.

## 🌱 쉽게 말하면
이 챗봇은 상담 창구 직원 같은 역할을 하려고 계획된 프로그램이다. 손님(사용자)이 채팅으로 질문하면, 관련 약관·판례를 찾아보고(RAG), 위험한 표현을 걸러낸 뒤(가드레일), AI가 답을 완성해서 한 번에 통째로 건네준다. 마치 편지를 조금씩 흘려보내지 않고 완성된 한 장을 봉투에 담아 전달하는 것과 같다(비스트리밍).

그런데 중요한 반전이 있다. 이 상담 창구는 **간판(Dockerfile)과 안내 규칙(스펙 문서)은 다 준비됐지만, 정작 안에서 일할 직원(`app.py` 실행 코드)은 아직 출근하지 않은 상태**다. 다만 직원이 쓸 도구들(검색·필터·AI 호출 모듈)은 옆 팀(리포트 워커)이 이미 만들어 놨기 때문에, 나중에 직원이 오면 그 도구들을 순서대로 쓰기만 하면 되는 "조립" 작업만 남았다.

> 쉽게 말하면: 재료(공용 모듈)는 다 있는데, 그 재료로 요리할 주방장(챗봇 본체)이 아직 없는 식당이다.

---

## 1. 현재 구현 상태 — **미구현(스펙 단계)**

`src/chatbot/` 디렉터리에는 **실행 코드가 존재하지 않는다.** 실제 파일 목록은 다음 3개(+`__pycache__`)뿐이다.

```
src/chatbot/Dockerfile
src/chatbot/README.md
src/chatbot/__init__.py
```

핵심 진입점으로 문서·Dockerfile이 참조하는 `app.py`(FastAPI 앱)는 **부재한다.** 즉 챗봇은 계약·배포 골격만 잡힌 **스펙 단계**이며, WebSocket(브라우저와 서버가 실시간 양방향으로 계속 이어진 대화 통로) 수락·세션·가드레일 조립 로직은 아직 한 줄도 구현되어 있지 않다.

> 쉽게 말하면: 문패는 붙었지만 문을 열면 방이 비어 있는 상태다.

### 1-1. `__init__.py` — 도크스트링 1줄뿐

파일 전체가 모듈 도크스트링(파일 맨 위에 붙이는 설명용 문자열) 한 줄이다(`src/chatbot/__init__.py:1`).

```python
"""Chatbot (12) — FastAPI WebSocket 직결(비스트리밍). 범용 노드."""
```

클래스·함수·라우터·`app` 객체 등 실질 코드는 전혀 없다.

### 1-2. `Dockerfile` — 진입점을 "구현 단계에서 추가"로 명시

배포 골격(프로그램을 서버에 담아 실행하기 위한 컨테이너 설계도)은 존재하나, 실제로 켜질 때 실행할 시작점(엔트리포인트)이 아직 없음을 주석으로 **정직하게 표기**하고 있다(`src/chatbot/Dockerfile:19-20`).

```dockerfile
# 엔트리포인트(chatbot/app.py:app)는 구현 단계에서 추가.
CMD ["uvicorn", "chatbot.app:app", "--host", "0.0.0.0", "--port", "8000"]
```

이 컨테이너를 지금 빌드·기동하면 `chatbot.app` 모듈이 없어 `uvicorn`(FastAPI 앱을 실제로 켜서 웹 요청을 받게 해 주는 실행기)이 임포트 에러로 즉시 실패한다(런타임 미검증).

Dockerfile 자체는 완성형이다. 정리하면 이렇다.

- 레포 루트를 빌드 컨텍스트로 `uv sync --no-dev --extra chatbot`(`Dockerfile:8`) — 챗봇에 필요한 패키지만 골라 설치한다.
- `python:3.12-slim` 2-스테이지 — 가벼운 파이썬 이미지를 2단계로 나눠 빌드한다.
- `PYTHONPATH=/app/src`(`Dockerfile:13`) — 파이썬이 소스 코드를 어디서 찾을지 알려준다.
- `EXPOSE 8000`(`Dockerfile:17`) — 8000번 포트를 바깥에 연다.
- GPU 불필요한 범용 노드용(`Dockerfile:1`) — 무거운 AI 계산은 다른 서버가 맡으므로, 이 컨테이너는 평범한 서버면 된다.

### 1-3. 계약 모델(`ChatClientMessage`/`ChatServerMessage`)도 **코드에 미정의**

README와 이벤트 명세는 WS(WebSocket) 입출력의 "코드 계약"(주고받을 메시지의 모양을 코드로 못 박아 둔 약속)으로 `core.contracts.ChatClientMessage` / `core.contracts.ChatServerMessage`를 지목한다(`src/chatbot/README.md:15-16`, `.claude/docs/chatbot-events.md:59,78`). 그러나 **실제 `src/core/contracts.py`에는 이 두 모델이 존재하지 않는다.** `__all__`(이 파일이 바깥에 공개하는 이름 목록)에도 없고(`src/core/contracts.py:19-31`), 파일 어디에도 `Chat`으로 시작하는 심볼이 없다.

```python
__all__ = [
    "OCR_JOB_TOPIC",
    "REPORT_JOB_TOPIC",
    "Chunk",
    "Citation",
    "ContentType",
    "DocType",
    "InputGuardResult",
    "OcrJob",
    "OutputGuardResult",
    "RagResult",
    "ReportJob",
]
```

정의된 계약은 OCR/리포트 Kafka 페이로드(`OcrJob`·`ReportJob`), RAG 결과(`Chunk`·`Citation`·`RagResult`), 가드레일 결과(`InputGuardResult`·`OutputGuardResult`)까지다. **챗봇 WS 메시지 계약은 문서상 약속만 존재하고 아직 코드화되지 않았다** — 구현 시 신설해야 할 항목이다.

> 쉽게 말하면: "이런 형식으로 주고받자"는 계약서를 문서에는 써 놨는데, 정작 코드 안에 그 계약서가 아직 안 들어가 있다.

### 1-4. 정리

| 항목 | 문서/스펙 | 실제 코드 |
|---|---|---|
| `app.py`(FastAPI 앱·`app` 객체) | Dockerfile CMD가 참조 | **없음** |
| `ChatClientMessage`/`ChatServerMessage` | README·events.md가 "코드 계약"으로 지목 | **`core/contracts.py`에 미정의** |
| WS 수락·JWT·세션·가드레일 조립 | 상세 스펙 존재 | **없음** |
| Dockerfile | 완성 | 존재(단, 진입점 없어 미기동) |
| `__init__.py` | — | 도크스트링 1줄 |
| 의존 공용 모듈(`rag`·`guardrail`·`ai_client`) | — | **구현되어 있음**(§5) |

> 표 읽는 법: 왼쪽은 "문서엔 이렇게 계획됐다", 오른쪽은 "실제 코드는 이렇다". 굵게 표시된 **없음/미정의**가 아직 안 만들어진 부분이다.

즉 **챗봇 본체는 미구현**이나, 그 조립 재료인 공용 모듈(RAG·가드레일·ai_client)은 이미 동작 코드로 존재한다. 챗봇 구현은 이들을 FastAPI WebSocket 핸들러에서 순서대로 호출하는 "조립" 작업이 된다.

---

## 2. 계획된 아키텍처 (문서 기준)

### 2-1. FastAPI WebSocket 직결

ALB(AWS 로드밸런서, 들어오는 요청을 여러 서버에 나눠 보내는 교통정리 장치)의 `/ws/chat` 경로 라우팅으로 **FastAPI가 WebSocket을 직접 수락**한다(Spring Boot 게이트웨이 미경유). LLM(대형 언어 모델, 답변 문장을 생성하는 AI)은 별도 GPU 노드를 HTTP로 호출하므로 챗봇 자체는 GPU가 필요 없는 **범용 노드**에 배포한다(`src/chatbot/README.md:3`, `.claude/docs/12_chatbot.md:6,20-22`, `.claude/docs/chatbot-events.md:6`).

> 쉽게 말하면: 무거운 AI 계산은 옆 건물(GPU 서버)에 전화로 맡기고, 챗봇 서버는 손님 응대와 심부름만 하니 값싼 일반 서버면 충분하다.

문서가 규정한 처리 파이프라인은 **입력 가드레일 → RAG 검색 → LLM 생성(완성) → 출력 가드레일**의 4단계다(`src/chatbot/README.md:9`, `.claude/docs/12_chatbot.md:30-36`).

`.claude/docs/12_chatbot.md:30-36`의 상세 순서:

1. 입력 가드레일 — PII(개인식별정보, 주민번호·전화번호처럼 개인을 특정하는 정보) 마스킹, 도메인 외 질문 차단
2. RAG 모듈(04번) — 쿼리에 적합한 namespace(검색 대상 묶음. 약관·판례·분쟁사례처럼 문서 종류별 구획) 선택, 약관·판례·분쟁사례 검색, 멀티턴 컨텍스트(앞선 여러 번의 대화 맥락) 구성
3. AI 라우터(`ai_client`) — RAG 결과와 대화 이력을 컨텍스트로 Ollama EXAONE 응답 생성 (완성까지 대기, 비스트리밍)
4. 출력 가드레일 — 단정표현 치환, 고지문 삽입
5. 완성된 응답을 WebSocket 메시지로 1회 전달

> 쉽게 말하면: (1) 질문에서 민감정보 지우고 엉뚱한 주제면 막고 → (2) 관련 자료 찾아오고 → (3) AI가 답 쓰고 → (4) 위험한 단정 표현을 부드럽게 바꾸고 안내문 붙여서 → (5) 완성본을 한 번에 건넨다.

### 2-2. 비스트리밍(완성 응답 1회)

문서의 최우선 전제는 **비스트리밍**(답을 토막토막 흘려보내지 않고 완성된 뒤 통째로 보냄)이다. LLM 완성 응답을 **1회** `message` 이벤트로 전달하며, 토큰 스트리밍이나 `stream`/`done` 청크가 **없다**(`.claude/docs/chatbot-events.md:7,90`, `src/chatbot/README.md:10`).

> `.claude/docs/chatbot-events.md:90`: "비스트리밍이므로 `stream`(토큰 청크)·`done`(완료 신호) 이벤트는 **없다**. 클라이언트는 요청 후 `message` 또는 `error`를 1회 기다린다(처리 중엔 "입력 중..." 인디케이터 표시 권장)."

이 비스트리밍 특성은 하부 `ai_client.chat()`이 애초에 `"stream": False`로 완성 응답만 받는 것과 정합적이다(`src/core/ai_client.py:86`, §5-3 참조).

> 쉽게 말하면: ChatGPT처럼 글자가 또르르 흘러나오는 방식이 아니라, 답이 다 완성될 때까지 기다렸다가 한 덩어리로 받는다. 그래서 클라이언트 화면엔 그 동안 "입력 중..." 표시를 띄워 두면 좋다.

### 2-3. 리포트 워커(Kafka)와의 차이

같은 "LLM 소비 컴포넌트"지만 통신·전달 방식이 대비된다.

| 구분 | 챗봇(12) | 리포트 워커(05) |
|---|---|---|
| 통신 방식 | FastAPI WebSocket **직결**(ALB `/ws/chat`) + REST(세션) | **Kafka** 토픽 소비/발행(`report-job`) |
| 트리거 | 사용자 WS 메시지(실시간) | `report-job` 메시지(비동기 잡) |
| 응답 전달 | WS로 **완성 응답 1회**(비스트리밍) | 리포트 산출물 DB 저장/후속 발행 |
| 실행 모델 | 온라인 요청-응답(대기형) | 오프라인 배치성 워커 |
| 오케스트레이션 | 단순 4단계 순차 조립(계획) | LangGraph 멀티에이전트 그래프 |
| LLM Judge | **미적용**(`run_judge=False`) | **적용**(`run_judge=True`, `agents.py:564`) |
| 배포 | 범용 노드, 8000 포트, `.[chatbot]` extra | Kafka 워커 |

> 표 읽는 법: 챗봇은 손님과 실시간으로 대화하는 "전화 상담"이고, 리포트 워커는 접수된 일감을 뒤에서 처리하는 "택배 배송"이라고 보면 된다. Kafka(작업을 큐에 쌓아 뒀다가 순서대로 꺼내 처리하는 메시지 우체통)는 리포트 쪽만 쓴다.

리포트 워커는 `report-job` 토픽을 Kafka로 소비하는 워커임에 반해(`REPORT_JOB_TOPIC = "report-job"`, `src/core/contracts.py:35`), 챗봇은 Kafka를 전혀 쓰지 않고 WebSocket·REST로 프런트와 직접 통신한다(`.claude/docs/12_chatbot.md:57-68`의 통신 표).

---

## 3. 계획된 상태/세션 관리

> 아래는 전부 **문서상 계획**이며, 대응 코드는 아직 없다(§1).

### 3-1. 세션 수명주기(REST + WS)

세션(한 사용자의 대화 한 묶음. 로그인처럼 "지금 누가 어디까지 대화했나"를 기억하는 단위)은 이렇게 흘러간다.

- **생성**: 프런트가 REST `POST /sessions`로 `session_id`와 `ws_url`을 발급받는다(`.claude/docs/chatbot-events.md:14-20`, `12_chatbot.md:46`).
- **연결**: `wss://<host>/ws/chat?session_id=<id>` + JWT(로그인 증표 역할을 하는 서명된 토큰)로 WS 수립. **on-connect 시 JWT(RS256) 스테이트리스 검증**(서버가 따로 저장해 둔 것 없이 토큰 서명만으로 진위 확인), 실패 시 거부(`4401`)(`chatbot-events.md:42-45`).
- **중복 연결**: 동일 `session_id`로 중복 연결 시 **기존 연결 해제 후 신규 수립**(종료 코드 `4409`)(`chatbot-events.md:45,101`, `12_chatbot.md:22`).
- **종료**: REST `DELETE /sessions/{session_id}` → `204`, Redis 세션 키·WS 연결 정리(`chatbot-events.md:22-23`).

> 쉽게 말하면: 먼저 상담 번호표(session_id)를 받고 → 그 번호표와 신분증(JWT)을 들고 통화를 연결하고 → 같은 번호로 또 걸면 먼저 통화는 끊기고 새 통화가 이어지며 → 상담이 끝나면 기록을 정리한다.

### 3-2. Redis — 다중 Pod 세션·컨텍스트 공유

FastAPI Pod(같은 챗봇을 복제해 여러 개 띄운 각각의 실행 단위)가 스케일아웃(요청이 많을 때 서버 대수를 늘림)돼도 세션 상태·멀티턴 컨텍스트를 **Redis(모든 Pod가 공유하는 초고속 메모리 저장소)로 공유**해, 재연결이 다른 Pod로 가도 컨텍스트를 복구한다. 세션 만료는 **24시간**이며 만료 시 Redis 키가 자동 삭제된다(`12_chatbot.md:22,24-26`, `chatbot-events.md:8,123`, `README.md:7`).

- 멀티턴 컨텍스트는 **이전 N턴 요약을 Redis에 캐시**해 PostgreSQL 조회를 최소화한다(`12_chatbot.md:26`, `chatbot-events.md:123`).
- Redis 통신 방식은 세션 상태 공유용 **Pub/Sub**(발행-구독. 한 곳에서 알림을 뿌리면 구독한 여러 Pod가 동시에 받는 방식)과 컨텍스트 캐시용 **GET/SET**(값을 넣고 꺼내는 단순 조회) 두 갈래로 규정된다(`12_chatbot.md:67-68`).

> 쉽게 말하면: 상담원이 여러 명(Pod)이어도 손님 정보를 공용 화이트보드(Redis)에 적어 두니, 다음에 다른 상담원이 받아도 앞 대화를 이어갈 수 있다. 그 메모는 24시간 지나면 저절로 지워진다.

### 3-3. PostgreSQL — 대화 이력(90일)

턴별 **질문·응답·인용 출처·타임스탬프**를 asyncpg(파이썬이 PostgreSQL과 비동기로 대화하게 해 주는 드라이버)로 저장한다. 보존 기간은 **90일**이며, 컨텍스트 윈도우(AI가 한 번에 볼 수 있는 대화 분량의 한계) 초과 시 **오래된 턴부터 요약·압축**한다(`12_chatbot.md:38-40`, `chatbot-events.md:36`, `README.md:11`). 이력은 REST `GET /sessions/{session_id}/history`로 조회한다(`chatbot-events.md:25-36`).

> 쉽게 말하면: Redis가 "지금 쓰는 임시 메모지"라면, PostgreSQL은 "90일간 보관하는 정식 상담 일지"다. 대화가 너무 길어지면 오래된 부분은 요약해서 자리를 아낀다.

### 3-4. 상태 전이 요약

문서 시퀀스(`chatbot-events.md:105-120`) 기준의 상태 흐름:

```
[세션 생성(REST)] → Redis SET 세션
  → [WS connect] → on-connect JWT 검증(실패=4401)
  → [message 수신] → 입력 가드레일 → RAG 검색 → LLM 생성(완성) → 출력 가드레일
  → [message 1회 응답(citations 포함)] → PG 이력 저장(90일)
  → (다음 메시지 반복)
  → [DELETE 세션] → Redis DEL, WS 정리
만료(24h)=4408 / 중복연결=4409 / 정상종료=1000
```

---

## 4. 이벤트 스키마 (`chatbot-events.md` 요약)

챗봇의 외부 인터페이스는 **REST(세션 관리) + WebSocket(실시간 메시지)** 이원 구성이다(`.claude/docs/chatbot-events.md:3`).

> 쉽게 말하면: 세션을 만들고 지우는 "사무 창구"는 REST로, 실제 대화를 주고받는 "통화선"은 WebSocket으로 나눠 쓴다.

### 4-1. REST 엔드포인트

| 메서드·경로 | 용도 | 요청 | 응답 |
|---|---|---|---|
| `POST /sessions` | 세션 생성 | `{}`(+선택 메타), `Authorization: Bearer <JWT(RS256)>` | `201` `{session_id, ws_url}` |
| `DELETE /sessions/{session_id}` | 세션 종료 | — | `204`(Redis 키·WS 정리) |
| `GET /sessions/{session_id}/history` | 이력 조회 | — | `200` `{session_id, turns[]}` |

세션 생성 응답 예(`chatbot-events.md:18-20`):

```json
{ "session_id": "s_9f3a...", "ws_url": "wss://<host>/ws/chat?session_id=s_9f3a..." }
```

이력 조회 응답의 `turns[]`는 `role`·`text`·`ts`를 갖고, assistant 턴은 `citations`를 포함한다(`chatbot-events.md:27-35`).

### 4-2. WebSocket `/ws/chat`

**연결(handshake)**: `wss://<host>/ws/chat?session_id=<id>` + JWT, on-connect RS256 검증(실패 `4401`), 동일 세션 중복 시 기존 해제 후 신규(`chatbot-events.md:42-45`).

**클라이언트 → 서버: `message`**(`chatbot-events.md:49-59`)

```json
{ "type": "message", "session_id": "s_9f3a...", "text": "교통사고 후유장해 보상 받을 수 있나요?" }
```

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| `type` | string | ✓ | `"message"` 고정 |
| `session_id` | string | ✓ | 세션 식별자 |
| `text` | string | ✓ | 사용자 입력(서버에서 PII 마스킹됨) |

→ 코드 계약 지목: `core.contracts.ChatClientMessage`(단, §1-3 — **미정의**).

**서버 → 클라이언트: `message`**(완성 응답, 비스트리밍 1회)(`chatbot-events.md:63-78`)

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

→ 코드 계약 지목: `core.contracts.ChatServerMessage`(§1-3 — **미정의**).

> 쉽게 말하면: `citations`는 답변이 어느 약관 조항·판례에서 나왔는지 출처를 함께 붙여 주는 각주 같은 것이다. AI가 근거 없이 지어내지 않았음을 보여 준다.

**서버 → 클라이언트: `error`**(`chatbot-events.md:80-88`)

```json
{ "type": "error", "code": "DOMAIN_BLOCKED", "message": "보험·법률 외 질문은 답변할 수 없습니다." }
```

| `code` | 의미 |
|---|---|
| `DOMAIN_BLOCKED` | 입력 가드레일 — 도메인 외 질문 차단 |
| `RATE_LIMITED` | 과도한 요청 |
| `INTERNAL` | LLM/RAG 등 내부 오류(안전 폴백 메시지 동반 가능) |

`stream`·`done` 이벤트는 **없다**(비스트리밍). 클라이언트는 `message` 또는 `error`를 1회 대기한다(`chatbot-events.md:90`).

### 4-3. WebSocket 종료(close) 코드(`chatbot-events.md:94-101`)

| 코드 | 의미 |
|---|---|
| `1000` | 정상 종료 |
| `4401` | 인증 실패(JWT 무효/만료) |
| `4408` | 세션 만료(24h) |
| `4409` | 동일 session_id 중복 연결로 기존 연결 해제 |

> 표 읽는 법: 통화가 끊길 때 서버가 남기는 "사유 코드"다. `1000`이면 정상, `44xx`대는 인증·만료·중복 같은 문제로 끊긴 경우다.

---

## 5. 리포트 워커와 공유하는 것 — 공용 모듈 함수 호출

챗봇의 통신 표(`.claude/docs/12_chatbot.md:64-65`)는 RAG·AI 라우터를 **"Python 함수 내부 호출"**·"Ollama HTTP"로 규정한다. Kafka가 아니라 **같은 프로세스 내 함수 호출**로 공용 모듈을 조립한다. 아래 3개 모듈은 **이미 구현되어 있어**, 챗봇은 이를 그대로 호출만 하면 된다.

> 쉽게 말하면: 멀리 있는 서비스에 네트워크로 요청을 보내는 게 아니라, 같은 프로그램 안에 있는 함수를 그냥 불러 쓰는 방식이다. 옆 방 사람에게 전화하는 게 아니라 바로 옆자리 동료에게 말 거는 셈이다.

### 5-1. `src/rag` — Hybrid RAG 검색

RAG는 "질문에 맞는 자료를 먼저 찾아와서, 그 자료를 근거로 AI가 답하게" 하는 방식이다. 공개 진입점은 `search()` 하나다(`src/rag/__init__.py:7-9`):

```python
from rag.search import RagError, search
__all__ = ["RagError", "search"]
```

시그니처(`src/rag/search.py:281-290`):

```python
async def search(
    query: str,
    insurance_type: str | None = None,
    namespaces: list[str] | None = None,
    top_k: int = DEFAULT_TOP_K,
    *,
    insurer: str | None = None,
    product: str | None = None,
    contract_date: datetime.date | None = None,
) -> RagResult:
```

- 반환은 `RagResult(ranked_chunks: list[Chunk], citations: list[Citation])`(`src/core/contracts.py:119-123`). 챗봇은 여기 `citations`를 서버 `message` 이벤트의 `citations` 필드로 그대로 실을 수 있다.
- 도크스트링은 이 모듈이 **`report_worker`·`chatbot`이 함수 호출로 공유**하는 04번 파이프라인임을 명시한다(`src/rag/__init__.py:1`). 범위 밖(비신체보험) 쿼리는 **빈 결과**로 반환된다(`src/rag/search.py:305,308-310`).
- 챗봇 구현 시 `namespaces=None`이면 라우터가 namespace(terms·case)를 결정한다(`search.py:284,307`).

> 쉽게 말하면: `search()`에 질문을 넣으면 관련 약관·판례를 찾아 "근거 문단(chunks)"과 "출처 목록(citations)"을 돌려준다. 챗봇은 그 출처 목록을 답변에 그대로 붙이면 된다. namespace를 안 정해 주면 알아서 어느 자료함을 뒤질지 골라 준다.

### 5-2. `src/guardrail` — 3단계 가드레일 (챗봇 차이: `run_judge=False`)

가드레일은 안전장치다. `guardrail/guards.py`가 입력/생성/출력 3단계를 모두 제공한다. 결과 모델은 `core.contracts`의 `InputGuardResult`/`OutputGuardResult`가 단일 출처다(`src/guardrail/guards.py:7,14`).

**입력 가드레일** — PII 마스킹 + 도메인 외 차단(`guards.py:36-43`):

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

- PII 마스킹은 정규식(문자 패턴을 규칙으로 찾아내는 방식)으로 주민번호(앞 6자리 보존)·전화번호·계좌번호를 치환한다(`guards.py:26-33`). 이 규칙은 `ocr_worker` 입력단과 **동일해야** 한다(어긋나면 한쪽 PII 유출)(`guards.py:8`).
- 도메인 차단 키워드는 간이 목록 `("부동산","주식","코인","비트코인","연애","요리","게임")`(`guards.py:22`). 챗봇은 `blocked=True` 시 `error` 이벤트 `DOMAIN_BLOCKED`로 매핑하게 된다(§4-2).

> 쉽게 말하면: 질문이 들어오면 (1) 주민번호·전화번호 같은 개인정보를 가려 버리고, (2) 보험·법률과 무관한 주제(주식·연애 등)면 아예 막는다. 막힌 질문은 사용자에게 `DOMAIN_BLOCKED` 오류로 안내된다.

**생성 가드레일** — 단정 금액표현 → "참고 추정 범위" 치환(`guards.py:53-57`, 동기 함수). 서버 `message`의 `text`가 "참고 추정 범위 ..."로 나가는 것과 정합적이다.

> 쉽게 말하면: "무조건 500만 원 받습니다" 같은 단언을 "참고 추정 범위: ..."처럼 조심스러운 표현으로 바꿔, 확정처럼 들리지 않게 한다.

**출력 가드레일** — 고지문 삽입 + (선택) LLM Judge(`guards.py:61-69`):

```python
async def guard_output(
    text: str, *, run_judge: bool = True, chunks: list | None = None
) -> OutputGuardResult:
    final = text or ""
    if DISCLAIMER not in final:
        final = f"> {DISCLAIMER}\n\n{final}"

    judge_failures: list[str] = []
    if run_judge and chunks:
        ...
```

여기가 **챗봇 ↔ 리포트의 핵심 차이**다. `guard_output`의 기본값은 `run_judge=True`이지만:

- **리포트 워커**는 `run_judge=True`로 LLM Judge(AI가 답변의 인용이 근거와 맞는지 다시 한 번 검사하는 심판 역할) 인용 검증을 수행한다(`src/report_worker/nodes/agents.py:564`):
  ```python
  state.get("report", ""), run_judge=True, chunks=state.get("retrieved_clauses", [])
  ```
- **챗봇**은 문서상 `run_judge=False`로 호출해 **LLM Judge를 건너뛴다.** LLM Judge는 리포트 전용이며, 챗봇 출력 가드레일은 **고지문 삽입·단정표현 치환만** 수행한다(`.claude/docs/chatbot-events.md:124`, `.claude/docs/contracts.md:189`). 이유는 온라인 대화의 지연을 낮추기 위함으로, `run_judge=False`면 추가 `ai_client.chat_json` 왕복이 없다.

> 쉽게 말하면: 리포트는 시간이 좀 걸려도 되니 AI 심판(LLM Judge)에게 한 번 더 검수받지만, 챗봇은 실시간 대화라 빨라야 하므로 그 검수 단계를 생략한다. 대신 안내문 붙이기와 단정 표현 바꾸기는 챗봇도 그대로 한다.

즉 챗봇 구현 시 출력 단계는 `await guard_output(text, run_judge=False)`로 호출하게 되며(기본값 True를 명시적으로 False로 덮어써야 함), `judge_failures`는 항상 빈 리스트가 된다.

> 참고: 테스트 `tests/test_guardrail.py:40`이 `guard_output("리포트 본문", run_judge=False, chunks=None)` 경로를 검증하고 있어, 챗봇이 쓸 `run_judge=False` 분기는 이미 테스트 커버되어 있다.

### 5-3. `src/core/ai_client` — LLM 라우터(비스트리밍)

Ollama EXAONE(별도 GPU 노드, `OLLAMA_BASE_URL` HTTP)를 호출하는 공용 클라이언트(`.claude/docs/12_chatbot.md:8`, `README.md:20`). Ollama는 언어 모델을 서버로 띄워 HTTP로 부르게 해 주는 실행 환경이고, EXAONE은 그 위에서 돌리는 언어 모델 계열이다. 공개 코루틴(비동기로 호출되는 함수)은 `chat`·`chat_json`·`embed`·`close_client`(`src/core/ai_client.py:59,69,101,135`).

챗봇의 LLM 생성 단계가 쓸 `chat()`은 **처음부터 비스트리밍**이다(`src/core/ai_client.py:82-94`):

```python
model = opts.pop("model", None) or settings.llm_model
payload: dict[str, Any] = {
    "model": model,
    "messages": messages,
    "stream": False,
    **opts,
}
client = _get_chat_client()
try:
    resp = await client.post("/chat/completions", json=payload)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]
```

- `"stream": False`(`ai_client.py:86`)라 완성 텍스트를 1회 반환 — 챗봇의 "완성 응답 1회" 전제와 그대로 맞는다.
- HTTP/형식 오류 시 `AiClientError`를 던지므로(`ai_client.py:95-98`), 챗봇은 이를 잡아 `error` 이벤트 `INTERNAL`로 매핑하게 된다(§4-2).
- **모델은 미정**(`settings.llm_model`)이다. 코드상 `llm_model` 기본값이 빈 문자열(`src/core/config.py:61`, `llm_model: str = ""  # 예: EXAONE 계열`)이라 env 주입 전에는 미정이다. "모델 미정" 표기는 `src/chatbot/README.md:20`·`.claude/docs/contracts.md:194`에 있다(참고로 `12_chatbot.md:8`은 AI 라우터를 "Ollama EXAONE"로 명시하므로 '미정' 근거로는 부적절).
- `chat_json`은 리포트·챗봇·가드레일이 공유하는 JSON 모드(답을 정해진 JSON 형식으로 받는 방식) 헬퍼(`ai_client.py:101-106`)지만, 챗봇 본류 응답은 자유형 텍스트이므로 `chat()`을 쓰게 된다.

> 쉽게 말하면: `chat()`은 "AI에게 대화를 던지고 완성된 답 한 덩어리를 받아 오는" 창구다. 어떤 모델을 쓸지는 아직 비워 뒀고(환경변수로 나중에 주입), 문제가 생기면 오류를 던져 챗봇이 `INTERNAL` 오류로 안내하게 된다.

### 5-4. 공유 계약 모델

RAG·가드레일 결과 모델(`Chunk`·`Citation`·`RagResult`·`InputGuardResult`·`OutputGuardResult`)은 `core.contracts`가 단일 출처로, 리포트·챗봇이 동일 스키마를 공유한다(`src/core/contracts.py:92-143`). 단 **챗봇 고유의 WS 메시지 계약(`ChatClientMessage`/`ChatServerMessage`)만 아직 이 파일에 추가되지 않았다**(§1-3) — 챗봇 구현의 첫 작업이 이 계약 신설이 되어야 한다.

> 쉽게 말하면: 데이터의 "모양 약속서"는 한 파일(`core.contracts`)에 모아 두고 리포트·챗봇이 같이 쓴다. 다만 챗봇 전용 메시지 약속 두 개만 아직 안 넣었으니, 그걸 넣는 게 챗봇 구현의 첫 단추다.

---

### 종합 정직성 노트

- **챗봇 본체(app.py, WS 핸들러, 세션·JWT·Redis·PG 연동)는 전부 미구현**이다. 존재하는 건 도크스트링 1줄(`__init__.py`), Dockerfile, README뿐이다.
- **WS 메시지 계약 모델 2종은 문서가 "코드 계약"이라 부르지만 실제 코드에 없다.** 문서-코드 불일치로 기록해 둔다.
- 반면 **조립 재료인 RAG·가드레일·ai_client 3개 공용 모듈은 구현·테스트되어 있어**, 챗봇 구현은 이들을 FastAPI WebSocket 위에서 4단계 순차 호출로 엮고, `guard_output`을 `run_judge=False`로 부르는 조립 작업으로 수렴한다.
