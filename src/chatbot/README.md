# chatbot — 상담 챗봇 (12)

FastAPI가 **WebSocket을 직접 수락**(ALB `/ws/chat`)하는 챗봇. LLM은 외부 GPU 노드를 HTTP 호출하므로 **범용 노드**에 배포한다. **비스트리밍**(완성 응답 1회 전달)이다.

## 처리 흐름

1. WebSocket 연결 수락 — **on-connect JWT(RS256) 검증**, 동일 session_id 중복 연결 시 기존 해제
2. **Redis**로 다중 Pod 세션 상태·멀티턴 컨텍스트 공유(24h 만료)
3. 메시지 수신 → **입력 가드레일 → RAG 검색 → LLM 생성(완성) → 출력 가드레일**
4. **완성 응답을 `ChatServerMessage`로 1회 전달**(citations 포함). 토큰 스트리밍·`stream`/`done` 없음
5. 세션 생성·종료는 REST, 대화 이력 PG 저장(90일)

## 입력 / 출력 (계약)

- **입력**: `core.contracts.ChatClientMessage` (WS 수신)
- **출력**: `core.contracts.ChatServerMessage` (WS 송신, 비스트리밍)

## 의존 / 배포

- `core`·`rag`·`guardrail`·`core.ai_client`(LLM, **모델 미정**) · Redis · RDS
- `.[chatbot]` extra (fastapi·uvicorn·websockets·python-jose)
- 진입점: `chatbot.app:app` (`uvicorn`) · 배포: `src/chatbot/Dockerfile`(slim) → 범용 노드, 8000 포트
- WebSocket·세션·JWT는 `app.py`에, 순수 처리 로직은 별도 모듈로 분리(테스트 용이)

## 참고

- [Notion 12 챗봇](../../.claude/docs/12_chatbot.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
