# Python 코드 컨벤션

본 프로젝트의 모든 Python 코드가 따르는 규칙이다. 최신 표준(PEP 8/484/585/604/695, Ruff 생태계, pydantic v2, async-first)을 기준으로 한다. 각 규칙은 "왜"를 함께 적는다 — 이유를 알면 엣지 케이스에서도 올바르게 판단할 수 있기 때문이다.

## 1. 런타임·프로젝트 구조
- **Python 3.12**, `src/` 레이아웃 사용. 패키지 코드는 전부 `src/` 아래에 둔다. — 테스트가 설치된 패키지를 import 하게 되어, "로컬에선 되는데 배포하면 안 되는" 문제를 예방한다.
- 패키지/의존성은 **uv**로 관리한다. `pyproject.toml`만 사용하고 `setup.py`는 만들지 않는다. `uv.lock`은 커밋한다. — 재현 가능한 빌드를 보장한다.
- 설정은 표준 `[project]` + `[tool.*]` 테이블로 `pyproject.toml`에 모은다.

## 2. 포맷팅·린팅 — Ruff 단일화
- 포맷팅·import 정렬·린팅을 모두 **Ruff**로 한다 (black·isort·flake8를 대체). — 도구 하나로 통일하면 설정 충돌과 CI 시간이 준다.
- 라인 길이 **100**. 들여쓰기 4칸 스페이스.
- 커밋 전 `ruff format` + `ruff check --fix`를 통과해야 한다.

## 3. 타입 힌트 (필수)
- 모든 **공개 함수·메서드의 시그니처에 타입 힌트**를 단다. 내부 헬퍼도 권장. — 경계면 버그를 정적으로 잡고, 에디터 자동완성과 리뷰 속도를 높인다.
- **빌트인 제네릭** 사용: `list[str]`, `dict[str, int]` (PEP 585). `typing.List` 등 구식 표기 금지.
- **유니온은 `X | None`** (PEP 604). `Optional[X]` 대신 `X | None`.
- 타입 별칭이 필요하면 **`type` 문**(PEP 695)을 쓴다: `type JobId = str`.
- `Any`는 최후수단. 외부 미지정 데이터 경계에서만 제한적으로.

## 4. 네이밍
- 함수·변수·모듈: `snake_case`. 클래스: `PascalCase`. 상수: `UPPER_SNAKE`. — PEP 8 표준.
- 약어도 클래스명에선 첫 글자만 대문자처럼 취급할 수 있으나, 일관성을 우선한다 (`OcrWorker` 또는 `OCRWorker` 중 하나로 통일).
- 의미 없는 1글자 변수 금지(루프 인덱스 `i`, 컴프리헨션 제외). 이름이 의도를 설명해야 한다.

## 5. Import
- **절대 import**만 사용. 상대 import(`from ..foo`) 금지. — 모듈 이동 시 깨지기 쉽고 가독성이 낮다.
- 와일드카드 import(`from x import *`) 금지.
- 순서는 Ruff(isort 규칙)가 정렬: 표준 라이브러리 → 서드파티 → 로컬.

## 6. 데이터 모델
- **외부 경계**(Kafka 메시지, WebSocket 메시지, API I/O)는 **pydantic v2 `BaseModel`**로 정의·검증한다. — 잘못된 페이로드를 진입점에서 거른다. 이 프로젝트는 Spring과의 계약(`core/contracts.py`)이 핵심이므로 필수.
- **내부 순수 데이터**는 `@dataclass(slots=True)` 또는 pydantic 중 맥락에 맞게. 가변/검증 불필요하면 dataclass가 가볍다.
- pydantic v2 API 사용(`model_validate`, `model_dump`). v1 메서드(`.dict()`, `.parse_obj()`) 금지.

## 7. 비동기 (async-first)
- I/O 경로는 **async/await**로 작성한다 (`aiokafka`, `asyncpg`, FastAPI). — 워커·웹소켓이 다수 연결을 효율적으로 처리한다.
- **async 함수 안에서 블로킹 호출 금지**(동기 `requests`, 동기 DB 드라이버, `time.sleep`). 블로킹 라이브러리는 `asyncio.to_thread`로 격리한다.
- 독립 I/O는 `asyncio.gather`로 병렬화한다 (예: tsvector 검색 + 벡터 검색 동시 실행).
- 리소스는 `async with`로 수명 관리. 풀(asyncpg, redis)은 앱 시작 시 1회 생성·재사용한다.

## 8. 에러 처리
- **구체적 예외**만 잡는다. `except Exception:` 광범위 캐치 금지(최상위 워커 루프의 의도적 격리 제외, 이때도 로깅·재발행 필수).
- 도메인 예외 계층을 둔다: `AppError` → `OcrError`, `RagError`, `GuardrailError` 등. — 호출자가 종류별로 대응할 수 있다.
- 예외를 삼키지 않는다. 복구 불가면 컨텍스트를 붙여 재발생(`raise ... from e`).

## 9. 로깅
- 표준 `logging`(또는 structlog) 기반 **구조적 로깅**. `print` 금지. — 운영에서 검색·집계가 가능해야 한다.
- 로그에 **PII를 남기지 않는다**(주민번호·계좌·연락처). 가드레일 마스킹 후 값만 로깅. — 개인정보보호법 준수가 이 도메인의 핵심 제약.
- 상관관계 추적용 식별자(`job_id`, `session_id`, `correlation_id`)를 로그 컨텍스트에 포함한다.

## 10. 함수·모듈 설계
- 함수는 **단일 책임**, 가급적 짧게. 깊은 중첩은 **조기 반환**(early return)으로 푼다.
- 부수효과(DB 쓰기, 발행)와 순수 로직(분류, 점수 계산)을 분리한다. — 순수 로직은 테스트가 쉽다.
- 매직 넘버는 명명 상수로. (예: RRF의 `RRF_K = 60`, trigram 임계 `SIMILARITY_THRESHOLD = 0.4`)

## 11. 주석·문서화
- 주석은 **왜**를 설명한다. 코드가 말하는 **무엇**을 반복하지 않는다.
- 공개 모듈·클래스·함수에 **Google 스타일 docstring**(Args/Returns/Raises). 자명한 내부 함수는 생략 가능.

## 12. 테스트
- **pytest + pytest-asyncio**. 비동기 테스트는 `@pytest.mark.asyncio`.
- AAA(Arrange-Act-Assert) 구조. 테스트 함수에 분기·반복 로직을 넣지 않는다. — 테스트가 또 다른 버그원이 되지 않게.
- 외부 의존(Kafka·PG·Ollama)은 로컬 docker-compose 또는 페이크/픽스처로 격리한다.
- 경계 계약(Kafka 메시지 스키마, WS 메시지)은 반드시 테스트로 고정한다.

## 13. 보안·개인정보 (도메인 필수)
- 외부 OCR/LLM API로 원문 전송 금지. 모든 추론은 로컬 GPU(PaddleOCR·Ollama). — 노션 02번 설계 근거.
- PII는 파이프라인 진입 직후 마스킹하고, 마스킹된 텍스트만 downstream(RAG·LLM)으로 넘긴다.
- 시크릿은 코드·로그·커밋에 두지 않는다. `pydantic-settings`로 환경변수에서 로드한다.
