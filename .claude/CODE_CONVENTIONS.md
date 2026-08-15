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
- **외부 경계**(SQS 메시지, WebSocket 메시지, API I/O)는 **pydantic v2 `BaseModel`**로 정의·검증한다. — 잘못된 페이로드를 진입점에서 거른다. 이 프로젝트는 Spring과의 계약(`core/contracts.py`)이 핵심이므로 필수.
- **내부 순수 데이터**는 `@dataclass(slots=True)` 또는 pydantic 중 맥락에 맞게. 가변/검증 불필요하면 dataclass가 가볍다.
- pydantic v2 API 사용(`model_validate`, `model_dump`). v1 메서드(`.dict()`, `.parse_obj()`) 금지.

## 7. 비동기 (async-first)
- I/O 경로는 **async/await**로 작성한다 (`asyncpg`, FastAPI). — 워커·웹소켓이 다수 연결을 효율적으로 처리한다.
- **async 함수 안에서 블로킹 호출 금지**(동기 `requests`, 동기 DB 드라이버, `time.sleep`). 블로킹 라이브러리는 `asyncio.to_thread`로 격리한다 — `boto3`(SQS·S3)가 대표 사례: 동기 SDK라 수신·삭제·발행 호출을 전부 `asyncio.to_thread`로 뗀다(`core/sqs/*`).
- 독립 I/O는 `asyncio.gather`로 병렬화한다 (예: tsvector 검색 + 벡터 검색 동시 실행).
- 리소스는 `async with`로 수명 관리. 풀(asyncpg, redis)은 앱 시작 시 1회 생성·재사용한다.

## 8. 에러 처리
- **구체적 예외**만 잡는다. `except Exception:` 광범위 캐치 금지(최상위 워커 루프의 의도적 격리 제외, 이때도 로깅·재발행 필수).
- 도메인 예외 계층을 둔다: `AppError` → `OcrError`, `RagError`, `GuardrailError` 등. — 호출자가 종류별로 대응할 수 있다.
- 예외를 삼키지 않는다. 복구 불가면 컨텍스트를 붙여 재발생(`raise ... from e`).
- **재시도 가치 판정은 마커 예외로**: 같은 입력이면 재전달해도 결과가 같은 결정적 실패는 도메인 예외에 `NonRetryableError`(`core/exceptions.py`)를 함께 상속시킨다(예: `class UnreadableFileError(OcrError, NonRetryableError)`). SQS 컨슈머가 이를 즉시 ack(삭제)로 처리해, 재전달로는 못 살리는 메시지가 재전달 상한까지 큐를 도는 낭비를 없앤다. 판정 기준은 "입력이 같으면 결과도 같은가" — 일시적 네트워크·권한 전파 지연처럼 시간이 해결하는 실패는 여기 넣지 않는다.

## 9. 로깅
- 표준 `logging`(또는 structlog) 기반 **구조적 로깅**. `print` 금지. — 운영에서 검색·집계가 가능해야 한다.
- 로그에 **PII를 남기지 않는다**(주민번호·계좌·연락처). 가드레일 마스킹 후 값만 로깅. — 개인정보보호법 준수가 이 도메인의 핵심 제약.
- **예외 메시지도 PII 경계다**: 원문을 파싱·렌더링하다 실패한 예외는 메시지에 원문 조각이 섞일 수 있다. `logger.error("...", error=str(exc))`처럼 메시지 본문을 로깅하지 말고, `error_type=type(exc).__name__`처럼 **타입/분류값만** 남긴다(예: `sqs handler terminal failure → ack`, `error_type=UnreadableFileError`).
- 상관관계 추적용 식별자(`job_id`, `session_id`, `correlation_id`)를 로그 컨텍스트에 포함한다.

## 10. 함수·모듈 설계
- 함수는 **단일 책임**, 가급적 짧게. 깊은 중첩은 **조기 반환**(early return)으로 푼다.
- 부수효과(DB 쓰기, 발행)와 순수 로직(분류, 점수 계산)을 분리한다. — 순수 로직은 테스트가 쉽다.
- 매직 넘버는 명명 상수로. (예: RRF의 `RRF_K = 60`, trigram 임계 `SIMILARITY_THRESHOLD = 0.4`)
- **다단계 DB 쓰기를 조합 가능하게**: repository 함수가 `asyncpg.Connection`을 직접 받으면 호출자가 여러 함수를 한 트랜잭션으로 묶을 수 없다. 대신 `Executor = asyncpg.Pool | asyncpg.Connection` 타입 별칭을 받게 해, 단일 호출은 `pool`을 그대로 넘기고 원자성이 필요한 호출은 `pool.acquire()` + `conn.transaction()`으로 감싼 `conn`을 넘긴다(`ocr_worker/repository.py` 참고).

## 11. 주석·문서화
- 주석은 **왜**를 설명한다. 코드가 말하는 **무엇**을 반복하지 않는다.
- 공개 모듈·클래스·함수에 **Google 스타일 docstring**(Args/Returns/Raises). 자명한 내부 함수는 생략 가능.

## 12. 테스트
- **pytest + pytest-asyncio**. 비동기 테스트는 `@pytest.mark.asyncio`.
- AAA(Arrange-Act-Assert) 구조. 테스트 함수에 분기·반복 로직을 넣지 않는다. — 테스트가 또 다른 버그원이 되지 않게.
- 외부 의존(SQS·PG·Ollama)은 로컬 docker-compose(LocalStack·pgvector) 또는 페이크/픽스처로 격리한다.
- 경계 계약(SQS 메시지 스키마, WS 메시지)은 반드시 테스트로 고정한다.

## 13. 보안·개인정보 (도메인 필수)
- 외부 OCR/LLM API로 원문 전송 금지. 모든 추론은 로컬 GPU(surya-ocr·Ollama). — 노션 02번 설계 근거.
- PII는 파이프라인 진입 직후 마스킹하고, 마스킹된 텍스트만 downstream(RAG·LLM)으로 넘긴다.
- 시크릿은 코드·로그·커밋에 두지 않는다. `pydantic-settings`로 환경변수에서 로드한다.

## 14. DB 스키마 소유권 경계
- DB는 스키마별로 소유자가 나뉜다: `ai`(ai_owner — OCR·리포트 워커), `core`(app_owner — Spring 백엔드), `corpus`(corpus_owner). 마이그레이션은 **자기 소유 스키마만** DDL을 낸다. — 다른 소유자의 오브젝트를 건드리면 권한 에러로 워커 기동 자체가 막힌다(실제 사고 #48~#50).
- `migrations/ai/*`처럼 워커가 진입 시 자동 적용하는 마이그레이션 디렉터리는 **자기 스키마 서브디렉터리만** 가리켜야 한다(`ocr_worker/__main__.py`의 `_MIGRATIONS_DIR = "migrations/ai"` 참고) — `migrations/` 전체를 돌리면 다른 owner 소유 오브젝트에 막힌다.
- 다른 팀(Spring)이 우리 스키마를 읽어야 하면(예: `ai.ocr_job_failures`를 `app_owner`가 `@Subselect`로 읽기 전용 소비) `GRANT SELECT`만 내주고 쓰기는 절대 주지 않는다. 대상 테이블이 아직 없을 수 있는 마이그레이션 순서라면 `IF to_regclass('ai.table_name') IS NOT NULL THEN ... END IF;`로 감싸 존재 여부를 먼저 확인한다(`deploy/schema_split.sql` 패턴).
