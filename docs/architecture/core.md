# core 공용 인프라 — "어떻게 동작하는가"의 배관

> 출처: AI 엔진 아키텍처 문서 세트 · 최종 점검일 2026-07-15 · 브랜치 `11-feature-langgraph-멀티에이전트-구현`
> 상위: [README](./README.md) · 원본 코드 정독 + 적대적 교차검증(코드 재대조) 완료

> 🎯 **한 문장 요약**
> 모든 워커와 공용 모듈이 함께 쓰는 여섯 개의 밑바탕 부품(AI 호출·환경설정·DB 연결·Kafka 소비/발행·로깅)이 각각 무엇을 하고 어떻게 동작하는지를 코드 근거와 함께 설명하는 문서다.

> 🌱 **쉽게 말하면**
> `src/core`는 건물로 치면 전기·수도·가스 같은 **기본 배관**이다. OCR 워커든 리포트 워커든 챗봇이든, 방(기능)마다 따로 우물을 파지 않고 이 공용 배관에서 물을 끌어 쓴다. 이 배관에는 여섯 개의 관이 있다 — AI 모델에게 말을 거는 관(`ai_client`), 설정값을 한곳에 모아두는 관(`config`), 데이터베이스와 연결을 유지하는 관(`db`), 일감을 받아오는 관(Kafka `consumer`), 결과를 내보내는 관(Kafka `producer`), 무슨 일이 있었는지 기록하는 관(`logging`)이다. 이 관들이 튼튼해야 위층의 모든 기능이 안정적으로 돈다. 그래서 "다른 모두가 여기에 의존하니 가장 먼저 튼튼하게 만든다"는 원칙이 붙어 있다.

`src/core`는 모든 워커(`ocr_worker`·`report_worker`·`chatbot`)와 공용 모듈(`rag`·`guardrail`)이 의존하는 **토대**다. `README.md`가 명시하듯 "다른 모두가 여기에 의존하므로 가장 먼저 안정화"하는 계층이며(`src/core/README.md:3`), 담당하는 모듈은 `config`(환경설정 단일 출처)·`contracts`(메시지 계약)·`kafka/`(consumer/producer)·`db`(asyncpg 풀)·`ai_client`(OpenAI 호환 추론)·`logging`(구조적 로깅)이다(`src/core/README.md:7-14`).

아래는 각 모듈이 **무엇을 하고, 무슨 설정을 참조하고, 어떻게 동작하며, 상태가 어떻게 바뀌는지**를 코드 근거와 함께 정리한 것이다.

---

## 1. `ai_client` — OpenAI 호환 추론 클라이언트 (Ollama/vLLM/TEI)

> 이 모듈은 AI 모델(LLM, 사람의 말을 이해하고 답을 생성하는 대형 언어 모델)에게 질문을 보내고 답을 받아오는 **전화기** 역할을 한다. OpenAI 호환(챗GPT를 만든 회사가 정한 요청·응답 형식을 그대로 따르는 방식)이라, 로컬에서 도는 Ollama든 클라우드의 vLLM이든 같은 방식으로 말을 걸 수 있다.

### 1.1 설계 원칙과 상태

모듈 docstring(파이썬 파일·함수 맨 위에 적는 설명 문자열)이 밝히는 세 가지 원칙이다(`src/core/ai_client.py:1-6`):

- `base_url`(요청을 보낼 서버 주소)·모델명·인증키는 **config에서 주입**하고 하드코딩(코드에 값을 직접 박아넣는 것)하지 않는다.
- **챗과 임베딩 엔드포인트를 분리**한다 — "챗과 임베딩은 서로 다른 노드에 있을 수 있어(EXAONE vs qwen3:embedding)"기 때문이다.
  > 쉽게 말하면: 대화를 담당하는 모델(EXAONE)과 문장을 좌표로 바꿔주는 임베딩(embedding, 문장의 뜻을 숫자 벡터로 바꿔 컴퓨터가 비교할 수 있게 만드는 것) 모델(qwen3:embedding)이 서로 다른 컴퓨터에 살 수 있으니, 전화번호(주소)도 따로 둔다는 뜻이다.
- 모든 호출은 async-first(비동기 우선 — 답을 기다리는 동안 다른 일을 멈추지 않고 계속 처리)이며 블로킹(한 작업이 끝날 때까지 프로그램 전체가 멈추는 것)을 유발하지 않는다(`httpx.AsyncClient`).

모듈 전역에 두 개의 지연 생성(lazy, 실제로 처음 필요해질 때까지 만들지 않고 미뤄두는 것) 클라이언트 싱글턴(프로세스 전체에서 딱 하나만 두고 재사용하는 객체)을 둔다(`src/core/ai_client.py:15-16`):

```python
_chat_client: httpx.AsyncClient | None = None
_embed_client: httpx.AsyncClient | None = None
```

### 1.2 예외 계층

```python
class AiClientError(RuntimeError):
    """추론 호출 실패의 기반 예외."""


class EmbeddingDimensionError(AiClientError):
    """임베딩 차원이 계약값(embedding_dim)과 다를 때 발생."""
```

`EmbeddingDimensionError`가 `AiClientError`를 상속(부모 클래스의 성질을 물려받는 것)하므로, 호출자가 `AiClientError` 하나만 잡아도 차원 오류까지 함께 처리된다(`src/core/ai_client.py:19-24`).

> 쉽게 말하면: "AI 호출 오류"라는 큰 우산 아래에 "임베딩 차원 오류"라는 작은 우산이 들어 있어서, 큰 우산 하나만 펴도 둘 다 막힌다.

### 1.3 인증 헤더

```python
def _auth_headers() -> dict[str, str]:
    """OpenAI 호환 인증 헤더. 로컬(`not-needed`)이면 헤더를 붙이지 않는다."""
    key = settings.ai_api_key
    if key and key != "not-needed":
        return {"Authorization": f"Bearer {key}"}
    return {}
```

`settings.ai_api_key`가 비어 있거나 문자열 `"not-needed"`(config 기본값)면 `Authorization` 헤더(요청에 신분증처럼 붙이는 인증 정보)를 생략한다(`src/core/ai_client.py:27-32`). 즉 로컬 Ollama처럼 인증이 필요 없는 곳에는 자연스럽게 헤더 없이 나가고, 실제 서비스에서는 env(환경변수)로 키를 넣어주면 Bearer 인증이 붙는다.

### 1.4 클라이언트 지연 생성과 종료

챗·임베딩 클라이언트는 최초 호출 시 딱 한 번 만들어져 계속 재사용된다(`src/core/ai_client.py:35-56`):

```python
def _get_chat_client() -> httpx.AsyncClient:
    """챗 추론용 공유 AsyncClient(지연 생성)."""
    global _chat_client
    if _chat_client is None:
        _chat_client = httpx.AsyncClient(
            base_url=settings.ai_base_url,
            timeout=settings.ai_timeout_seconds,
            headers=_auth_headers(),
        )
    return _chat_client
```

임베딩용도 구조는 같지만 `base_url=settings.embedding_base_url`을 쓴다(`src/core/ai_client.py:47-56`) — 이것이 "챗과 다른 엔드포인트일 수 있다"는 설계를 실제로 구현한 부분이다. 두 클라이언트 모두 타임아웃(응답을 이만큼 기다려도 안 오면 포기하는 시간)은 똑같이 `settings.ai_timeout_seconds`(기본 60초)를 쓴다.

종료는 앱이 꺼질 때 딱 한 번 부르는 `close_client()`가 맡는다(`src/core/ai_client.py:59-66`):

```python
async def close_client() -> None:
    """공유 AsyncClient들을 종료한다(앱 종료 시 1회)."""
    global _chat_client, _embed_client
    for client in (_chat_client, _embed_client):
        if client is not None:
            await client.aclose()
    _chat_client = None
    _embed_client = None
```

두 클라이언트를 모두 `aclose()`로 닫고 전역 참조를 `None`으로 되돌려서, 다음에 다시 필요해지면 새로 지연 생성될 수 있도록 상태를 깨끗이 초기화한다.

### 1.5 `chat()` — 비스트리밍 채팅 완성

> 이 함수는 AI에게 대화 메시지를 던지고 **완성된 답 하나를 통째로** 받아오는 통로다. (비스트리밍 — 글자를 한 자씩 흘려보내지 않고, 다 쓴 답을 한 번에 준다.)

시그니처(함수가 무엇을 받고 무엇을 돌려주는지 알려주는 선언): `async def chat(messages: list[dict[str, str]], **opts: Any) -> str`(`src/core/ai_client.py:69`).

동작(`src/core/ai_client.py:82-98`):

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
except httpx.HTTPError as exc:
    raise AiClientError("chat 호출 실패") from exc
except (KeyError, IndexError, TypeError) as exc:
    raise AiClientError("chat 응답 형식이 올바르지 않음") from exc
```

핵심 사항:
- 모델은 `opts["model"]`로 호출할 때마다 바꿔 끼울(오버라이드) 수 있고, 안 주면 `settings.llm_model`(예: EXAONE 계열)이 기본값이 된다.
- **항상 `stream: False`** — 챗봇도 비스트리밍(완성 응답 1회)이라는 아키텍처와 딱 맞는다.
- `**opts`로 `temperature`(답의 무작위성·창의성 정도) 등 임의의 OpenAI 호환 파라미터를 그대로 실어 보낸다.
- POST 경로는 `/chat/completions`이고, 여기에 `base_url`(`settings.ai_base_url`, 기본 `http://localhost:11434/v1`)이 앞에 붙어 완전한 주소가 된다.
- HTTP 오류(`httpx.HTTPError`, `raise_for_status()` 포함)는 `AiClientError("chat 호출 실패")`로, 응답 구조 파싱 실패(`KeyError`/`IndexError`/`TypeError`)는 `AiClientError("chat 응답 형식이 올바르지 않음")`으로 감싸서 던진다. 원인이 된 예외는 `from exc`로 함께 매달아(체이닝) 남긴다.

**재시도 없음:** `chat`/`embed` 자체에는 재시도 로직이 없다. 타임아웃은 있지만, 실패하면 곧바로 예외를 던진다. 다시 시도하는 일은 위층(예: Kafka 컨슈머의 `_handle_with_retry`, §4.6)에서 맡는다.

> 쉽게 말하면: 이 전화기는 한 번 걸어 안 받으면 스스로 다시 걸지 않는다. "다시 걸기"는 위층이 알아서 한다.

### 1.6 `chat_json()` — 관용적 JSON 파서

> AI에게 "JSON(데이터를 키:값 형태로 적는 표준 텍스트 형식)으로 답해줘"라고 시키면, 종종 코드펜스(```` ``` ````로 감싼 코드 블록 표시)나 잡담을 섞어 보낸다. 이 함수는 그런 지저분한 답에서도 **너그럽게(관용적으로)** JSON만 뽑아낸다.

시그니처: `async def chat_json(messages: list[dict[str, str]], **opts: Any) -> Any`(`src/core/ai_client.py:101`).

docstring이 밝히듯 "리포트·챗봇·가드레일이 공유하는 JSON 모드 헬퍼"로, LLM이 ```` ```json ```` 코드펜스나 잡음을 섞어도 관용적으로 파싱한다(`src/core/ai_client.py:102-117`). 동작(`src/core/ai_client.py:118-132`):

```python
raw = await chat(messages, **opts)
s = raw.strip()
if s.startswith("```"):
    s = s.split("```", 2)[1] if s.count("```") >= 2 else s.strip("`")
    s = s[4:].strip() if s.lower().startswith("json") else s.strip()
try:
    return json.loads(s)
except (json.JSONDecodeError, ValueError):
    start, end = s.find("{"), s.rfind("}")
    if start != -1 and end != -1:
        try:
            return json.loads(s[start : end + 1])
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}
```

파싱 전략은 3단계다:
1. **코드펜스 제거** — ```` ``` ````로 시작하면 펜스 사이를 뽑고, `json` 접두어가 있으면 앞 4글자를 잘라낸다.
2. **그대로 시도** — 정리한 문자열을 바로 `json.loads`로 파싱한다.
3. **중괄호 폴백(대비책)** — 실패하면 첫 `{`와 마지막 `}` 사이만 잘라 다시 파싱한다. 그래도 실패하거나 중괄호가 아예 없으면 빈 딕셔너리 `{}`를 돌려준다.

> 쉽게 말하면: AI가 답 주변에 군더더기를 붙여도, 중괄호 `{ }`로 감싼 알맹이만 골라 먹는다. 도저히 못 먹겠으면 빈 접시 `{}`를 내놓는다.

주의: `chat()` 자체의 HTTP·형식 오류(`AiClientError`)는 삼키지 않고 그대로 위로 올려보낸다(`src/core/ai_client.py:105-106`, `118`). 즉 "파싱 실패 → `{}`"는 오직 **내용을 해석하는 단계**에만 적용되고, LLM 호출 자체가 실패한 경우는 호출자에게 예외로 전달된다.

### 1.7 `embed()` — 임베딩 (1024차원 계약 강제)

> 임베딩은 문장을 **좌표(숫자들의 나열, 벡터)로 바꾸는 일**이다. "고양이"와 "강아지"는 가까운 좌표에, "고양이"와 "세금"은 먼 좌표에 놓여, 컴퓨터가 뜻이 비슷한지 거리로 잴 수 있게 된다.

시그니처: `async def embed(text: str) -> list[float]`(`src/core/ai_client.py:135`).

동작(`src/core/ai_client.py:148-164`):

```python
payload: dict[str, Any] = {"model": settings.embedding_model, "input": text}
client = _get_embed_client()
try:
    resp = await client.post("/embeddings", json=payload)
    resp.raise_for_status()
    data = resp.json()
    vector: list[float] = data["data"][0]["embedding"]
except httpx.HTTPError as exc:
    raise AiClientError("embed 호출 실패") from exc
except (KeyError, IndexError, TypeError) as exc:
    raise AiClientError("embed 응답 형식이 올바르지 않음") from exc

if len(vector) != settings.embedding_dim:
    raise EmbeddingDimensionError(
        f"임베딩 차원 불일치: {len(vector)} != {settings.embedding_dim}"
    )
return vector
```

핵심 사항:
- 모델은 `settings.embedding_model`(예: `qwen3:embedding`), POST 경로는 `/embeddings`, 주소는 임베딩 전용 클라이언트(`settings.embedding_base_url`)를 쓴다.
- 돌려받은 벡터의 길이가 `settings.embedding_dim`(계약상 **1024**, config `DEFAULT_EMBEDDING_DIM = 1024`)과 다르면 `EmbeddingDimensionError`를 던진다. 이는 뒤에서 벡터를 저장할 pgvector `vector(1024)` 컬럼과 아귀가 맞는지를 실행 중에 지켜주는 방어 장치다.
  > 쉽게 말하면: 벡터가 1024칸짜리여야 하는데 다른 크기가 오면, 잘못된 걸 DB에 넣기 전에 그 자리에서 막는다. 상자 크기(1024)와 물건 크기가 다르면 아예 담지 않는 것.
- 오류 매핑은 `chat`과 똑같은 방식(HTTP 오류 / 형식 오류)이다.

---

## 2. `config` — 환경설정 단일 출처 (pydantic-settings)

> 이 모듈은 모든 설정값(서버 주소, 비밀번호, 토픽 이름 등)을 **한 서랍에 모아두는 곳**이다. 여기저기서 제각기 값을 읽어오면 관리가 어렵고 실수가 나므로, "설정은 무조건 여기서만 읽는다"는 규칙을 세운다. pydantic-settings는 이 값들을 `.env` 파일이나 환경변수에서 안전하게 읽어와 타입까지 검사해 주는 도구다.

### 2.1 설계

모듈 docstring이 밝히는 원칙(`src/core/config.py:1-9`): 전 워커·모듈이 공유하는 설정을 한 곳에 모으고, 모델·엔드포인트를 하드코딩하지 않으며, 시크릿(DB 비밀번호 같은 비밀값)을 코드·로그·커밋에 남기지 않는다. 그리고 "`os.getenv`(환경변수를 직접 읽는 함수) 산재를 금지하고 여기서만 로드한다"(`src/core/config.py:8-9`).

임베딩 차원은 모듈 상수로 못박혀 있다(`src/core/config.py:15-16`):

```python
# 임베딩 차원은 계약상 고정값(qwen3:embedding 1024d, BGE-M3 폴백도 1024d).
DEFAULT_EMBEDDING_DIM = 1024
```

### 2.2 `Settings` 로딩 규약

```python
class Settings(BaseSettings):
    """전 워커가 공유하는 환경설정. env 변수명은 필드명과 동일(대소문자 무시)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
```

`.env` 파일에서 UTF-8로 읽어오고, 환경변수 이름은 필드명과 같되 대소문자는 구분하지 않으며(`case_sensitive=False`), 정의되지 않은 낯선 환경변수는 그냥 무시한다(`extra="ignore"`)(`src/core/config.py:19-27`).

### 2.3 전체 필드 (verbatim)

> 아래 네 덩어리는 실제 코드를 **그대로(verbatim)** 옮긴 것이다. 각 줄 오른쪽 `#` 주석이 그 값이 무슨 뜻인지 알려준다.

**환경·관측성**(`src/core/config.py:29-33`):

```python
environment: str = "local"  # local | dev | prod
log_level: str = ""  # 빈값이면 환경별 결정(local=DEBUG, 그 외=INFO)
service_name: str = "ai-engine"  # 워커별 env(SERVICE_NAME)로 덮어씀
instance_id: str = ""  # 인스턴스/Pod 식별자(비면 hostname)
```

**Database**(`src/core/config.py:35-40`):

```python
database_url: str = "postgresql://postgres:postgres@localhost:5432/ai_engine"
db_pool_min_size: int = 2
db_pool_max_size: int = 10
rds_ca_path: str | None = None  # RDS TLS CA 번들 경로. 로컬 PG면 비움(SSL 끔)
redis_url: str = "redis://localhost:6379/0"
```

**Kafka**(`src/core/config.py:42-49`):

```python
kafka_bootstrap_servers: str = "localhost:9092"
kafka_ocr_job_topic: str = "ocr-job-queue"
kafka_report_job_topic: str = "report-job"
kafka_security_protocol: str = "PLAINTEXT"  # PLAINTEXT | SSL | SASL_SSL
kafka_consumer_group: str = "ocr-worker"
kafka_dlq_suffix: str = ".dlq"
kafka_max_retries: int = 3
```

**S3**(`src/core/config.py:51-54`):

```python
aws_region: str = "ap-northeast-2"
s3_bucket: str = ""
```

**PII 마스킹**(`src/core/config.py:55-56`):

```python
use_ner: bool = False  # NER 디텍터 활성 여부(false면 정규식만)
```

> PII(Personally Identifiable Information — 주민번호·전화번호처럼 개인을 특정할 수 있는 정보), NER(Named Entity Recognition — 문장에서 이름·장소 같은 고유명사를 알아서 찾아내는 기술)를 가리킨다. 여기서는 NER을 끄면(false) 정규식(정해진 글자 패턴을 찾는 규칙)만으로 개인정보를 가린다.

**AI 서빙**(`src/core/config.py:58-65`):

```python
ai_base_url: str = "http://localhost:11434/v1"  # 챗 추론 엔드포인트
ai_api_key: str = "not-needed"  # OpenAI 호환 인증(로컬은 미사용)
llm_model: str = ""  # 예: EXAONE 계열
embedding_base_url: str = "http://localhost:11434/v1"  # 임베딩 엔드포인트(별도 노드 가능)
embedding_model: str = ""  # 예: qwen3:embedding (1024d)
embedding_dim: int = DEFAULT_EMBEDDING_DIM
ai_timeout_seconds: float = 60.0  # 추론 HTTP 요청 타임아웃
```

### 2.4 "빈 문자열 강제주입" 설계

`llm_model`과 `embedding_model`의 기본값은 **빈 문자열** `""`이다(`src/core/config.py:61`, `63`). 주석에 "예: EXAONE 계열", "예: qwen3:embedding (1024d)"라고만 적고 실제 모델명을 넣지 않은 건, docstring의 "AI 모델은 미정 → base_url/model만 다룸(하드코딩 금지)"(`README.md:9`) 방침 때문이다. 즉 **모델명은 반드시 env로 넣어줘야** 하며, 넣지 않으면 빈 문자열이 그대로 payload의 `"model"` 값으로 나간다. 코드에는 빈 값을 막는 검증이 없으므로, 모델명을 안 넣은 실수는 config가 아니라 추론 엔드포인트가 거부하는 형태로 드러난다(즉, 이 계층에는 fail-fast 가드 — 잘못을 즉시 앞단에서 잡아 멈추는 안전장치 — 가 없다. 정직하게 말하면 미구현).

> 쉽게 말하면: 모델 이름 칸을 일부러 비워 뒀다. "여기는 네가 채워야 하는 칸"이라는 신호다. 안 채우고 실행하면 이 문서 계층에서 미리 경고해 주지는 않고, 그다음 AI 서버가 "그런 모델 없다"며 거절하는 방식으로 뒤늦게 드러난다.

마찬가지로 `log_level`(`""` → 환경별 결정), `instance_id`(`""` → hostname), `s3_bucket`(`""`)도 빈 문자열을 기본값으로 두고, 실제 값은 런타임·env가 채우도록 설계했다.

### 2.5 싱글턴 접근

```python
@lru_cache
def get_settings() -> Settings:
    ...
    return Settings()


# 단일 settings 인스턴스. `from core.config import settings` 또는 `get_settings()` 사용.
settings = get_settings()
```

`@lru_cache`(한 번 계산한 결과를 기억해 두고 다음부터 재사용하는 캐시)로 프로세스에 딱 하나의 인스턴스만 있도록 보장하고(`src/core/config.py:68-75`), 모듈을 임포트하는 순간 `settings = get_settings()`로 바로 만들어 둔다(`src/core/config.py:79`). 다른 모듈은 `from core.config import settings`(예: `ai_client`, `logging`)나 `get_settings()`(예: `db`, `kafka`)로 이 하나의 설정을 가져다 쓴다.

---

## 3. `db` — asyncpg 연결 풀 lifecycle

> 데이터베이스에 연결할 때마다 새로 접속을 여는 건 느리고 낭비다. 그래서 미리 몇 개의 연결을 만들어 **주머니(풀, pool)에 담아두고 돌려쓴다.** asyncpg는 PostgreSQL(관계형 데이터베이스)에 비동기로 접속하는 파이썬 라이브러리다. 이 모듈은 그 연결 주머니를 언제 만들고 언제 닫을지(lifecycle, 수명 주기)를 관리한다.

### 3.1 설계와 두 가지 사용 패턴

docstring(`src/core/db.py:1-11`)의 요지: 앱이 시작할 때 풀을 한 번 만들어 계속 재사용하고, RDS(아마존이 운영해 주는 클라우드 DB)는 암호화 연결(TLS)을 요구하므로 `rds_ca_path`(인증서 파일 경로)가 있으면 그 인증서로 검증하는 SSL 컨텍스트를, 로컬 PG면 SSL을 끈다(같은 코드가 양쪽에서 다 동작한다). 또 pgvector(PostgreSQL에서 벡터를 저장·검색하게 해주는 확장 기능)의 `vector` 타입을 커넥션마다 등록해 `list[float] ↔ vector(1024)`를 서로 주고받게 한다.

두 가지 사용 방식을 모두 지원한다(`src/core/db.py:8-11`):
- **전역 싱글턴:** `init_pool()` → `get_pool()` → `close_pool()` (RAG·오래 사는 서비스용)
- **컨텍스트 매니저:** `async with db_pool() as pool:` (워커 진입점용)

> 쉽게 말하면: 오래 켜 두는 서비스는 주머니를 한 번 열어 계속 들고 다니고(전역 싱글턴), 잠깐 돌고 끝나는 워커는 `async with` 블록 안에서만 주머니를 열었다가 블록을 나갈 때 자동으로 닫는다(컨텍스트 매니저).

전역 상태는 모듈 변수 하나로 관리한다(`src/core/db.py:25`):

```python
_pool: asyncpg.Pool | None = None
```

초기화 전에 접근하면 알려주는 예외 `PoolNotInitializedError(RuntimeError)`도 정의해 둔다(`src/core/db.py:28-29`).

### 3.2 SSL 컨텍스트 빌드

```python
def _build_ssl(ca_path: str | None) -> ssl.SSLContext | None:
    """RDS면 CA 번들로 SSL 컨텍스트를, 로컬 PG면 None(SSL 끔)을 반환한다."""
    if not ca_path:
        return None
    return ssl.create_default_context(cafile=ca_path)
```

`rds_ca_path`가 비어 있으면 `None`(SSL 끔)을, 값이 있으면 그 CA 번들(신뢰할 수 있는 인증서 묶음)로 검증하는 표준 SSL 컨텍스트를 만든다(`src/core/db.py:32-36`).

### 3.3 커넥션 초기화 — pgvector 등록

```python
async def _init_connection(conn: asyncpg.Connection) -> None:
    """신규 커넥션마다 pgvector 타입을 등록한다(`vector` ↔ `list[float]`)."""
    await register_vector(conn)
```

풀이 새 커넥션을 만들 때마다 이 콜백(`init=`)이 호출되어 `register_vector`로 pgvector 코덱(변환 규칙)을 등록한다(`src/core/db.py:39-41`). 이게 없으면 RAG 벡터 검색에서 파이썬 `list[float]`를 DB의 `vector`로 넘길 수 없다.

> 쉽게 말하면: 파이썬의 "숫자 리스트"와 DB의 "벡터"는 서로 말이 안 통하는데, 새 연결마다 통역사(코덱)를 붙여 주는 단계다.

### 3.4 저수준 팩토리 `create_pool`

```python
async def create_pool(settings: Settings | None = None) -> asyncpg.Pool:
    """asyncpg 풀을 생성한다(SSL·pgvector·pool 크기 반영). 저수준 팩토리."""
    settings = settings or get_settings()
    pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        ssl=_build_ssl(settings.rds_ca_path),
        init=_init_connection,
    )
    logger.info(
        "db pool created",
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
    )
    return pool
```

DSN(DB 접속 주소 문자열)은 `settings.database_url`, 풀 크기는 `db_pool_min_size`(기본 2)~`db_pool_max_size`(기본 10), SSL은 `_build_ssl`, 커넥션 초기화는 `_init_connection`을 끼워 조립한다(`src/core/db.py:44-59`). 만든 뒤에는 구조적 로그 `"db pool created"`를 남긴다.

### 3.5 전역 싱글턴 lifecycle

```python
async def init_pool(settings: Settings | None = None) -> asyncpg.Pool:
    """전역 풀을 생성한다(앱 시작 1회). 이미 있으면 재사용한다."""
    global _pool
    if _pool is None:
        _pool = await create_pool(settings)
    return _pool
```

`init_pool`은 `_pool`이 아직 `None`일 때만 새로 만들고, 이미 있으면 그대로 재사용한다(멱등 — 같은 걸 여러 번 실행해도 결과가 한 번 실행한 것과 같음)(`src/core/db.py:62-67`).

```python
def get_pool() -> asyncpg.Pool:
    ...
    if _pool is None:
        raise PoolNotInitializedError("init_pool()을 먼저 호출하세요")
    return _pool
```

`get_pool`은 동기 함수로, 초기화 전에 부르면 `PoolNotInitializedError`를 던져 "먼저 `init_pool()`을 부르라"는 실수를 곧바로 알려준다(`src/core/db.py:70-78`).

```python
async def close_pool() -> None:
    """전역 풀을 종료한다(앱 종료 시). 미초기화면 no-op."""
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None
```

`close_pool`은 풀을 닫고 `_pool`을 다시 `None`으로 되돌린다(초기화 안 됐으면 아무것도 안 함 — no-op)(`src/core/db.py:81-87`). 상태는 `None → (init) → Pool → (close) → None` 순서로 오간다.

### 3.6 컨텍스트 매니저 `db_pool`

```python
@asynccontextmanager
async def db_pool(settings: Settings | None = None) -> AsyncIterator[asyncpg.Pool]:
    """풀 수명을 관리하는 async 컨텍스트(워커 진입점용). 종료 시 안전하게 닫는다."""
    pool = await create_pool(settings)
    try:
        yield pool
    finally:
        await pool.close()
        logger.info("db pool closed")
```

전역 상태 `_pool`은 건드리지 않고 **자기만의 풀**을 새로 만들어 `yield`(블록 안에 넘겨줌)하고, 블록이 끝나면 `finally`에서 반드시 닫는다(`src/core/db.py:90-98`). 워커 진입점이 `async with`로 풀의 수명을 그 블록 범위에 딱 묶어 쓰는 용도다.

**주의(정직한 관찰):** 이 모듈에는 별도의 `fetch()`/`execute()`(쿼리를 실행해 주는) 헬퍼 함수가 **없다**. 풀 lifecycle만 제공하고, 실제 쿼리는 호출자가 `pool.fetch(...)` / `pool.execute(...)` 같은 asyncpg 풀 메서드나 `async with pool.acquire() as conn:`을 직접 쓰는 구조다. (프롬프트가 기대했던 "fetch/execute 헬퍼"는 이 파일에 구현되어 있지 않다.)

---

## 4. Kafka `consumer` — aiokafka 소비 루프

> Kafka는 여러 서비스가 메시지를 주고받는 **우체통(메시지 큐)** 같은 것이다. 일감을 넣어두면(발행) 워커가 하나씩 꺼내(소비) 처리한다. 이 `consumer`는 리포트/OCR 워커가 그 우체통에서 일감을 꺼내오는 부분이다.

리포트/OCR 워커가 토픽(우체통의 특정 칸, 주제별 채널)에서 작업을 받아오는 경로다. docstring이 계약을 요약한다(`src/core/kafka/consumer.py:1-8`):

- 역직렬화(bytes를 객체로 되돌리기)·검증 실패 → DLQ(Dead Letter Queue — 처리 못 한 메시지를 따로 모아두는 실패 전용 우체통)로 보내 파이프라인을 막지 않는다
- 핸들러가 잠깐 실패하면 → 그 자리에서 재시도, 그래도 안 되면 DLQ
- 처리에 성공한 뒤에만 **수동 오프셋 커밋**(at-least-once — 메시지를 최소 한 번은 처리 보장; 그래서 핸들러는 멱등이어야 함)
- SIGTERM/SIGINT(종료 신호) 수신 시 우아하게 종료

> 쉽게 말하면: 편지를 꺼내 처리하고 나서야 "이 편지 처리 끝"이라고 도장(오프셋 커밋)을 찍는다. 도장 찍기 전에 프로그램이 죽으면, 재시작 후 그 편지를 다시 꺼낸다. 그래서 편지를 두 번 처리해도 결과가 같도록(멱등) 핸들러를 만들어야 한다.

### 4.1 튜닝 상수 (verbatim)

```python
_POLL_TIMEOUT_MS = 1000
_BATCH_MAX_RECORDS = 1
_BACKOFF_BASE = 2
_MAX_BACKOFF_SECONDS = 10
```

폴(우체통 확인) 타임아웃은 1초, 한 번에 **1건씩**(`_BATCH_MAX_RECORDS = 1`) 꺼내 처리하며, 재시도 대기시간은 밑이 2인 지수(2배씩 늘어남, 최대 10초)로 벌린다(`src/core/kafka/consumer.py:24-27`).

### 4.2 제네릭 클래스와 생성자

```python
class KafkaConsumer[T: BaseModel]:
```

PEP 695 제네릭 문법(타입을 나중에 끼워 넣을 수 있게 하는 파이썬 신문법)으로 `T`를 pydantic `BaseModel`(데이터 구조를 정의·검증하는 클래스)로 제한한다(`src/core/kafka/consumer.py:30`). 생성자(`src/core/kafka/consumer.py:37-49`)는 `topic`, `schema`(검증에 쓸 pydantic 타입), `handler`(실제 처리 함수, `Callable[[T], Awaitable[None]]`), 선택적 `settings`를 받고, 종료 신호용 `asyncio.Event`를 준비한다:

```python
def __init__(
    self,
    topic: str,
    schema: type[T],
    handler: Callable[[T], Awaitable[None]],
    *,
    settings: Settings | None = None,
) -> None:
    self._topic = topic
    self._schema = schema
    self._handler = handler
    self._settings = settings or get_settings()
    self._stopping = asyncio.Event()
```

> 쉽게 말하면: 이 컨슈머는 "어느 우체통(topic)에서, 어떤 모양의 편지(schema)를, 어떤 함수(handler)로 처리할지"를 조립해서 받는 범용 부품이다. 편지 내용 자체는 바깥에서 정해 준다.

### 4.3 `run()` — 시작·루프·정리

```python
async def run(self) -> None:
    """컨슈머를 시작해 종료 신호까지 처리 루프를 돈다."""
    settings = self._settings
    consumer: AIOKafkaConsumer = AIOKafkaConsumer(
        self._topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        security_protocol=settings.kafka_security_protocol,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    self._install_signal_handlers()
    await consumer.start()
    logger.info("consumer started", topic=self._topic, group=settings.kafka_consumer_group)
    try:
        async with KafkaProducer(settings) as dlq_producer:
            await self._consume_loop(consumer, dlq_producer)
    finally:
        await consumer.stop()
        logger.info("consumer stopped", topic=self._topic)
```

핵심 설정(`src/core/kafka/consumer.py:51-70`):
- `enable_auto_commit=False` — **자동 도장(커밋) 끔**(수동으로 찍겠다는 전제).
- `auto_offset_reset="earliest"` — 찍어둔 도장 기록이 없을 땐 가장 오래된 편지부터 읽는다.
- `group_id=settings.kafka_consumer_group`(기본 `"ocr-worker"`), 브로커 주소·보안 프로토콜도 config에서 가져온다.
- 신호 핸들러를 설치한 뒤 `consumer.start()`.
- **DLQ 프로듀서를 컨텍스트로 미리 확보**(`async with KafkaProducer(settings) as dlq_producer`)한 채 루프를 돈다. 즉 실패 편지를 보낼 프로듀서 하나를 열어 두고 계속 재사용한다.
- `finally`에서 반드시 `consumer.stop()`을 불러 뒷정리를 보장한다.

### 4.4 우아한 종료 — 신호 핸들러

```python
def _install_signal_handlers(self) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, self._stopping.set)
        except NotImplementedError:
            # Windows 등 미지원 플랫폼 — KeyboardInterrupt로 종료된다.
            logger.debug("signal handler unsupported", signal=sig)
```

종료 신호(SIGTERM·SIGINT)를 받으면 `self._stopping.set()`을 걸어, 루프가 다음 바퀴에서 스스로 빠져나오게 한다(`src/core/kafka/consumer.py:72-79`). Windows처럼 `add_signal_handler`가 `NotImplementedError`를 던지는 플랫폼에서는 그 예외를 삼키고 KeyboardInterrupt(Ctrl+C)에 기댄다(개발 환경이 Windows인 이 프로젝트에서 실제로 의미 있는 폴백이다).

> 쉽게 말하면: "이제 그만"이라는 신호가 오면 곧장 뽑아버리지 않고, 지금 처리 중인 편지 한 통을 마저 끝낸 다음 조용히 문을 닫는다.

### 4.5 소비 루프와 커밋 지점

```python
async def _consume_loop(self, consumer: AIOKafkaConsumer, dlq: KafkaProducer) -> None:
    while not self._stopping.is_set():
        batches = await consumer.getmany(
            timeout_ms=_POLL_TIMEOUT_MS, max_records=_BATCH_MAX_RECORDS
        )
        for records in batches.values():
            for record in records:
                await self._process(record, dlq)
                await consumer.commit()  # 처리 성공 후에만 커밋(at-least-once)
```

`_stopping`이 켜질 때까지 `getmany`로 우체통을 확인하고, **편지 한 통을 `_process`로 처리한 직후 `consumer.commit()`으로 도장을 찍는다**(`src/core/kafka/consumer.py:81-89`). 도장이 처리 뒤에 오므로, 처리 도중 죽으면 오프셋이 안 찍혀 재시작 시 그 편지를 다시 처리한다 — 이것이 at-least-once이고, 그래서 핸들러가 멱등이어야 한다.

**정직한 관찰:** `commit()`은 `_process`가 예외를 던지지 않는 한 항상 호출된다. `_process`는 검증 실패든 재시도 초과든 모두 내부에서 DLQ로 흘려보내고 **정상 반환**하므로(§4.6~4.7), DLQ로 보낸 뒤에도 도장은 찍힌다. 즉 "DLQ에 넣었으면 그 편지는 소비 완료로 친다"는 설계다.

### 4.6 검증과 재시도

```python
async def _process(self, record: ConsumerRecord, dlq: KafkaProducer) -> None:
    try:
        model = self._schema.model_validate_json(record.value)
    except ValidationError as exc:
        logger.warning("invalid message → DLQ", topic=self._topic, error=str(exc))
        await self._to_dlq(dlq, record)
        return
    await self._handle_with_retry(model, record, dlq)
```

받은 원본 `record.value`(bytes/JSON)를 바로 `schema.model_validate_json`으로 pydantic 검증한다. 형식이 어긋나 `ValidationError`가 나면 경고 로그를 남기고 DLQ로 보낸 뒤 끝낸다(`src/core/kafka/consumer.py:91-98`).

```python
async def _handle_with_retry(
    self, model: T, record: ConsumerRecord, dlq: KafkaProducer
) -> None:
    retries = self._settings.kafka_max_retries
    for attempt in range(1, retries + 1):
        try:
            await self._handler(model)
            return
        except Exception:  # 최상위 격리: 유실 없이 재시도·DLQ (CODE_CONVENTIONS §8)
            logger.warning("handler error", topic=self._topic, attempt=attempt, max=retries)
            if attempt < retries:
                await asyncio.sleep(min(_BACKOFF_BASE**attempt, _MAX_BACKOFF_SECONDS))
    logger.error("handler exhausted retries → DLQ", topic=self._topic)
    await self._to_dlq(dlq, record)
```

동작(`src/core/kafka/consumer.py:100-113`):
- 핸들러를 최대 `settings.kafka_max_retries`회(기본 3) 부른다.
- 성공하면 즉시 반환한다.
- `Exception`을 최상위에서 격리해 편지를 잃지 않고 다시 시도한다(CODE_CONVENTIONS §8 근거).
- 마지막 시도가 아니면 `min(2**attempt, 10)`초를 쉰다(2초 → 4초 → …, 상한 10초).
- 모든 시도를 소진하면 error 로그를 남기고 DLQ로 보낸다.

> 쉽게 말하면: 실패하면 곧장 포기하지 않고, 2초·4초·8초… 점점 더 뜸을 들이며(최대 10초) 세 번까지 다시 해 본다. 그래도 안 되면 실패 우체통(DLQ)에 넣는다.

### 4.7 DLQ 전송

```python
async def _to_dlq(self, dlq: KafkaProducer, record: ConsumerRecord) -> None:
    dlq_topic = f"{self._topic}{self._settings.kafka_dlq_suffix}"
    await dlq.publish_raw(dlq_topic, record.value, record.key)
```

DLQ 우체통 이름은 `원본토픽 + kafka_dlq_suffix`(기본 `.dlq`)로 만든다(`src/core/kafka/consumer.py:115-117`). 예: `report-job` → `report-job.dlq`. **원본 raw bytes(`record.value`, `record.key`)를 손대지 않고 그대로** 다시 보내(재발행) 원본을 잃지 않는다(§5.3의 `publish_raw`).

---

## 5. Kafka `producer` — 결과 발행

> `consumer`가 편지를 꺼내는 쪽이라면, `producer`는 처리 결과를 다음 우체통에 **넣는 쪽**이다.

docstring 요지(`src/core/kafka/producer.py:1-6`): 결과 이벤트(예: `ReportJob`)를 토픽에 발행하고, pydantic 모델을 UTF-8 JSON으로 직렬화(객체를 전송용 bytes로 변환)하며, 파티션 키(같은 키끼리 같은 칸으로 몰아주는 값)로 멱등·순서를 보장한다(`enable_idempotence`). DLQ용 raw bytes 경로도 함께 제공한다. "직접 클라이언트를 만들지 않고 항상 이 래퍼를 거친다."

### 5.1 컨텍스트 매니저 lifecycle

```python
class KafkaProducer:
    """수명관리되는 aiokafka 프로듀서. `async with KafkaProducer()`로 사용한다."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._producer: AIOKafkaProducer | None = None

    async def __aenter__(self) -> Self:
        producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            security_protocol=self._settings.kafka_security_protocol,
            acks="all",
            enable_idempotence=True,
        )
        await producer.start()
        self._producer = producer
        logger.info("producer started")
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._producer is not None:
            await self._producer.stop()
            logger.info("producer stopped")
```

전송 신뢰성 설정(`src/core/kafka/producer.py:26-41`):
- `acks="all"` — 모든 in-sync replica(원본과 똑같이 맞춰진 복제본)가 편지를 받은 걸 확인한 뒤에야 성공으로 친다.
- `enable_idempotence=True` — 브로커 쪽에서 중복을 걸러내고 순서를 지켜 준다.
- `__aenter__`에서 `start()`, `__aexit__`에서 `stop()`을 불러, 프로듀서 수명을 `async with` 블록 범위에 묶는다.

> 쉽게 말하면: 편지를 넣고 "잘 받았다"는 확인(모든 복제본까지)이 올 때까지 기다린다. 실수로 같은 편지를 두 번 넣어도 브로커가 알아서 하나로 정리한다.

프로듀서를 시작하지도 않고 쓰려는 실수를 막는 가드도 있다(`src/core/kafka/producer.py:43-46`):

```python
def _require(self) -> AIOKafkaProducer:
    if self._producer is None:
        raise RuntimeError("producer not started; use 'async with KafkaProducer()'")
    return self._producer
```

### 5.2 `publish` — pydantic 모델 발행

```python
async def publish(self, topic: str, message: BaseModel, *, key: str) -> None:
    """pydantic 메시지를 토픽에 발행한다(key 기준 파티셔닝)."""
    payload = message.model_dump_json().encode("utf-8")
    await self._require().send_and_wait(topic, value=payload, key=key.encode("utf-8"))
    logger.info("published", topic=topic, key=key)
```

pydantic 모델을 `model_dump_json()`으로 바꾼 뒤 UTF-8로 인코딩해 값으로, `key`(키워드로만 넘길 수 있는 인자)를 UTF-8로 인코딩해 파티션 키로 넣고, `send_and_wait`로 **전송이 끝날 때까지 기다린다**(`src/core/kafka/producer.py:48-52`). 같은 key는 같은 파티션으로 가므로, 키 단위로 순서가 보장된다.

### 5.3 `publish_raw` — DLQ용 raw bytes

```python
async def publish_raw(self, topic: str, value: bytes | None, key: bytes | None) -> None:
    """raw bytes를 그대로 발행한다(DLQ 전달 등 — 원본 유실 방지)."""
    await self._require().send_and_wait(topic, value=value, key=key)
    logger.warning("published raw", topic=topic)
```

직렬화 없이 bytes를 있는 그대로 발행한다(`src/core/kafka/producer.py:54-57`). 컨슈머의 `_to_dlq`가 이 경로로 원본 `record.value`/`record.key`를 다시 보낸다(§4.7). 로그 레벨을 `warning`으로 둔 건, DLQ 전송이 정상이 아닌 예외적 경로임을 눈에 띄게 하려는 것이다.

---

## 6. `logging` — 구조적 로깅 (structlog + OTel 시맨틱)

> 로그를 그냥 줄글로 남기면 나중에 검색·집계가 어렵다. **구조적 로깅**은 로그를 "키:값"이 딱딱 나뉜 JSON 형태로 남겨, 기계가 걸러보기 쉽게 하는 방식이다. structlog는 그런 로그를 만들어 주는 라이브러리, OTel(OpenTelemetry)은 로그·추적의 필드 이름을 업계 표준으로 맞추는 규격이다.

docstring 요지(`src/core/logging.py:1-15`): 모든 로그를 JSON으로 구조화하되 local 환경에서는 사람이 읽기 좋은 콘솔 형태로 보여주고, `trace_id`·`request_id`(요청 하나를 처음부터 끝까지 따라가게 해주는 꼬리표)를 contextvars(async 작업 경계를 넘어서도 값을 유지해 주는 파이썬 저장소)로 전파하며, PII는 화면에 찍기 직전 자동으로 가린다. 핵심 원칙은 `print` 금지, 원문 PII 미로깅, 상관관계 식별자 바인딩이다(`src/core/logging.py:7-12`). 단, 실제 OTel SDK/exporter(로그·추적을 외부 모니터링 시스템으로 실제 전송하는 부분) 연동은 이 모듈 범위 밖이며 "모니터링 인프라 단계에서 추가"한다고 명시한다(`src/core/logging.py:13-15`) — 즉 지금은 필드 이름 짓기·contextvars 전파만 제공한다(정직한 스코프 표기).

### 6.1 PII 마스킹 방어선

정규식 4종으로 정형 PII(형식이 정해진 개인정보)를 부분적으로 가린다(`src/core/logging.py:45-53`). 순수 숫자 패턴만 매칭하므로 UUID(예: `job_id`)는 그대로 보존된다(`src/core/logging.py:41`):

- 주민등록번호 `_RRN_RE` → 성별 자리 1자리만 남김(`901010-1******`)
- 카드번호 `_CARD_RE` → 뒤 4자리만 남김(전화보다 먼저 적용)
- 휴대전화 `_PHONE_RE` → `010-****-5678`
- 이메일 `_EMAIL_RE` → 앞 1~3자만 남김(`hon***@example.com`)

```python
def mask_pii(text: str) -> str:
    ...
    text = _RRN_RE.sub(r"\1-\2******", text)
    text = _CARD_RE.sub(r"****-****-****-\1", text)
    text = _PHONE_RE.sub(r"\1-****-\2", text)
    text = _EMAIL_RE.sub(r"\1***\2", text)
    return text
```

적용 순서가 의미가 있다 — 카드번호를 전화번호보다 먼저 치환한다(`src/core/logging.py:47`, `68-71`). docstring은 이것이 "사고 방지용 방어선"일 뿐이고 비정형 PII(이름·주소처럼 형식이 정해지지 않은 정보)는 못 잡으므로 근본 책임은 호출자에게 있다고 못박는다(`src/core/logging.py:57-61`).

> 쉽게 말하면: 실수로 개인정보가 로그에 새어 나가도 최소한 가려주는 **안전망**이다. 다만 이름·주소처럼 정해진 틀이 없는 정보는 못 걸러내니, 애초에 안 남기는 건 코드 작성자 몫이다.

### 6.2 프로세서 체인

리소스 필드(서비스명·환경 같은 공통 꼬리표)는 모듈이 로드될 때 `settings`에서 초기화되고, 나중에 `configure_logging()`에서 다시 갱신된다(`src/core/logging.py:79-82`):

```python
_SERVICE: str = settings.service_name
_ENVIRONMENT: str = settings.environment
_INSTANCE_ID: str = settings.instance_id or socket.gethostname()
```

환경별 기본 레벨과 OTel severity(심각도) 매핑도 상수로 둔다(`src/core/logging.py:84-87`):

```python
_DEFAULT_LEVEL_BY_ENV = {"local": "DEBUG", "dev": "INFO", "prod": "INFO"}
_SEVERITY_MAP = {"warning": "WARN", "critical": "FATAL"}
```

`_shared_processors()`가 structlog·표준 로그가 함께 쓰는 처리 파이프라인을 조립한다(`src/core/logging.py:142-154`): contextvars 병합 → 로그레벨 추가 → `level→severity` 변환 → UTC 타임스탬프(`timestamp`) → 로거명 추가 → 리소스 필드(`_resource_processor`) → 예외 분해(`_exception_processor`) → **PII 마스킹(`_mask_processor`)** → `event`를 `message`로 이름 변경. 즉 마스킹은 화면에 찍기 바로 직전 단계에서 모든 문자열 값에 한꺼번에 적용된다(`src/core/logging.py:134-139`).

> 쉽게 말하면: 로그 한 줄이 완성되기까지 여러 공정을 컨베이어벨트처럼 지난다. 그 벨트의 **맨 마지막 직전**에 개인정보 가림막을 세워, 밖으로 나가는 모든 글자를 한 번에 훑어 가린다.

### 6.3 `configure_logging()`

앱을 시작할 때 한 번 불러 structlog와 표준 로깅을 함께 구성한다(`src/core/logging.py:162-203`). 요점:
- `_SERVICE`/`_ENVIRONMENT`/`_INSTANCE_ID`를 settings 값으로 갱신한다.
- 레벨은 `settings.log_level`이 있으면 그 값을, 없으면 `_DEFAULT_LEVEL_BY_ENV`(local=DEBUG, 그 외 INFO)를 쓴다(`src/core/logging.py:173-174`).
- 렌더러(로그를 어떤 모양으로 출력할지)는 local이면 `ConsoleRenderer`(사람이 읽기 좋은 콘솔), 그 외에는 `JSONRenderer(ensure_ascii=False)` — 이 옵션으로 **한글이 깨지지 않고 보존**된다(`src/core/logging.py:177-181`).
- 표준 라이브러리의 `ProcessorFormatter`로 서드파티 로그(asyncpg·aiokafka·httpx·uvicorn)까지 같은 포맷으로 흘려보내고, 루트 핸들러를 교체한다(`src/core/logging.py:183-196`).

### 6.4 상관관계 컨텍스트 API

> "상관관계"란 흩어진 로그들을 하나의 요청으로 **묶어 추적**하는 것이다. 요청마다 고유 꼬리표(`trace_id`·`request_id`)를 달아 두면, 나중에 그 꼬리표로 관련 로그를 한 번에 모아 볼 수 있다.

- `get_logger(name)` → 값을 붙일 수 있는 structlog 로거를 돌려준다(`src/core/logging.py:206-215`). core의 다른 모듈들이 `logger = get_logger(__name__)`로 가져다 쓴다(`db.py:23`, `consumer.py:22`, `producer.py:16`).
- `bind_context(**fields)` / `clear_context()` → contextvars에 필드를 붙이거나 지운다(`src/core/logging.py:218-228`).
- `new_request_context(...)` → 요청이 들어올 때 `trace_id`(없으면 `uuid4().hex`, 32자리 16진수 = W3C 형식)와 `request_id`(없으면 `req-{12hex}`)를 만들어 붙이고 `trace_id`를 돌려준다(`src/core/logging.py:231-247`):

```python
trace_id = trace_id or uuid.uuid4().hex  # 32 hex = W3C trace-id 형식
request_id = request_id or f"req-{uuid.uuid4().hex[:12]}"
bind_context(trace_id=trace_id, request_id=request_id, **extra)
return trace_id
```

- `log_event(logger, event_type, **fields)` → 비즈니스 이벤트를 `event_type` 필드와 함께 INFO 레벨로 기록한다(`src/core/logging.py:250-258`). 예: `"rag.search.completed"`, `"report.generated"`.

---

## 7. 종합 — 워커는 이 배관 위에서 이렇게 돈다

> 지금까지 본 부품들이 어떻게 맞물려 **한 대의 워커**로 돌아가는지, 부팅부터 종료까지 순서대로 따라가 보자.

이 세 모듈이 맞물려 "워커가 실제로 어떻게 돌아가는가"를 이룬다:

1. **부팅:** 워커는 `configure_logging()`으로 로깅을 세우고(`logging.py:162`), `settings`(config 싱글턴)로 모든 엔드포인트·토픽·풀 크기를 읽는다. DB는 `init_pool()`(오래 사는 서비스용) 또는 `async with db_pool()`(워커 진입점용)로 커넥션 풀을 확보하며, 커넥션마다 pgvector가 등록된다(`db.py:39-41`).
2. **소비:** `KafkaConsumer[T].run()`이 토픽을 `enable_auto_commit=False`로 구독하고(`consumer.py:59`), 안에서 DLQ용 `KafkaProducer`를 `async with`로 연다(`consumer.py:66`). 한 번에 1건씩(`_BATCH_MAX_RECORDS`) 꺼내 pydantic 검증 → 핸들러(최대 `kafka_max_retries`회, 지수 백오프로 재시도) → **성공 후 수동 커밋**의 at-least-once 사이클을 돈다(`consumer.py:81-113`).
3. **처리 중 LLM/임베딩:** 핸들러 안에서 `ai_client.chat()`/`chat_json()`/`embed()`로 EXAONE·qwen3 등 config가 주입한 모델을 부른다. 임베딩은 1024차원 계약을 실행 중에 강제로 확인한다(`ai_client.py:160-163`).
4. **결과 발행:** `KafkaProducer.publish(topic, model, key=...)`로 다음 단계 토픽(예: `report-job`)에 pydantic 결과를 UTF-8 JSON·키 파티셔닝·`acks=all`·idempotence로 발행한다(`producer.py:48-52`).
5. **실패 격리:** 검증 실패나 재시도 소진 메시지는 `원본토픽.dlq`로 raw bytes를 그대로 흘려보내 원본을 잃지 않는다(`consumer.py:115-117`, `producer.py:54-57`).
6. **종료:** SIGTERM/SIGINT → `_stopping` set → 루프 탈출 → `consumer.stop()`(`consumer.py:72-79`, `68-70`). 풀과 AI 클라이언트는 각각 `close_pool()`·`close_client()`로 정리한다(`db.py:81-87`, `ai_client.py:59-66`).

**정직한 미구현/공백 표기:**
- `core/contracts.py`는 README에서 "메시지 계약 단일 출처"로 예고되지만(`README.md:10`), 이 배관 파일들에는 스키마 정의가 없다(컨슈머·프로듀서는 제네릭 `T: BaseModel`로 스키마를 바깥에서 주입받는다). 실제 계약 모델은 별도 파일(호출자가 제공)에 있다.
- `db.py`에는 `fetch`/`execute` 래퍼가 없다 — 풀 lifecycle만 제공하고, 쿼리는 호출자가 asyncpg 풀 API를 직접 쓴다.
- `ai_client`에는 호출 단위 재시도가 없다(재시도는 컨슈머 계층 책임). `llm_model`/`embedding_model`이 빈 문자열일 때 곧바로 막아주는 fail-fast 검증도 없다.
- `logging`은 OTel 필드 이름 짓기·contextvars 전파만 제공하고, 실제 trace exporter 연동은 미구현이다(의도적으로 범위 밖으로 뺌)(`logging.py:13-15`).
