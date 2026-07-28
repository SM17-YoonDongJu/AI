"""환경설정 단일 출처 (pydantic-settings) — 정본(canonical) 슈퍼셋.

전 워커·모듈(OCR·RAG·리포트·챗봇·가드레일)이 공유하는 설정을 한 곳에 모은다. 모델·엔드포인트는
코드에 하드코딩하지 않고 env로 주입하며, 시크릿(DB 비밀번호 등)은 코드·로그·커밋에 두지 않는다.
아래 기본값은 로컬 개발(docker-compose.dev)용이며 실제 값이 아니다.

필드 네이밍은 팀 정본 규약을 따른다 — 인프라(DB·Kafka·AI)는 서술적 이름, 관측성은 환경/서비스
식별자. `os.getenv` 산재를 금지하고 여기서만 로드한다.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

# 임베딩 차원은 계약상 고정값(qwen3:embedding 1024d, BGE-M3 폴백도 1024d).
DEFAULT_EMBEDDING_DIM = 1024

# CD 스모크: shared(core) 변경으로 3개 서비스 배포 경로 검증(값·동작 무변경). 재트리거 4.


class Settings(BaseSettings):
    """전 워커가 공유하는 환경설정. env 변수명은 필드명과 동일(대소문자 무시)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- 환경·관측성 ---
    environment: str = "local"  # local | dev | prod
    log_level: str = ""  # 빈값이면 환경별 결정(local=DEBUG, 그 외=INFO)
    service_name: str = "ai-engine"  # 워커별 env(SERVICE_NAME)로 덮어씀
    instance_id: str = ""  # 인스턴스/Pod 식별자(비면 hostname)

    # --- Database (AWS RDS PostgreSQL / 로컬 PG) ---
    database_url: str = "postgresql://postgres:postgres@localhost:5432/ai_engine"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10
    rds_ca_path: str | None = None  # RDS TLS CA 번들 경로. 로컬 PG면 비움(SSL 끔)
    redis_url: str = "redis://localhost:6379/0"

    # --- Kafka (AWS MSK / 로컬 redpanda) ---
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_ocr_job_topic: str = "ocr-job-queue"
    kafka_report_job_topic: str = "report-job"
    kafka_security_protocol: str = "PLAINTEXT"  # PLAINTEXT | SSL | SASL_SSL
    kafka_consumer_group: str = "ocr-worker"
    kafka_dlq_suffix: str = ".dlq"
    kafka_max_retries: int = 3

    # --- S3 (OCR 원본 — OCR Worker가 GetObject) ---
    aws_region: str = "ap-northeast-2"
    s3_bucket: str = ""

    # --- Notion (약관 코퍼스 소스 — jjg 인테그레이션, 공식 REST API) ---
    notion_token: str = ""  # 시크릿(ntn_...). SSM/.env 주입, 코드·커밋 금지
    # 데이터 출처 카탈로그(컨트롤 테이블: 카테고리·우선순위 P0~P3·수집단계)
    notion_catalog_data_source_id: str = "6b817446-047a-4924-abd6-4f334bbbeaa2"
    # terms(약관) 파일 DB — 카탈로그 `출처` relation의 대상
    notion_corpus_data_source_id: str = "45785f96-fd07-460c-8f90-e8dd17cb2f00"
    notion_api_version: str = "2022-06-28"  # Notion-Version 헤더 고정
    notion_sync_interval_seconds: int = 3600  # 카탈로그→PG 주기 동기화 간격
    notion_rate_limit_rps: float = 3.0  # Notion API 평균 레이트리밋 상한

    # --- Corpus S3 스테이징 (Notion 첨부 → S3, 청킹/임베딩은 범위 밖) ---
    corpus_categories: str = "terms"  # MVP 필터. 확장 시 "terms,precedent,medical,legal"
    s3_corpus_prefix: str = "corpus/"  # 키 = corpus/{category}/{sha256}.pdf (OCR 키공간과 분리)
    s3_corpus_sse: str = "AES256"  # SSE-S3(공개 약관·비-PII)
    corpus_poll_interval_seconds: float = 5.0  # 우선순위 큐 폴링 간격
    corpus_max_concurrent_uploads: int = 4  # 동시 업로드 수
    corpus_download_tmp_dir: str = "/tmp"  # 대용량 스트리밍 임시(처리 후 즉시 삭제)

    # --- 수요도 우선순위 (가중치·상수만 env; 상품종류 룩업표는 priority 모듈) ---
    corpus_w_demand: float = 0.6  # 수요도(상품종류) 가중
    corpus_w_urgency: float = 0.4  # 긴급도(시행일 신선도) 가중
    corpus_demand_halflife_days: float = 7.0  # 수요 부스트 반감기(일)
    corpus_demand_boost_cap: float = 40.0  # 수요 부스트 상한

    # --- PII 마스킹 ---
    use_ner: bool = False  # NER 디텍터 활성 여부(false면 정규식만)
    # NER 모델(로컬 실행 — 외부 API로 PII 전송 금지). 이름(PS)만 사용.
    ner_model: str = "Leo97/KoELECTRA-small-v3-modu-ner"
    # NER 확률 임계값. 마스킹은 recall 우선이라 낮게; 과잉 마스킹 시 상향(노션 §2).
    ner_score_threshold: float = 0.5

    # --- AI 서빙 (OpenAI 호환: Ollama/vLLM/TEI) — 모델 미정, env 주입 ---
    ai_base_url: str = "http://localhost:11434/v1"  # 챗 추론 엔드포인트
    ai_api_key: str = "not-needed"  # OpenAI 호환 인증(로컬은 미사용)
    llm_model: str = ""  # 예: EXAONE 계열
    embedding_base_url: str = "http://localhost:11434/v1"  # 임베딩 엔드포인트(별도 노드 가능)
    embedding_model: str = ""  # 예: qwen3:embedding (1024d)
    embedding_dim: int = DEFAULT_EMBEDDING_DIM
    ai_timeout_seconds: float = 60.0  # 추론 HTTP 요청 타임아웃


@lru_cache
def get_settings() -> Settings:
    """프로세스 단일 `Settings` 인스턴스를 반환한다(캐시).

    Returns:
        env에서 로드된 설정. 동일 인스턴스가 재사용된다.
    """
    return Settings()


# 단일 settings 인스턴스. `from core.config import settings` 또는 `get_settings()` 사용.
settings = get_settings()
