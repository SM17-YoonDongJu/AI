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
