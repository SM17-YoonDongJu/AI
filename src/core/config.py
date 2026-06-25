"""중앙 환경설정.

모든 환경값을 `pydantic-settings`로 한 곳에서 로드한다. `os.getenv` 산재를 금지한다
(CODE_CONVENTIONS §1·§13). 워커·모듈은 `get_settings()`로 캐시된 단일 인스턴스를 쓴다.

AI 모델·엔드포인트는 미정이므로 하드코딩하지 않고 env로만 주입한다.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """프로세스 전역 설정. env(.env) → 필드 자동 매핑(대소문자 무시)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ── 환경 ────────────────────────────────────────────────
    env: str = "local"
    log_level: str = "INFO"

    # ── Database (AWS RDS PostgreSQL / 로컬 PG) ──────────────
    database_url: str = "postgresql://postgres:postgres@localhost:5432/aiengine"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10
    # RDS는 TLS를 요구 → CA 번들 경로. 로컬 PG면 비워둠(SSL 끔).
    rds_ca_path: str | None = None

    # ── Kafka (AWS MSK / 로컬 redpanda) ──────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_ocr_job_topic: str = "ocr-job-queue"
    kafka_report_job_topic: str = "report-job"
    # 인증 모드: PLAINTEXT(MSK 9092·로컬) / SSL(9094) / SASL_SSL(9098 IAM).
    # 운영 전환 시 코드 변경 없이 env로만 바꾼다.
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_consumer_group: str = "ocr-worker"
    # 처리 실패 메시지 격리용 DLQ 토픽 접미사(원본 토픽 + 접미사).
    kafka_dlq_suffix: str = ".dlq"
    # 핸들러 일시적 실패 시 인프로세스 재시도 횟수. 초과 시 DLQ.
    kafka_max_retries: int = 3

    # ── S3 (OCR 원본 — OCR Worker가 GetObject) ───────────────
    aws_region: str = "ap-northeast-2"
    s3_bucket: str = ""

    # ── PII 마스킹 ───────────────────────────────────────────
    # NER 디텍터 활성 여부(미설치/false면 정규식 디텍터만 사용).
    use_ner: bool = False
    # NER 모델(로컬 실행 — 외부 API로 PII 전송 금지). 이름(PS)만 사용.
    ner_model: str = "Leo97/KoELECTRA-small-v3-modu-ner"
    # NER 확률 임계값. 마스킹은 recall 우선이라 낮게; 과잉 마스킹 시 상향(노션 §2).
    ner_score_threshold: float = 0.5

    # ── AI 서빙 (OpenAI 호환: Ollama/vLLM/TEI) — 모델 미정 ────
    ai_base_url: str = "http://localhost:11434/v1"
    ai_api_key: str = "not-needed"
    llm_model: str = ""
    embedding_base_url: str = "http://localhost:11434/v1"
    embedding_model: str = ""
    embedding_dim: int = 1024


@lru_cache
def get_settings() -> Settings:
    """프로세스 1회 로드되는 설정 싱글턴을 반환한다."""
    return Settings()
