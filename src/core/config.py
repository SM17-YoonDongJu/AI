"""환경설정 단일 출처 (pydantic-settings).

모든 env 값을 한 곳에서 로드·검증한다. 모델·엔드포인트는 코드에 하드코딩하지 않고
주입한다(env). 시크릿(DB 비밀번호 등)은 코드·로그·커밋에 두지 않으며 운영에서는 env로
주입한다. 아래 기본값은 로컬 개발(docker-compose.dev) 편의를 위한 것이며 실제 값이 아니다.
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

    # --- 데이터스토어 ---
    db_dsn: str = "postgresql://postgres:postgres@localhost:5432/ai_engine"  # asyncpg DSN
    redis_url: str = "redis://localhost:6379/0"

    # --- Kafka ---
    kafka_bootstrap: str = "localhost:9092"  # bootstrap.servers

    # --- AI 추론 (OpenAI 호환 엔드포인트: Ollama/vLLM/TEI) ---
    ollama_base_url: str = "http://localhost:11434/v1"  # OpenAI 호환 base_url
    # 모델명은 미정 → 반드시 env로 주입(하드코딩 금지). 비어 있으면 ai_client 호출 시 실패.
    chat_model: str = ""  # 예: EXAONE 계열
    embedding_model: str = ""  # 예: qwen3:embedding (1024d)
    embedding_dim: int = DEFAULT_EMBEDDING_DIM
    ai_timeout_seconds: float = 60.0  # 추론 HTTP 요청 타임아웃

    # --- 관측성(로깅) ---
    # 로그 리소스 필드. service_name은 워커별로 env(SERVICE_NAME)로 덮어쓴다.
    environment: str = "local"  # local | dev | prod
    service_name: str = "ai-engine"
    instance_id: str = ""  # 인스턴스/Pod 식별자(env 주입). 비면 hostname 사용
    # 빈값이면 환경별로 결정(local=DEBUG, 그 외=INFO). 명시 시 그 값을 우선한다.
    log_level: str = ""


@lru_cache
def get_settings() -> Settings:
    """프로세스 단일 `Settings` 인스턴스를 반환한다(캐시).

    Returns:
        env에서 로드된 설정. 동일 인스턴스가 재사용된다.
    """
    return Settings()


# 단일 settings 인스턴스. `from core.config import settings` 또는 `get_settings()` 사용.
settings = get_settings()
