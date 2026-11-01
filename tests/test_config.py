"""config.py 환경설정 테스트."""

import pytest
from pydantic import ValidationError

from core.config import DEFAULT_EMBEDDING_DIM, Settings, get_settings


def test_embedding_dim_default_is_contract_value() -> None:
    # Arrange / Act
    cfg = Settings()

    # Assert: 임베딩 차원은 계약 고정값 1024
    assert cfg.embedding_dim == DEFAULT_EMBEDDING_DIM == 1024


def test_model_names_default_empty_to_force_injection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 로컬 .env·프로세스 env 격리 — "기본값이 비어 있다"는 코드 기본값 검증이므로
    # 개발자 로컬의 LLM_MODEL 값이 새어 들면 CI에선 통과하고 로컬에선 상시 실패한다.
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.delenv("EMBEDDING_MODEL", raising=False)

    # Act
    cfg = Settings(_env_file=None)

    # Assert: 모델 미정 → env 주입 강제(하드코딩 금지)
    assert cfg.llm_model == ""
    assert cfg.embedding_model == ""


def test_env_overrides_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setenv("LLM_MODEL", "qwen-test")
    monkeypatch.setenv("EMBEDDING_DIM", "512")

    # Act
    cfg = Settings()

    # Assert
    assert cfg.llm_model == "qwen-test"
    assert cfg.embedding_dim == 512


def test_get_settings_returns_singleton() -> None:
    # Arrange / Act / Assert
    assert get_settings() is get_settings()


def test_environment_accepts_env_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 배포 템플릿(.env.*.example·docker-compose*.yml)은 전부 ENV를 주입한다.
    # 별칭이 없으면 이 필드가 항상 기본값(local)에 고정되고, core.crypto.get_pii_dek()가
    # 그 값으로 dev(평문 env 키)/prod(KMS)를 가르므로 이건 보안 버그다.
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setenv("ENV", "prod")

    # Act
    cfg = Settings(_env_file=None)

    # Assert
    assert cfg.environment == "prod"


def test_environment_still_accepts_full_name(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: ENV 별칭 추가가 기존 ENVIRONMENT 변수명을 깨면 안 된다.
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "prod")

    # Act
    cfg = Settings(_env_file=None)

    # Assert
    assert cfg.environment == "prod"


def test_ai_provider_defaults_keep_local_path(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 코드 기본값 검증 — 개발자 셸·.env 값이 새어 들지 않게 격리
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("EMBEDDING_PROVIDER", raising=False)
    monkeypatch.delenv("EMBEDDING_API_KEY", raising=False)
    monkeypatch.delenv("LLM_EFFORT", raising=False)
    monkeypatch.delenv("LLM_MAX_TOKENS", raising=False)
    monkeypatch.delenv("LLM_TIMEOUT_SECONDS", raising=False)

    # Act
    cfg = Settings(_env_file=None)

    # Assert: 챗 기본은 Claude. 임베딩은 재임베딩 전이라 로컬 기본을 유지한다
    assert cfg.llm_provider == "anthropic"
    assert cfg.embedding_provider == "openai_compat"
    assert cfg.embedding_api_key == "not-needed"
    assert cfg.llm_effort == "high"
    assert cfg.llm_max_tokens == 64000
    assert cfg.llm_timeout_seconds == 600.0


def test_ai_provider_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("LLM_EFFORT", "xhigh")
    monkeypatch.setenv("EMBEDDING_PROVIDER", "voyage")
    monkeypatch.setenv("EMBEDDING_API_KEY", "voyage-test-key")

    # Act
    cfg = Settings(_env_file=None)

    # Assert
    assert cfg.llm_provider == "anthropic"
    assert cfg.llm_effort == "xhigh"
    assert cfg.embedding_provider == "voyage"
    assert cfg.embedding_api_key == "voyage-test-key"


@pytest.mark.parametrize(
    ("env_name", "value"),
    [("LLM_PROVIDER", "openai"), ("EMBEDDING_PROVIDER", "cohere"), ("LLM_EFFORT", "extreme")],
)
def test_ai_provider_rejects_unknown_value(
    monkeypatch: pytest.MonkeyPatch, env_name: str, value: str
) -> None:
    # Arrange: 오타 provider로 조용히 기본 경로에 떨어지지 않고 기동 시점에 실패해야 한다
    monkeypatch.setenv(env_name, value)

    # Act / Assert
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
