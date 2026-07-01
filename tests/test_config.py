"""config.py 환경설정 테스트."""

import pytest

from core.config import DEFAULT_EMBEDDING_DIM, Settings, get_settings


def test_embedding_dim_default_is_contract_value() -> None:
    # Arrange / Act
    cfg = Settings()

    # Assert: 임베딩 차원은 계약 고정값 1024
    assert cfg.embedding_dim == DEFAULT_EMBEDDING_DIM == 1024


def test_model_names_default_empty_to_force_injection() -> None:
    # Arrange / Act
    cfg = Settings()

    # Assert: 모델 미정 → env 주입 강제(하드코딩 금지)
    assert cfg.llm_model == ""
    assert cfg.embedding_model == ""


def test_env_overrides_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setenv("LLM_MODEL", "exaone-test")
    monkeypatch.setenv("EMBEDDING_DIM", "512")

    # Act
    cfg = Settings()

    # Assert
    assert cfg.llm_model == "exaone-test"
    assert cfg.embedding_dim == 512


def test_get_settings_returns_singleton() -> None:
    # Arrange / Act / Assert
    assert get_settings() is get_settings()
