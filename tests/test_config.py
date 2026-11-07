"""config.py 환경설정 테스트."""

import math

import pytest
from pydantic import ValidationError

from core.config import (
    DEFAULT_EMBEDDING_DIM,
    OCR_NON_VLM_RESERVE_SECONDS,
    OCR_VISIBILITY_SAFETY_MARGIN_SECONDS,
    Settings,
    get_settings,
)
from ocr_worker.ocr import PDF_RENDER_DPI


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


def test_ai_provider_defaults_are_claude_and_voyage(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 코드 기본값 검증 — 개발자 셸·.env 값이 새어 들지 않게 격리
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("EMBEDDING_PROVIDER", raising=False)
    monkeypatch.delenv("EMBEDDING_API_KEY", raising=False)
    monkeypatch.delenv("EMBEDDING_BASE_URL", raising=False)
    monkeypatch.delenv("LLM_EFFORT", raising=False)
    monkeypatch.delenv("LLM_MAX_TOKENS", raising=False)
    monkeypatch.delenv("LLM_TIMEOUT_SECONDS", raising=False)

    # Act
    cfg = Settings(_env_file=None)

    # Assert: 챗 기본은 Claude, 임베딩 기본은 Voyage(코퍼스와 같은 모델 계열)
    assert cfg.llm_provider == "anthropic"
    assert cfg.embedding_provider == "voyage"
    assert cfg.embedding_base_url == "https://ai.mongodb.com/v1"
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


# ── VLM(클러스터 vLLM) 최악 경로 ──────────────────────────────────
# 시간 계산에 쓰는 생성 속도의 **보수적 하한**(토큰/초). Paddle 전환 전(qwen3-vl 시절) 청커
# 실측(Policy-Chunker 87b6821)이다 — PaddleOCR-VL-0.9B 운영 실측은 토큰 간격 0.028초(≈35.7
# tok/s, K8s-manifests charts/etl-chunker/values.yaml)라 이 하한으로 잡으면 안전 쪽이다.
_CONSERVATIVE_DECODE_TOKENS_PER_S = 18.7
_VLM_ENV_NAMES = (
    "VLM_MAX_TOKENS",
    "VLM_TIMEOUT_SECONDS",
    "VLM_CONNECT_TIMEOUT_SECONDS",
    "VLM_REREAD_MAX_TOKENS",
    "VLM_REREAD_TIMEOUT_SECONDS",
    "VLM_PAGE_BUDGET_SECONDS",
    "VLM_REREAD_BUDGET_SECONDS",
    "SQS_VISIBILITY_TIMEOUT",
)


def test_vlm_defaults_fit_worst_case_in_sqs_visibility(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: heartbeat가 없어 문서 1건이 가시성(600초) 안에 끝나야 중복 처리가 없다(config 주석).
    # 기본값 계약이라 개발자 env를 격리한다.
    for name in _VLM_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)

    # Act
    cfg = Settings(_env_file=None)

    # Assert: 단건 상한은 각 단계 예산 안에, 두 단계 예산의 합은 가시성 안에(나머지 단계 몫을
    # 빼고 — 기동 검사 ocr_worker.__main__._check_vlm_time_budget와 같은 상수).
    assert cfg.vlm_max_tokens / _CONSERVATIVE_DECODE_TOKENS_PER_S <= cfg.vlm_timeout_seconds
    assert (
        cfg.vlm_reread_max_tokens / _CONSERVATIVE_DECODE_TOKENS_PER_S
        <= cfg.vlm_reread_timeout_seconds
    )
    assert cfg.vlm_connect_timeout_seconds + cfg.vlm_timeout_seconds <= cfg.vlm_page_budget_seconds
    assert (
        cfg.vlm_connect_timeout_seconds + cfg.vlm_reread_timeout_seconds
        <= cfg.vlm_reread_budget_seconds
    )
    assert (
        cfg.vlm_page_budget_seconds
        + cfg.vlm_reread_budget_seconds
        + OCR_NON_VLM_RESERVE_SECONDS
        + OCR_VISIBILITY_SAFETY_MARGIN_SECONDS
        <= cfg.sqs_visibility_timeout
    )


def test_vlm_budget_keeps_a_nonzero_safety_margin() -> None:
    # Assert: 예산이 끝난 뒤의 정리 시간·VLM 외 몫(실측 전 가정)을 위해 여유를 0으로 두지 않는다.
    assert OCR_VISIBILITY_SAFETY_MARGIN_SECONDS > 0


def test_vlm_concurrency_must_be_positive(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 0이면 세마포어가 영원히 안 열려 VLM 단계가 예산 끝까지 멎는다 — 기동 시점에 거른다.
    monkeypatch.setenv("VLM_CONCURRENCY", "0")

    # Act / Assert
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_ocr_det_limit_default_does_not_downscale_a4_pages() -> None:
    # Arrange: A4(297mm)를 PDF_RENDER_DPI로 렌더한 긴 변. 검출 상한이 이보다 작으면 A4 쪽도 줄어
    # 단어 상자가 긴 줄로 합쳐지고 한국어 인식이 무너진다(합성 측정 53/60 → 32·42/60).
    a4_long_side_px = math.ceil(297 / 25.4 * PDF_RENDER_DPI)

    # Act
    limit = Settings(_env_file=None).ocr_det_limit_side_len

    # Assert: 상한은 켜져 있고(큰 사진 OOM 방지) A4는 그대로 둔다.
    assert limit > 0
    assert limit >= a4_long_side_px


def test_ocr_det_limit_rejects_negative(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setenv("OCR_DET_LIMIT_SIDE_LEN", "-1")

    # Act / Assert
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
