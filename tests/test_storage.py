"""S3 경계 단위 테스트 — 원본 삭제(``delete_object``)·사본 업로드(``put_object``, #18).

원본 삭제는 **되돌릴 수 없는** 유일한 S3 연산이라, "어떤 버킷/키로 부르는가"와
"실패를 어떻게 도메인 예외로 바꾸는가"를 테스트로 고정한다. 업로드는 ``masked/`` 사본
전용이라 서버측 암호화(SSE-KMS)가 **항상** 걸리는지를 고정한다 — 마스킹 검증이 실패하면
사본에 PII가 남을 수 있어(방어심층) 조건부로 켜서는 안 된다.

boto3/botocore는 배포(ocr extra)에만 설치돼 dev 환경엔 없다. 그래서 클라이언트는
모듈 전역 캐시를 페이크로 갈아끼우고, lazy import 대상인 ``botocore.exceptions``만
최소 스텁으로 심는다(잡는 예외 타입이 실제로 존재해야 실패 매핑을 검증할 수 있다).
다른 S3 경계처럼 소비자 쪽에서 통째로 페이크하는 방식은 ``tests/test_pipeline.py``의
삭제 게이트 테스트가 담당한다.
"""

import sys
import types
from collections.abc import Iterator
from typing import Any

import pytest

from core.config import Settings
from core.exceptions import OcrError
from ocr_worker import storage


class _StubBotoCoreError(Exception):
    """botocore.exceptions.BotoCoreError 스텁."""


class _StubClientError(Exception):
    """botocore.exceptions.ClientError 스텁."""


class _FakeS3Client:
    """boto3 S3 client 대역 — delete/put 인자를 기록하고 지정 예외를 낸다."""

    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.put_calls: list[dict[str, Any]] = []
        self._error = error

    def delete_object(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error

    def put_object(self, **kwargs: Any) -> None:
        self.put_calls.append(kwargs)
        if self._error is not None:
            raise self._error


@pytest.fixture(autouse=True)
def _botocore_stub() -> Iterator[None]:
    """``from botocore.exceptions import ...``가 성공하도록 최소 모듈을 심는다."""
    module = types.ModuleType("botocore.exceptions")
    module.BotoCoreError = _StubBotoCoreError  # type: ignore[attr-defined]
    module.ClientError = _StubClientError  # type: ignore[attr-defined]
    sys.modules["botocore.exceptions"] = module
    try:
        yield
    finally:
        del sys.modules["botocore.exceptions"]


def _use_client(
    monkeypatch: pytest.MonkeyPatch, client: Any, *, bucket: str = "ai-engine-docs"
) -> None:
    """캐시된 S3 클라이언트와 설정을 페이크로 대체한다(boto3 미설치 환경용)."""
    monkeypatch.setattr(storage, "_s3_client", client)
    monkeypatch.setattr(storage, "get_settings", lambda: Settings(s3_bucket=bucket))


async def test_delete_object_calls_s3_with_bucket_and_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    client = _FakeS3Client()
    _use_client(monkeypatch, client)

    # Act
    await storage.delete_object("uploads/abc.pdf")

    # Assert
    assert client.calls == [{"Bucket": "ai-engine-docs", "Key": "uploads/abc.pdf"}]


async def test_delete_object_wraps_client_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: botocore 예외는 호출자(pipeline)가 botocore를 몰라도 되도록 OcrError로 감싼다.
    _use_client(monkeypatch, _FakeS3Client(error=_StubClientError("AccessDenied")))

    # Act / Assert
    with pytest.raises(OcrError, match="S3 삭제 실패"):
        await storage.delete_object("uploads/abc.pdf")


async def test_delete_object_requires_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 버킷 미설정이면 S3를 아예 부르지 않는다(빈 버킷으로 호출 금지).
    client = _FakeS3Client()
    _use_client(monkeypatch, client, bucket="")

    # Act / Assert
    with pytest.raises(OcrError, match="S3 버킷"):
        await storage.delete_object("uploads/abc.pdf")
    assert client.calls == []


async def test_put_object_encrypts_masked_copy_with_sse_kms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: put_object는 masked/ 사본 전용 경로다(원본 다운로드·삭제와 무관).
    client = _FakeS3Client()
    _use_client(monkeypatch, client)

    # Act
    await storage.put_object("masked/job-1/page-0.png", b"png-bytes", "image/png")

    # Assert: 버킷/키/본문/타입 + **항상** SSE-KMS. SSEKMSKeyId는 주지 않는다 —
    # 생략 시 AWS 관리형 기본 키(aws/s3)가 쓰여 별도 키 프로비저닝·IAM이 필요 없다.
    assert client.put_calls == [
        {
            "Bucket": "ai-engine-docs",
            "Key": "masked/job-1/page-0.png",
            "Body": b"png-bytes",
            "ContentType": "image/png",
            "ServerSideEncryption": "aws:kms",
        }
    ]
    assert "SSEKMSKeyId" not in client.put_calls[0]


async def test_put_object_wraps_client_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    _use_client(monkeypatch, _FakeS3Client(error=_StubClientError("AccessDenied")))

    # Act / Assert
    with pytest.raises(OcrError, match="S3 업로드 실패"):
        await storage.put_object("masked/job-1/page-0.png", b"png", "image/png")


async def test_put_object_requires_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    client = _FakeS3Client()
    _use_client(monkeypatch, client, bucket="")

    # Act / Assert
    with pytest.raises(OcrError, match="S3 버킷"):
        await storage.put_object("masked/job-1/page-0.png", b"png", "image/png")
    assert client.put_calls == []
