"""S3 원본 다운로드 (이슈 #16).

OCR 작업의 원본 파일을 S3에서 **메모리로** 내려받는다. 로컬 디스크에 잔류시키지
않는다(CODE_CONVENTIONS §13 — 원본은 S3에만). boto3는 동기 SDK이므로
``asyncio.to_thread``로 격리해 워커 이벤트 루프를 막지 않는다(§7).

boto3는 배포(ocr extra)에서만 설치되므로 **lazy import**한다 — 로컬 CPU 개발
환경엔 미설치이며, 단위 테스트는 이 모듈을 페이크로 대체한다.
"""

import asyncio
from typing import Any

from core.config import get_settings
from core.exceptions import OcrError
from core.logging import get_logger

logger = get_logger(__name__)

# 프로세스 1회 생성해 재사용하는 boto3 S3 클라이언트(연결 풀 재사용).
_s3_client: Any | None = None


def _client() -> Any:
    """캐시된 boto3 S3 클라이언트를 반환한다(첫 호출 시 lazy 생성)."""
    global _s3_client
    if _s3_client is None:
        import boto3  # lazy: 배포 ocr extra에만 설치

        settings = get_settings()
        _s3_client = boto3.client("s3", region_name=settings.aws_region)
    return _s3_client


def _get_object_sync(s3_key: str) -> bytes:
    """S3 GetObject(동기). 본문 전체를 바이트로 읽어 반환한다.

    Raises:
        OcrError: 버킷 미설정 또는 S3 호출 실패(원인 예외를 체이닝).
    """
    from botocore.exceptions import BotoCoreError, ClientError  # lazy

    settings = get_settings()
    if not settings.s3_bucket:
        raise OcrError("S3 버킷이 설정되지 않았습니다(S3_BUCKET).")
    try:
        response = _client().get_object(Bucket=settings.s3_bucket, Key=s3_key)
        return response["Body"].read()  # type: ignore[no-any-return]
    except (BotoCoreError, ClientError) as exc:
        # s3_key는 UUID 치환된 내부 키(PII 아님) → 로깅 허용.
        raise OcrError(f"S3 다운로드 실패: {s3_key}") from exc


async def get_object(s3_key: str) -> bytes:
    """S3에서 원본 파일을 메모리로 내려받는다(비동기 — 블로킹 SDK를 스레드 격리).

    Args:
        s3_key: 업로드 파일의 S3 키(파일명은 UUID로 치환된 내부 식별자).

    Returns:
        파일 원본 바이트.

    Raises:
        OcrError: 버킷 미설정 또는 S3 호출 실패.
    """
    return await asyncio.to_thread(_get_object_sync, s3_key)


def _put_object_sync(s3_key: str, data: bytes, content_type: str) -> None:
    """S3 PutObject(동기). 비식별 이미지 사본 등 가공 산출물을 올린다.

    Raises:
        OcrError: 버킷 미설정 또는 S3 호출 실패(원인 예외를 체이닝).
    """
    from botocore.exceptions import BotoCoreError, ClientError  # lazy

    settings = get_settings()
    if not settings.s3_bucket:
        raise OcrError("S3 버킷이 설정되지 않았습니다(S3_BUCKET).")
    try:
        _client().put_object(
            Bucket=settings.s3_bucket, Key=s3_key, Body=data, ContentType=content_type
        )
    except (BotoCoreError, ClientError) as exc:
        raise OcrError(f"S3 업로드 실패: {s3_key}") from exc


async def put_object(s3_key: str, data: bytes, content_type: str) -> None:
    """가공 산출물(비식별 이미지 사본 등)을 S3에 올린다(비동기 — 스레드 격리).

    원본 키와 분리된 키 공간(예: ``masked/<job_id>/page-N.png``)에 저장한다 —
    키 네이밍·``ocr_results`` 적재는 호출자(파이프라인 #15)가 정한다.

    Args:
        s3_key: 저장할 S3 키.
        data: 파일 바이트(예: PNG).
        content_type: MIME 타입(예: ``image/png``).

    Raises:
        OcrError: 버킷 미설정 또는 S3 호출 실패.
    """
    await asyncio.to_thread(_put_object_sync, s3_key, data, content_type)
