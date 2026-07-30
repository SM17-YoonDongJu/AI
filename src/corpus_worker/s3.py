"""약관 첨부 S3 스테이징 (이슈 #35 P2 — ocr_worker.storage 패턴 재사용).

내용주소(content-addressed) 키(``corpus/{category}/{sha256}.pdf``)로 dedup 업로드한다:
같은 내용은 같은 키라, ``head_exists``로 존재를 확인해 중복 업로드를 건너뛴다. 업로드는
boto3 ``upload_file``(대용량 멀티파트 스트리밍)을 쓰고, 동기 SDK이므로 ``asyncio.to_thread``로
격리해 워커 이벤트 루프를 막지 않는다(§7).

boto3는 배포(ocr extra)에만 설치되므로 **lazy import**한다 — 단위 테스트는 이 객체를
페이크로 대체한다. botocore 예외는 항상-임포트 가능한 ``CorpusStagingError``로 감싸
소비자가 botocore를 import하지 않게 한다(§8).
"""

from __future__ import annotations

import asyncio
from typing import Any

from core.config import Settings, get_settings
from core.exceptions import CorpusStagingError
from core.logging import get_logger

logger = get_logger(__name__)

# HeadObject가 없는 객체에 주는 HTTP 상태(404). 내용주소 dedup의 "미존재" 신호.
_NOT_FOUND_STATUS = 404
# 약관은 전부 PDF — 키 접미사·ContentType 고정.
CORPUS_CONTENT_TYPE = "application/pdf"
_KEY_SUFFIX = ".pdf"


def corpus_key(category: str, sha256: str, *, settings: Settings | None = None) -> str:
    """내용주소 S3 키를 만든다: ``{s3_corpus_prefix}{category}/{sha256}.pdf``.

    Args:
        category: 코퍼스 카테고리(terms 등) — 키 공간 분리.
        sha256: 파트 내용 해시(hex) — dedup 앵커.
        settings: 설정. None이면 ``get_settings()``.

    Returns:
        OCR 원본 키공간과 분리된 코퍼스 스테이징 키.
    """
    settings = settings or get_settings()
    return f"{settings.s3_corpus_prefix}{category}/{sha256}{_KEY_SUFFIX}"


class CorpusS3:
    """코퍼스 스테이징용 S3 경계(HeadObject dedup + 멀티파트 upload_file).

    boto3 client는 lazy 생성하되 주입 가능하다(테스트 페이크). 프로세스 1회 생성한
    client를 재사용한다(연결 풀 재사용).
    """

    def __init__(self, *, settings: Settings | None = None, client: Any | None = None) -> None:
        """S3 경계를 구성한다.

        Args:
            settings: 설정. None이면 ``get_settings()``.
            client: boto3 S3 client. None이면 첫 사용 시 lazy 생성한다.
        """
        self._settings = settings or get_settings()
        self._client = client

    def _get_client(self) -> Any:
        """캐시된 boto3 S3 client를 반환한다(첫 호출 시 lazy 생성)."""
        if self._client is None:
            import boto3  # lazy: 배포 ocr extra에만 설치

            self._client = boto3.client("s3", region_name=self._settings.aws_region)
        return self._client

    def _bucket(self) -> str:
        """설정된 버킷명을 반환한다(미설정이면 명확한 예외)."""
        bucket = self._settings.s3_bucket
        if not bucket:
            raise CorpusStagingError("S3 버킷이 설정되지 않았습니다(S3_BUCKET).")
        return bucket

    async def head_exists(self, key: str) -> bool:
        """객체 존재를 확인한다(내용주소 dedup). 없으면 False.

        Args:
            key: 확인할 S3 키.

        Returns:
            객체가 있으면 True, 404면 False.

        Raises:
            CorpusStagingError: 버킷 미설정 또는 404 외 S3 오류.
        """
        return await asyncio.to_thread(self._head_exists_sync, key)

    def _head_exists_sync(self, key: str) -> bool:
        """HeadObject(동기). 404는 False로, 그 외 오류는 예외로 변환한다."""
        from botocore.exceptions import BotoCoreError, ClientError  # lazy

        bucket = self._bucket()
        try:
            self._get_client().head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if _client_error_status(exc) == _NOT_FOUND_STATUS:
                return False
            raise CorpusStagingError(f"S3 HeadObject 실패: {key}") from exc
        except BotoCoreError as exc:
            raise CorpusStagingError(f"S3 HeadObject 실패: {key}") from exc

    async def put_file(self, temp_path: str, key: str, *, content_type: str, sse: str) -> None:
        """임시파일을 멀티파트 스트리밍으로 업로드한다(비동기 — 스레드 격리).

        Args:
            temp_path: 업로드할 로컬 임시파일 경로.
            key: 대상 S3 키(내용주소).
            content_type: MIME 타입(예: ``application/pdf``).
            sse: 서버측 암호화(예: ``AES256``).

        Raises:
            CorpusStagingError: 버킷 미설정 또는 업로드 실패.
        """
        await asyncio.to_thread(self._put_file_sync, temp_path, key, content_type, sse)

    def _put_file_sync(self, temp_path: str, key: str, content_type: str, sse: str) -> None:
        """upload_file(동기 · 멀티파트). ExtraArgs로 ContentType·SSE를 지정한다."""
        from botocore.exceptions import BotoCoreError, ClientError  # lazy

        bucket = self._bucket()
        extra_args = {"ContentType": content_type, "ServerSideEncryption": sse}
        try:
            self._get_client().upload_file(temp_path, bucket, key, ExtraArgs=extra_args)
        except (BotoCoreError, ClientError) as exc:
            raise CorpusStagingError(f"S3 업로드 실패: {key}") from exc
        logger.info("corpus_s3_put_done", key=key)

    async def aclose(self) -> None:
        """boto3 client 연결을 정리한다(워커 종료 시). 미생성이면 no-op."""
        client = self._client
        if client is None:
            return
        close = getattr(client, "close", None)
        if close is not None:
            await asyncio.to_thread(close)


def _client_error_status(exc: Any) -> int | None:
    """botocore ClientError에서 HTTP 상태 코드를 안전하게 뽑는다(없으면 None)."""
    response = getattr(exc, "response", None) or {}
    metadata = response.get("ResponseMetadata") or {}
    status = metadata.get("HTTPStatusCode")
    return int(status) if isinstance(status, int) else None
