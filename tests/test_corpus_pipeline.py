"""process_document 스테이징 파이프라인 테스트 (이슈 #35 P2).

네트워크·S3·DB 없이 페이크로 격리해 검증한다: 다운로드→sha256→dedup 업로드→상태 전이,
head_exists=True면 put 스킵(dedup), 이미 업로드된 파트 스킵(재개), 실패→mark_document_failed
(attempts++), 임시파일 삭제(로컬 미잔류).
"""

import os
import tempfile
from typing import Any

import httpx

from core.config import Settings
from core.exceptions import CorpusStagingError
from corpus_worker.downloader import DownloadResult
from corpus_worker.pipeline import ProcessDeps, process_document
from corpus_worker.repository import ClaimedDoc, PartRow

PAGE = "33333333-3333-3333-3333-333333333333"
PART0 = "aaaaaaaa-0000-0000-0000-000000000000"
PART1 = "bbbbbbbb-0000-0000-0000-000000000000"
SHA0 = "0" * 64
SHA1 = "1" * 64
URL0 = "https://notion/0"
URL1 = "https://notion/1"


class FakePool:
    """asyncpg.Pool.execute 흉내 — mark_* SQL·인자를 기록한다."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, sql: str, *args: Any) -> str:
        self.executed.append((sql, args))
        return "UPDATE 1"


class FakeNotion:
    """NotionSource.file_urls 흉내 — 호출 횟수를 세어 문서당 1회 조회를 검증한다."""

    def __init__(self, urls: list[str]) -> None:
        self._urls = urls
        self.calls = 0

    async def file_urls(self, page_id: str) -> list[str]:
        self.calls += 1
        return list(self._urls)


class FakeS3:
    """CorpusS3 흉내 — 존재 집합으로 dedup을 흉내내고 put 호출을 기록한다."""

    def __init__(self, existing: set[str] | None = None) -> None:
        self.existing = set(existing or set())
        self.head_calls: list[str] = []
        self.put_calls: list[tuple[str, str, str, str]] = []

    async def head_exists(self, key: str) -> bool:
        self.head_calls.append(key)
        return key in self.existing

    async def put_file(self, temp_path: str, key: str, *, content_type: str, sse: str) -> None:
        self.put_calls.append((temp_path, key, content_type, sse))
        self.existing.add(key)


class FakeDownloader:
    """download 콜러블 흉내 — 실제 임시파일을 만들어 삭제 여부를 검증 가능하게 한다."""

    def __init__(self, tmp_dir: str, meta: dict[str, tuple[str, int]]) -> None:
        self._tmp_dir = tmp_dir
        self._meta = meta
        self.created_paths: list[str] = []
        self.suffixes: list[str] = []

    async def __call__(self, url: str, suffix: str) -> DownloadResult:
        self.suffixes.append(suffix)
        sha256, byte_size = self._meta[url]
        fd, path = tempfile.mkstemp(suffix=suffix, dir=self._tmp_dir)
        os.write(fd, b"x")
        os.close(fd)
        self.created_paths.append(path)
        return DownloadResult(temp_path=path, sha256=sha256, byte_size=byte_size)


def _settings() -> Settings:
    return Settings(s3_corpus_prefix="corpus/", s3_corpus_sse="AES256", corpus_max_attempts=5)


def _deps(pool: FakePool, notion: FakeNotion, downloader: Any, s3: FakeS3) -> ProcessDeps:
    return ProcessDeps(
        pool=pool,  # type: ignore[arg-type]
        notion_source=notion,
        download=downloader,
        s3=s3,
        settings=_settings(),
    )


def _uploaded_part_calls(pool: FakePool) -> list[tuple[str, tuple[Any, ...]]]:
    return [(sql, args) for sql, args in pool.executed if "corpus_file_part" in sql]


async def test_process_document_downloads_uploads_and_marks(tmp_path) -> None:
    # Arrange
    pool = FakePool()
    notion = FakeNotion([URL0, URL1])
    downloader = FakeDownloader(str(tmp_path), {URL0: (SHA0, 10), URL1: (SHA1, 20)})
    s3 = FakeS3()
    deps = _deps(pool, notion, downloader, s3)
    doc = ClaimedDoc(notion_page_id=PAGE, category="terms", part_total=2)
    parts = [
        PartRow(PART0, 0, "약관.pdf", None, "pending"),
        PartRow(PART1, 1, "약관.pdf", None, "pending"),
    ]

    # Act
    await process_document(deps, doc, parts)

    # Assert: 두 파트 모두 내용주소 키로 업로드(head miss)
    assert [call[1] for call in s3.put_calls] == [
        f"corpus/terms/{SHA0}.pdf",
        f"corpus/terms/{SHA1}.pdf",
    ]
    assert s3.put_calls[0][2] == "application/pdf"  # content_type
    assert s3.put_calls[0][3] == "AES256"  # sse
    assert downloader.suffixes == [".pdf", ".pdf"]  # 파일명에서 판정한 확장자가 그대로 전달
    assert len(_uploaded_part_calls(pool)) == 2
    assert any("status = 'done'" in sql for sql, _ in pool.executed)
    assert notion.calls == 1  # 신선 URL은 문서당 1회만 조회
    for path in downloader.created_paths:
        assert not os.path.exists(path)  # 임시파일 미잔류


async def test_process_document_derives_type_from_file_name(tmp_path) -> None:
    # Arrange: 약관이 전부 PDF는 아니다 — HWP 첨부는 .hwp 키·ContentType으로 업로드돼야 한다
    pool = FakePool()
    notion = FakeNotion([URL0])
    downloader = FakeDownloader(str(tmp_path), {URL0: (SHA0, 10)})
    s3 = FakeS3()
    deps = _deps(pool, notion, downloader, s3)
    doc = ClaimedDoc(notion_page_id=PAGE, category="terms", part_total=1)
    parts = [PartRow(PART0, 0, "특별약관.hwp", None, "pending")]

    # Act
    await process_document(deps, doc, parts)

    # Assert: .pdf로 하드코딩되지 않고 실제 파일명에서 판정한 타입을 씀
    assert [call[1] for call in s3.put_calls] == [f"corpus/terms/{SHA0}.hwp"]
    assert s3.put_calls[0][2] == "application/x-hwp"
    assert downloader.suffixes == [".hwp"]
    assert len(_uploaded_part_calls(pool)) == 1


async def test_process_document_fails_when_file_name_missing(tmp_path) -> None:
    # Arrange: notion_file_name이 없으면 타입을 추측하지 않고 즉시 실패한다
    pool = FakePool()
    notion = FakeNotion([URL0])
    downloader = FakeDownloader(str(tmp_path), {URL0: (SHA0, 10)})
    s3 = FakeS3()
    deps = _deps(pool, notion, downloader, s3)
    doc = ClaimedDoc(notion_page_id=PAGE, category="terms", part_total=1)
    parts = [PartRow(PART0, 0, None, None, "pending")]

    # Act
    await process_document(deps, doc, parts)

    # Assert: 다운로드·업로드는 시도조차 안 하고 문서를 실패 처리한다
    assert downloader.created_paths == []
    assert s3.put_calls == []
    assert any("attempts = attempts + 1" in sql for sql, _ in pool.executed)
    assert _uploaded_part_calls(pool) == []


async def test_process_document_skips_put_on_dedup_hit(tmp_path) -> None:
    # Arrange: 같은 내용 키가 이미 S3에 존재
    key = f"corpus/terms/{SHA0}.pdf"
    pool = FakePool()
    notion = FakeNotion([URL0])
    downloader = FakeDownloader(str(tmp_path), {URL0: (SHA0, 10)})
    s3 = FakeS3(existing={key})
    deps = _deps(pool, notion, downloader, s3)
    doc = ClaimedDoc(notion_page_id=PAGE, category="terms", part_total=1)
    parts = [PartRow(PART0, 0, "약관.pdf", None, "pending")]

    # Act
    await process_document(deps, doc, parts)

    # Assert: dedup → put 생략, 그래도 파트는 uploaded로 전이
    assert s3.put_calls == []
    assert len(_uploaded_part_calls(pool)) == 1
    for path in downloader.created_paths:
        assert not os.path.exists(path)


async def test_process_document_resumes_skipping_uploaded_parts(tmp_path) -> None:
    # Arrange: part0은 이미 uploaded → 스킵, part1만 처리
    pool = FakePool()
    notion = FakeNotion([URL0, URL1])
    downloader = FakeDownloader(str(tmp_path), {URL1: (SHA1, 20)})
    s3 = FakeS3()
    deps = _deps(pool, notion, downloader, s3)
    doc = ClaimedDoc(notion_page_id=PAGE, category="terms", part_total=2)
    parts = [
        PartRow(PART0, 0, "약관.pdf", "already", "uploaded"),
        PartRow(PART1, 1, "약관.pdf", None, "pending"),
    ]

    # Act
    await process_document(deps, doc, parts)

    # Assert: part1만 다운로드·업로드
    assert len(downloader.created_paths) == 1
    assert [call[1] for call in s3.put_calls] == [f"corpus/terms/{SHA1}.pdf"]
    assert len(_uploaded_part_calls(pool)) == 1


async def test_process_document_marks_failed_on_download_error(tmp_path) -> None:
    # Arrange: 다운로드가 httpx 오류
    class FailingDownloader:
        async def __call__(self, url: str, suffix: str) -> DownloadResult:
            raise httpx.HTTPError("boom")

    pool = FakePool()
    notion = FakeNotion([URL0])
    deps = _deps(pool, notion, FailingDownloader(), FakeS3())
    doc = ClaimedDoc(notion_page_id=PAGE, category="terms", part_total=1)
    parts = [PartRow(PART0, 0, "약관.pdf", None, "pending")]

    # Act
    await process_document(deps, doc, parts)

    # Assert: mark_document_failed(attempts++, max_attempts) 실행, done·part upload 없음
    failed = [(sql, args) for sql, args in pool.executed if "attempts = attempts + 1" in sql]
    assert failed
    assert failed[0][1][2] == _settings().corpus_max_attempts  # $3 = max_attempts
    assert not any("status = 'done'" in sql for sql, _ in pool.executed)
    assert _uploaded_part_calls(pool) == []


async def test_process_document_cleans_temp_and_fails_on_upload_error(tmp_path) -> None:
    # Arrange: head miss 후 put이 실패
    class FailingS3(FakeS3):
        async def put_file(self, temp_path: str, key: str, *, content_type: str, sse: str) -> None:
            raise CorpusStagingError("s3 down")

    pool = FakePool()
    notion = FakeNotion([URL0])
    downloader = FakeDownloader(str(tmp_path), {URL0: (SHA0, 10)})
    deps = _deps(pool, notion, downloader, FailingS3())
    doc = ClaimedDoc(notion_page_id=PAGE, category="terms", part_total=1)
    parts = [PartRow(PART0, 0, "약관.pdf", None, "pending")]

    # Act
    await process_document(deps, doc, parts)

    # Assert: 실패해도 임시파일은 정리되고 문서는 failed 반영, 파트는 미전이
    assert not os.path.exists(downloader.created_paths[0])
    assert any("attempts = attempts + 1" in sql for sql, _ in pool.executed)
    assert _uploaded_part_calls(pool) == []
