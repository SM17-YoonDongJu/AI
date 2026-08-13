"""약관 첨부 파일명 → 타입 검증 (PDF 전용 정책).

corpus_worker는 PDF만 코퍼스에 올린다. Notion 첨부가 실제로는 HWP·zip·txt·법령 md·
판례 메타 json 등 다양하더라도, PDF가 아닌 첨부는 스테이징하지 않는다 — 파일명이
``.pdf``로 끝나지 않으면 추측 없이 즉시 실패시켜(§8) ``mark_document_failed``로
재시도/수동 검토에 넘긴다.
"""

from __future__ import annotations

from dataclasses import dataclass

from core.exceptions import CorpusSyncError

_PDF_EXT = ".pdf"
_PDF_CONTENT_TYPE = "application/pdf"


@dataclass(slots=True, frozen=True)
class FileType:
    """판정된 파일 타입(S3 키 접미사·ContentType)."""

    ext: str
    content_type: str


def detect(notion_file_name: str | None) -> FileType:
    """Notion 첨부 파일명이 PDF인지 확인한다(코퍼스는 PDF만 허용).

    Args:
        notion_file_name: Notion ``files`` 첨부의 원본 파일명.

    Returns:
        PDF일 때만 ``FileType(ext=".pdf", content_type="application/pdf")``.

    Raises:
        CorpusSyncError: 파일명이 없거나 ``.pdf``로 끝나지 않는 경우.
    """
    if not notion_file_name:
        raise CorpusSyncError("첨부 파일명 없음 — 타입 판정 불가")
    if not notion_file_name.lower().endswith(_PDF_EXT):
        raise CorpusSyncError(f"PDF가 아닌 첨부(코퍼스는 PDF만 허용): {notion_file_name}")
    return FileType(ext=_PDF_EXT, content_type=_PDF_CONTENT_TYPE)
