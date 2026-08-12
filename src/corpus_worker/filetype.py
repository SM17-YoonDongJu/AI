"""약관 첨부 파일명 → 확장자·ContentType 판정 (이슈 #35 하드코딩 제거).

Notion 첨부는 전부 PDF가 아니다(hwp·zip·txt 등 혼재) — 파일명에서 정직하게 판정해
S3 키 접미사·ContentType에 반영한다. 확장자를 모르면 추측하지 않고 ``CorpusSyncError``로
실패시켜(§8) ``mark_document_failed`` 재시도·수동 검토에 맡긴다.
"""

from __future__ import annotations

from dataclasses import dataclass

from core.exceptions import CorpusSyncError

# 압축 컨테이너 등 복합 확장자는 마지막 조각만 보면 내용물 정보가 사라진다
# (예: ".hwp.zip"을 ".zip"으로만 보면 "zip 안이 HWP"라는 사실이 소실) — 단순 확장자보다
# 먼저 매칭해야 한다.
_COMPOUND_EXTENSIONS: dict[str, str] = {
    ".hwp.zip": "application/zip",
}

_SIMPLE_EXTENSIONS: dict[str, str] = {
    ".pdf": "application/pdf",
    ".hwp": "application/x-hwp",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".txt": "text/plain",
    ".zip": "application/zip",
}


@dataclass(slots=True, frozen=True)
class FileType:
    """판정된 파일 타입(S3 키 접미사·ContentType)."""

    ext: str
    content_type: str


def detect(notion_file_name: str | None) -> FileType:
    """Notion 첨부 파일명에서 확장자·ContentType을 판정한다.

    Args:
        notion_file_name: Notion ``files`` 첨부의 원본 파일명.

    Returns:
        판정된 ``FileType``(예: ``.hwp.zip`` → ``application/zip``).

    Raises:
        CorpusSyncError: 파일명이 없거나 알려진 확장자로 끝나지 않는 경우 — 타입을
            추측해 잘못 라벨링하지 않는다.
    """
    if not notion_file_name:
        raise CorpusSyncError("첨부 파일명 없음 — 타입 판정 불가")
    lowered = notion_file_name.lower()
    for ext, content_type in _COMPOUND_EXTENSIONS.items():
        if lowered.endswith(ext):
            return FileType(ext=ext, content_type=content_type)
    for ext, content_type in _SIMPLE_EXTENSIONS.items():
        if lowered.endswith(ext):
            return FileType(ext=ext, content_type=content_type)
    raise CorpusSyncError(f"알 수 없는 첨부 확장자: {notion_file_name}")
