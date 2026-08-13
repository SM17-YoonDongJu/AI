"""corpus_worker.filetype.detect 테스트 — PDF 전용 정책.

corpus_worker는 PDF만 코퍼스에 올린다. PDF가 아닌 첨부(HWP·zip·txt·법령 md·판례
json 등)와 파일명 없음은 전부 실패로 처리해 재시도/수동 검토로 넘기는지 검증한다.
"""

import pytest

from core.exceptions import CorpusSyncError
from corpus_worker.filetype import FileType, detect


def test_detect_accepts_pdf() -> None:
    assert detect("표준약관.pdf") == FileType(ext=".pdf", content_type="application/pdf")


def test_detect_is_case_insensitive() -> None:
    assert detect("약관.PDF") == FileType(ext=".pdf", content_type="application/pdf")


def test_detect_raises_when_file_name_missing() -> None:
    with pytest.raises(CorpusSyncError):
        detect(None)
    with pytest.raises(CorpusSyncError):
        detect("")


def test_detect_raises_on_non_pdf_attachments() -> None:
    # 코퍼스는 PDF만 허용 — HWP·zip·txt·법령 md·판례 json 등은 전부 거부한다
    for name in [
        "특별약관.hwp",
        "특별약관.hwp.zip",
        "안내문.txt",
        "01_insurance-business-act.md",
        "labels.json",
        "확장자없음",
    ]:
        with pytest.raises(CorpusSyncError):
            detect(name)
