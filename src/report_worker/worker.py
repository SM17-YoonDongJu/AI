"""report-job 핸들러 — ReportJob를 LangGraph 그래프에 태워 리포트 초안을 생성한다.

KafkaConsumer가 소비·검증·재시도·DLQ·오프셋 커밋을 담당하므로 여기서는 (1) 그래프 실행,
(2) 하드 실패 시 예외를 올려 재시도/DLQ를 유도하는 일만 한다. 처리는 report_id로 멱등하다
(persist가 report_drafts를 ON CONFLICT(report_id) 업서트).
"""

from __future__ import annotations

from core.contracts import ReportJob
from core.logging import bind_context, clear_context, get_logger
from report_worker.graph import build_graph

logger = get_logger(__name__)

# 하드 실패(리포트 미저장)로 보아 재시도/DLQ를 유도할 노드 실패 마커 prefix.
# safe_node는 "{fn}_failed:..." 형태로, load_context는 "ocr_result_missing"도 errors에 남긴다.
# 컨텍스트 조립 실패나 저장 실패는 메시지를 잃지 않도록 raise로 승격한다. 그 외(rag_empty·
# input_blocked·*_llm_failed 등)는 부분결과로 커밋한다(이슈 #11 방침).
# persist_blocked 실패("persist_blocked_failed")는 여기에 넣지 않는다 — 차단 기록은 초안 없이
# status만 갱신하는 부수효과라, 실패해도 메시지 유실이 아니라 status 미기록일 뿐이므로 하드
# 실패로 승격하지 않는다(재처리해도 동일 차단 → 무한 재시도 방지).
_HARD_FAILURE_PREFIXES = ("load_context_failed", "persist_failed", "ocr_result_missing")

# 그래프는 DB에 접촉하지 않고 조립되므로 import 시 1회 컴파일해 재사용한다.
_graph = build_graph()


class ReportWorkerError(RuntimeError):
    """리포트 생성 하드 실패 — KafkaConsumer가 재시도/DLQ 처리하도록 전파한다."""


async def handle_job(job: ReportJob) -> None:
    """단일 report-job 처리. 성공 시 조용히 반환(→커밋), 하드 실패 시 raise(→재시도/DLQ)."""
    bind_context(report_id=job.report_id, job_id=job.job_id)
    try:
        state = {
            "report_id": job.report_id,
            "ocr_result_id": job.ocr_result_id,
            "claim_id": job.claim_id,
            "user_ref": job.user_ref,
            "doc_type": job.doc_type.value,
        }
        final = await _graph.ainvoke(state)
        errors = final.get("errors", [])
        hard = [e for e in errors if str(e).startswith(_HARD_FAILURE_PREFIXES)]
        if hard:
            raise ReportWorkerError(f"리포트 생성 하드 실패: {hard}")
        logger.info("report generated", report_id=job.report_id, error_count=len(errors))
    finally:
        clear_context()
