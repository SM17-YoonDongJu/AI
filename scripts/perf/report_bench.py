"""리포트 그래프 벤치 — 합성 시나리오를 운영 그래프로 돌려 노드·LLM 호출별 시간과 토큰을 잰다.

DB 입력(load_context)은 ``run_experiments.py``의 합성 시나리오로, 저장(persist*)은 아무것도 쓰지
않는 대역으로 바꾼다. 나머지 노드 — 진단·RAG(dev 코퍼스, 읽기 전용)·판례·장해·지급·작성·가드레일·
Judge·수치 — 는 운영 코드 그대로 Claude를 부른다. **Claude 호출은 과금된다**: 기본 상한
``--max-reports 1``(파일럿)이고, 더 돌리려면 상한을 명시해야 한다.

실제 사용자 데이터는 쓰지 않는다 — 합성 시나리오와 공개 약관·판례 청크만 외부 API로 나간다.

전제:
  - dev RDS로 가는 SSM 터널: localhost:<--db-port>(기본 15432) → brbs-rds-dev:5432
  - AWS 프로필(기본 brbs)로 Secrets Manager ``app/report-dev``를 읽을 수 있다(DB 접속·Voyage 키).
    값은 이 프로세스 환경에만 넣고 출력·저장하지 않는다
  - Claude 키는 ``--claude-key-file``(권한 600 파일)에서 읽는다 — dev 시크릿의 키는 쓰지 않는다

사용법:
    uv run --extra report python scripts/perf/report_bench.py out/perf-report \\
        --claude-key-file ~/.config/brbs/claude-bench.key --label pilot --scenarios A
    # 조건당 20건(4시나리오 x 5회)
    ... --label baseline --reps 5 --max-reports 20

결과(``<out>/<label>/``): events.jsonl(계측 이벤트) · runs.json(실행별 요약) · summary.json ·
meta.json.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import ssl
import sys
import time
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# 합성 시나리오 — run_experiments.py와 같은 입력(마스킹된 진단서 텍스트·질문은 합성값).
_COVERAGES = ["상해후유장해", "상해입원일당", "수술비특약", "골절진단비"]
SCENARIOS: dict[str, dict[str, Any]] = {
    "A": {
        "name": "십자인대 파열",
        "insurer": "메리츠화재",
        "product": "다모아상해보험",
        "enrolled": date(2022, 5, 1),
        "offered": 1200000,
        "accident_type": "disability",
        "claim_dx": "우측 슬관절 전방십자인대 파열",
        "masked_text": (
            "진단서\n진단명: 우측 슬관절 전방십자인대 파열, 반월상연골 파열\n"
            "상병코드: S83.5\n수술명: 관절경적 전방십자인대 재건술\n"
            "입원: 14일\n향후 후유장해 평가 필요 소견."
        ),
        "entities": {"icd": "S83.5", "surgery": True, "admission_days": 14},
        "question": "보험금이 적게 나온 것 같아요. 후유장해나 누락 특약 확인해주세요.",
    },
    "B": {
        "name": "대퇴골 골절",
        "insurer": "메리츠화재",
        "product": "다모아상해보험",
        "enrolled": date(2022, 5, 1),
        "offered": 2000000,
        "accident_type": "disability",
        "claim_dx": "우측 대퇴골 골절",
        "masked_text": (
            "진단서\n진단명: 우측 대퇴골 골절(폐쇄성)\n상병코드: S72.0\n"
            "수술명: 관혈적 정복술 및 금속내고정술\n입원: 21일\n"
            "골유합 지연 시 후유장해 평가 필요."
        ),
        "entities": {"icd": "S72.0", "surgery": True, "admission_days": 21},
        "question": "골절로 수술받고 21일 입원했는데 받을 수 있는 특약이 뭔가요?",
    },
    "C": {
        "name": "커버리지 없는 보험사",
        "insurer": "가상손해보험",
        "product": "없는상해보험",
        "enrolled": date(2021, 3, 1),
        "offered": 800000,
        "accident_type": "disability",
        "claim_dx": "좌측 손목 골절",
        "masked_text": (
            "진단서\n진단명: 좌측 요골 원위부 골절\n상병코드: S52.5\n"
            "처치: 도수정복 및 석고고정\n입원: 5일."
        ),
        "entities": {"icd": "S52.5", "surgery": False, "admission_days": 5},
        "question": "제 약관이 조회가 안 되는데 보상 가능한가요?",
    },
    "D": {
        "name": "발목 염좌(경미)",
        "insurer": "메리츠화재",
        "product": "다모아상해보험",
        "enrolled": date(2022, 5, 1),
        "offered": 100000,
        "accident_type": "other",
        "claim_dx": "우측 발목 염좌",
        "masked_text": (
            "진단서\n진단명: 우측 발목 염좌(경도)\n상병코드: S93.4\n"
            "처치: 통원 물리치료 2주\n수술·입원 없음. 후유장해 없음."
        ),
        "entities": {"icd": "S93.4", "surgery": False, "admission_days": 0},
        "question": "발목 삐어서 통원치료만 했는데 받을 게 있나요?",
    },
}

# dev 리포트 워커와 같은 모델 설정(K8s-manifests envs/dev/report.values.yaml).
_DEV_REPORT_ENV = {
    "ENV": "dev",
    "LOG_LEVEL": "INFO",
    "LLM_PROVIDER": "anthropic",
    "LLM_MODEL": "claude-opus-5-5",
    "LLM_EFFORT": "high",
    "EMBEDDING_PROVIDER": "voyage",
    "EMBEDDING_BASE_URL": "https://ai.mongodb.com/v1",
    "EMBEDDING_MODEL": "voyage-4-large",
    "EMBEDDING_DIM": "1024",
    "USE_NER": "false",
}
_RDS_CA_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
_RDS_CA_CACHE = Path.home() / ".cache" / "brbs" / "rds-global-bundle.pem"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out", type=Path, help="결과 상위 디렉터리")
    parser.add_argument("--claude-key-file", type=Path, help="Claude API 키 파일(측정 시 필수)")
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Claude 없이 DB 터널·TLS·읽기 전용·Voyage 임베딩·RAG 검색만 확인한다",
    )
    parser.add_argument("--label", default="pilot", help="조건 이름(결과 하위 디렉터리)")
    parser.add_argument("--scenarios", default="A,B,C,D", help="쉼표 구분 시나리오(A~D)")
    parser.add_argument("--reps", type=int, default=1, help="시나리오별 반복 수")
    parser.add_argument("--max-reports", type=int, default=1, help="과금 상한 — 리포트 수")
    parser.add_argument("--db-port", type=int, default=15432, help="dev RDS SSM 터널 로컬 포트")
    parser.add_argument(
        "--report-timeout",
        type=float,
        default=900.0,
        help="리포트 1건 상한(초). 넘기면 중단하고 timeout으로 기록한 뒤 다음 건으로 간다",
    )
    parser.add_argument("--aws-profile", default="brbs")
    parser.add_argument("--secret-id", default="app/report-dev")
    return parser.parse_args()


def _prepare_env(args: argparse.Namespace) -> None:
    """설정 import 전에 환경을 만든다. 비밀값은 환경에만 넣고 출력하지 않는다."""
    import boto3

    key = "not-needed"
    if not args.preflight:
        if args.claude_key_file is None:
            raise SystemExit("--claude-key-file이 필요하다(--preflight만 키 없이 돈다)")
        key = args.claude_key_file.expanduser().read_text(encoding="utf-8").strip()
        if not key.startswith("sk-ant-"):
            raise SystemExit("Claude API 키 형식이 아니다(sk-ant-로 시작해야 한다) — 키 파일 확인")
    secret = json.loads(
        boto3.Session(profile_name=args.aws_profile)
        .client("secretsmanager", region_name="ap-northeast-2")
        .get_secret_value(SecretId=args.secret_id)["SecretString"]
    )
    # dev RDS를 SSM 터널(localhost)로 붙고, 세션을 읽기 전용으로 연다(벤치는 아무것도 쓰지 않는다).
    url = urlsplit(secret["DATABASE_URL"])
    netloc = f"{url.username}:{url.password}@localhost:{args.db_port}"
    # 원래 options(search_path 등)는 그대로 살리고 읽기 전용 세션 설정만 덧붙인다.
    params = dict(parse_qsl(url.query))
    params["options"] = f"{params.get('options', '')} -cdefault_transaction_read_only=on".strip()
    query = urlencode(params)
    os.environ.update(_DEV_REPORT_ENV)
    os.environ["DATABASE_URL"] = urlunsplit((url.scheme, netloc, url.path, query, ""))
    os.environ["EMBEDDING_API_KEY"] = secret["EMBEDDING_API_KEY"]
    os.environ["AI_API_KEY"] = key
    os.environ["RDS_CA_PATH"] = str(_ensure_rds_ca())


def _ensure_rds_ca() -> Path:
    """RDS 공개 CA 번들을 캐시에 받아 둔다(공개 트러스트스토어)."""
    if not _RDS_CA_CACHE.exists():
        import httpx

        _RDS_CA_CACHE.parent.mkdir(parents=True, exist_ok=True)
        _RDS_CA_CACHE.write_bytes(httpx.get(_RDS_CA_URL, timeout=30).raise_for_status().content)
    return _RDS_CA_CACHE


def _tunnel_ssl(ca_path: str | None) -> ssl.SSLContext | bool:
    """SSM 터널(localhost)용 TLS — CA는 검증하되 호스트명은 RDS 엔드포인트라 대조하지 않는다."""
    if not ca_path:
        return False
    context = ssl.create_default_context(cafile=ca_path)
    context.check_hostname = False
    return context


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    args = _parse_args()
    picked = [s.strip().upper() for s in args.scenarios.split(",") if s.strip()]
    unknown = sorted(set(picked) - set(SCENARIOS))
    if unknown:
        print(f"모르는 시나리오: {unknown}", file=sys.stderr)
        return 2
    planned = len(picked) * args.reps
    if not args.preflight and planned > args.max_reports:
        print(
            f"계획 {planned}건이 과금 상한 --max-reports {args.max_reports}를 넘는다"
            " — 상한을 명시해 올린다",
            file=sys.stderr,
        )
        return 2
    _prepare_env(args)
    here = Path(__file__).resolve()
    sys.path.insert(0, str(here.parents[2] / "src"))
    sys.path.insert(0, str(here.parent))
    if args.preflight:
        return asyncio.run(_preflight())
    out_dir: Path = args.out / args.label
    out_dir.mkdir(parents=True, exist_ok=True)
    return asyncio.run(_run(args, picked, out_dir))


async def _preflight() -> int:
    """Claude 없이 측정 경로의 바깥 의존(터널·TLS·읽기 전용·Voyage·RAG)을 확인한다."""
    from core import db
    from rag import search

    db._build_ssl = _tunnel_ssl
    pool = await db.init_pool()
    try:
        read_only = await pool.fetchval("SHOW default_transaction_read_only")
        policy_rows = await pool.fetchval(
            "SELECT count(*) FROM policy_chunks WHERE insurer = $1", SCENARIOS["A"]["insurer"]
        )
        t0 = time.perf_counter()
        result = await search(SCENARIOS["A"]["claim_dx"], namespaces=["terms"], top_k=5)
        rag_s = time.perf_counter() - t0
    finally:
        await db.close_pool()
    print(f"read_only={read_only} · policy_chunks({SCENARIOS['A']['insurer']})={policy_rows}")
    print(f"rag.search terms top5: {len(result.ranked_chunks)}건 · {rag_s:.1f}s")
    return 0 if read_only == "on" and result.ranked_chunks else 1


class _ChatUsage(logging.Handler):
    """``ai_client.chat.finished`` 이벤트를 메모리에 모은다(실행별 토큰 합계 출력용)."""

    def __init__(self) -> None:
        super().__init__()
        self.events: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        if not isinstance(record.msg, dict):
            return
        # 완료는 log_event(event_type 있음), API 오류는 logger.warning(message만 있음)으로 남는다.
        kind = record.msg.get("event_type") or record.msg.get("message")
        if kind in ("ai_client.chat.finished", "ai_client.chat.api_error"):
            self.events.append({**record.msg, "event_type": kind})


async def _run(args: argparse.Namespace, picked: list[str], out_dir: Path) -> int:
    import structlog
    from summarize_events import collect, iter_events, summarize

    from core import db
    from core.config import get_settings
    from core.logging import configure_logging, get_logger
    from report_worker.nodes import agents
    from report_worker.worker import _log_graph_finished

    db._build_ssl = _tunnel_ssl  # 터널 전용 — 운영 코드는 바꾸지 않는다
    settings = get_settings()
    events_path = out_dir / "events.jsonl"

    configure_logging()
    root = logging.getLogger()
    console = root.handlers[0]
    console.setLevel(logging.WARNING)
    file_handler = logging.FileHandler(events_path, mode="w", encoding="utf-8")
    file_handler.setFormatter(console.formatter)
    root.addHandler(file_handler)
    usage = _ChatUsage()
    root.addHandler(usage)
    logger = get_logger("report_bench")

    current: dict[str, Any] = {}

    @agents.safe_node
    async def load_context(state: Any) -> dict[str, Any]:
        """DB 대신 합성 시나리오로 컨텍스트를 채운다(운영 load_context와 같은 키)."""
        sc = current["scenario"]
        return {
            "case_info": {
                "accident_type": sc["accident_type"],
                "diagnosis": sc["claim_dx"],
                "offered_amount": sc["offered"],
                "insurer": sc["insurer"],
                "product_name": sc["product"],
                "enrolled_at": sc["enrolled"],
                "question": sc["question"],
                "description": None,
                "additional_information": None,
            },
            "masked_text": sc["masked_text"],
            "entities": dict(sc["entities"]),
            "subscribed_coverages": list(_COVERAGES),
            "input_documents": [
                {"doc_type": "diagnosis", "ocr_confidence": 0.95, "doc_type_confidence": 0.9}
            ],
            "errors": [],
        }

    @agents.safe_node
    async def persist(state: Any) -> dict[str, Any]:
        """저장 대역 — 아무것도 쓰지 않는다(결과는 최종 상태에서 읽는다)."""
        return {}

    @agents.safe_node
    async def persist_blocked(state: Any) -> dict[str, Any]:
        return {}

    @agents.safe_node
    async def persist_refused(state: Any) -> dict[str, Any]:
        return {}

    agents.load_context = load_context
    agents.persist = persist
    agents.persist_blocked = persist_blocked
    agents.persist_refused = persist_refused
    from report_worker.graph import build_graph

    graph = build_graph()
    await db.init_pool()
    runs: list[dict[str, Any]] = []
    started_at = datetime.now(UTC)
    try:
        for rep in range(1, args.reps + 1):
            for sid in picked:
                current["scenario"] = SCENARIOS[sid]
                usage.events.clear()
                state = {
                    "report_id": str(uuid.uuid4()),
                    "ocr_result_id": str(uuid.uuid4()),
                    "claim_id": None,
                    "user_ref": "bench",
                    "doc_type": "diagnosis",
                }
                t0 = time.perf_counter()
                timed_out = False
                with structlog.contextvars.bound_contextvars(
                    bench_phase="measure", bench_rep=rep, bench_scenario=sid
                ):
                    # LLM 스트림이 200 OK 뒤 끝나지 않고 멈춘 사례가 있다(dev 실측 30분+) — SDK
                    # 타임아웃은 청크 사이 간격이라 이걸 못 끊는다. 리포트 단위 상한을 건다.
                    try:
                        final = await asyncio.wait_for(
                            graph.ainvoke(state), timeout=args.report_timeout
                        )
                    except TimeoutError:
                        timed_out = True
                        final = {}
                        logger.warning("bench_report_timeout", timeout_s=args.report_timeout)
                    wall = time.perf_counter() - t0
                    if not timed_out:
                        _log_graph_finished(final, wall_ms=round(wall * 1000))
                chats = [e for e in usage.events if e["event_type"] == "ai_client.chat.finished"]
                run = {
                    "scenario": sid,
                    "rep": rep,
                    "timed_out": timed_out,
                    "wall_s": round(wall, 1),
                    "node_ms": {e["node"]: e["ms"] for e in final.get("node_trace") or []},
                    "outside_nodes_s": round(
                        wall - sum(e["ms"] for e in final.get("node_trace") or []) / 1000, 1
                    ),
                    "llm_calls": len(chats),
                    "llm_errors": len(usage.events) - len(chats),
                    "input_tokens": sum(e.get("input_tokens") or 0 for e in chats),
                    "output_tokens": sum(e.get("output_tokens") or 0 for e in chats),
                    "cache_read_tokens": sum(e.get("cache_read_input_tokens") or 0 for e in chats),
                    "stop_reasons": sorted({str(e.get("stop_reason")) for e in chats}),
                    "served_models": sorted({str(e.get("model")) for e in chats}),
                    "errors": list(final.get("errors") or []),
                    "llm_failure": final.get("llm_failure"),
                    "confidence_level": final.get("confidence_level"),
                    "estimated_range": final.get("estimated_range"),
                    "judge_failures": len(final.get("judge_failures") or []),
                }
                runs.append(run)
                # 실행마다 바로 쓴다 — 중단(SIGTERM 등)돼도 끝난 실행은 남는다.
                (out_dir / "runs.json").write_text(
                    json.dumps(runs, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
                )
                print(
                    f"  rep{rep} {sid}: {wall:.1f}s · LLM {run['llm_calls']}회 "
                    f"(in {run['input_tokens']:,} / out {run['output_tokens']:,} tok) · "
                    f"신뢰도 {run['confidence_level']} · errors {len(run['errors'])}"
                )
    finally:
        await db.close_pool()
        ended_at = datetime.now(UTC)
        file_handler.flush()
        with events_path.open(encoding="utf-8") as handle:
            measured = [e for e in iter_events(handle) if e.get("bench_phase") == "measure"]
        (out_dir / "summary.json").write_text(
            json.dumps(summarize(collect(measured)), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (out_dir / "runs.json").write_text(
            json.dumps(runs, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        meta = {
            "label": args.label,
            "version": os.environ.get("BENCH_VERSION") or "local",
            "model": settings.llm_model,
            "effort": settings.llm_effort,
            "embedding_model": settings.embedding_model,
            "scenarios": picked,
            "reps": args.reps,
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
        }
        (out_dir / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("report_bench_finished", runs=len(runs))
    print(f"\n결과: {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
