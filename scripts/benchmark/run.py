"""벤치마크 오케스트레이터 CLI.

    PYTHONPATH=src:scripts python -m benchmark.run --models gemma3:12b,exaone3.5:7.8b --repeats 3 \
        --judge-model qwen2.5:32b --out scripts/benchmark/results

흐름: 프리플라이트(policy_chunks 적재 확인) → 검색 정확성 1회(임베딩 축) → 모델별{속도 프로브 +
시나리오 x repeat 실행 + 품질 채점} → 집계표(summary.md/json/csv) + 원자료(raw_runs.jsonl).

전제: tempVectorDB(policy_chunks 적재) + Ollama(후보/임베딩/judge 모델) 기동. README.md 참고.
production 코드는 수정하지 않는다 — settings.llm_model 런타임 교체 + ai_client 계측 래핑으로만 동작.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any

from battery import SCENARIOS, _cleanup, _seed

from benchmark import instrument, metrics, quality_judge, retrieval, speed_probe
from benchmark.cases import GOLD, RETRIEVAL_GOLD, CaseGold
from core.config import settings
from core.db import close_pool, get_pool, init_pool
from core.logging import get_logger
from report_worker.graph import build_graph
from report_worker.rag.hybrid import close_pool as close_rag_pool

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AI 모델 선정 벤치마크(report_worker LangGraph 대상)")
    p.add_argument("--models", help="후보 모델 CSV (예: gemma3:12b,exaone3.5:7.8b)")
    p.add_argument("--repeats", type=int, default=3, help="시나리오당 반복(속도 분산용)")
    p.add_argument(
        "--judge-model", default="", help="LLM Judge/품질 채점 고정 모델(후보와 달라야 함)"
    )
    p.add_argument(
        "--embedding-model", default="", help="임베딩 모델 오버라이드(주의: 재적재 필요)"
    )
    p.add_argument("--top-k", type=int, default=8, help="검색 평가 컷오프")
    p.add_argument("--no-quality", action="store_true", help="리포트 품질 LLM 채점 생략")
    p.add_argument("--no-speed-probe", action="store_true", help="정밀 tok/s 프로브 생략")
    p.add_argument(
        "--discover-retrieval", action="store_true", help="검색 골드 라벨링용 후보만 출력"
    )
    p.add_argument("--skip-preflight", action="store_true", help="policy_chunks 적재 확인 생략")
    p.add_argument("--out", default="scripts/benchmark/results", help="결과 출력 디렉터리")
    return p.parse_args()


async def _preflight() -> int:
    """policy_chunks 적재 행수를 확인한다(0이면 RAG가 비어 측정 무의미)."""
    pool = get_pool()
    async with pool.acquire() as conn:
        return int(await conn.fetchval("SELECT count(*) FROM policy_chunks") or 0)


async def _draft_saved(report_id: str) -> bool:
    pool = get_pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT count(*) FROM report_drafts WHERE report_id = $1", uuid.UUID(report_id)
        )
    return bool(n)


def _job(ids: tuple[str, str, str, Any]) -> dict[str, Any]:
    rid, oid, cid, uid = ids
    return {
        "report_id": rid,
        "ocr_result_id": oid,
        "claim_id": cid,
        "user_ref": str(uid),
        "doc_type": "diagnosis",
    }


async def _run_one(app: Any, ids: tuple, label: str) -> dict[str, Any]:
    """시나리오 1회 실행 → 텔레메트리·최종상태·채점을 담은 raw run."""
    job = _job(ids)
    t0 = time.perf_counter()
    with instrument.record_run() as tel:
        state, timings = await instrument.astream_timed(app, job)
    e2e = time.perf_counter() - t0

    draft = await _draft_saved(ids[0])
    gold = GOLD.get(label, CaseGold())
    score = metrics.score_case(state, gold, draft_saved=draft)

    node_calls = instrument.attribute_calls_to_nodes(tel.calls, timings, t0)
    per_node_latency = {
        node: round(sum(c.latency_s for c in calls), 4) for node, calls in node_calls.items()
    }
    return {
        "label": label,
        "e2e_s": round(e2e, 4),
        "n_llm_calls": sum(1 for c in tel.calls if c.kind != "embed"),
        "n_embed_calls": sum(1 for c in tel.calls if c.kind == "embed"),
        "per_call_latency_s": [round(c.latency_s, 4) for c in tel.calls],
        "per_node_latency_s": per_node_latency,
        "score": score,
        # 품질 채점을 위해 report 보관(마지막 repeat만 쓰지만 run마다 저장은 과함 → runner가 선별)
        "report": state.get("report", "") or "",
        "question": (state.get("case_info") or {}).get("question", ""),
    }


async def run_model(app: Any, model: str, args: argparse.Namespace) -> dict[str, Any]:
    """한 후보 모델에 대해 속도 프로브 + 시나리오 x repeat 실행 + 품질 채점."""
    settings.llm_model = model
    logger.info("bench.model.start", model=model, repeats=args.repeats)

    # warm-up: 첫 시나리오 1회(VRAM 로드·그래프 경로 JIT 배제). 결과 폐기.
    warm_ids = await _seed(*SCENARIOS[0])
    try:
        with instrument.record_run():
            await instrument.astream_timed(app, _job(warm_ids))
    finally:
        await _cleanup(*warm_ids)

    # 정밀 tok/s 프로브(통제된 프롬프트)
    probe_result: dict[str, Any] | None = None
    if not args.no_speed_probe:
        try:
            probe_result = await speed_probe.probe(
                model, settings.ai_base_url, settings.ai_api_key, repeats=args.repeats
            )
        except Exception as exc:  # 프로브 실패는 치명적 아님 — 경고 후 계속
            logger.warning("bench.speed_probe.failed", model=model, error=str(exc))

    # 시나리오 실행
    raw_runs: list[dict[str, Any]] = []
    last_report: dict[str, dict[str, str]] = {}
    for scenario in SCENARIOS:
        label = scenario[0]
        for rep in range(args.repeats):
            ids = await _seed(*scenario)
            try:
                run = await _run_one(app, ids, label)
                run["repeat"] = rep
                raw_runs.append(run)
                last_report[label] = {"report": run["report"], "question": run["question"]}
            finally:
                await _cleanup(*ids)
        logger.info("bench.scenario.done", model=model, label=label)

    # 리포트 품질 채점(고정 judge)
    quality: dict[str, Any] = {}
    judge_model = args.judge_model.strip()
    if not args.no_quality and judge_model and judge_model != model:
        for label, rep in last_report.items():
            report_text = rep["report"]
            if not report_text or GOLD.get(label, CaseGold()).should_block:
                continue  # 차단·빈 리포트는 채점 제외
            try:
                quality[label] = await quality_judge.judge_report(
                    report_text, case_summary=f"{label}: {rep['question']}", judge_model=judge_model
                )
            except Exception as exc:
                logger.warning("bench.quality.failed", model=model, label=label, error=str(exc))
    elif not args.no_quality and judge_model == model:
        logger.warning("bench.quality.skip_self_judge", model=model)

    return {"model": model, "raw_runs": raw_runs, "speed_probe": probe_result, "quality": quality}


# ── 집계 ──────────────────────────────────────────────────────────
_MEAN_KEYS = [
    "verified_rate",
    "judge_failures",
    "json_failures",
    "amount_assertions_caught",
    "applicable_f1",
    "missing_f1",
    "disability_combined_rate",
]
_RATE_KEYS = [
    "draft_saved",
    "block_correct",
    "terms_parse_correct",
    "disability_route_correct",
    "accident_type_correct",
    "pii_ok",
    "disability_rate_in_range",
]


def aggregate_model(model_result: dict[str, Any]) -> dict[str, Any]:
    """모델별 raw run·프로브·품질을 요약 지표로 접는다."""
    runs = model_result["raw_runs"]
    e2e = [r["e2e_s"] for r in runs]
    per_call = [x for r in runs for x in r["per_call_latency_s"]]
    speed = metrics.aggregate_speed(e2e, per_call)

    agg: dict[str, Any] = {"model": model_result["model"], **speed}

    for key in _MEAN_KEYS:
        vals = [r["score"][key] for r in runs if r["score"].get(key) is not None]
        agg[key] = round(metrics.mean(vals), 4) if vals else None
    for key in _RATE_KEYS:
        flags = [bool(r["score"][key]) for r in runs if r["score"].get(key) is not None]
        rate = metrics.rate_of(flags)
        agg[key] = round(rate, 4) if rate is not None else None

    # 정밀 속도: native decode_tok_s(가능) 우선, 아니면 openai output_tok_s. 프롬프트 평균.
    probe = model_result.get("speed_probe")
    agg["decode_tok_s"] = _probe_tok_s(probe, "native", "decode_tok_s")
    agg["output_tok_s"] = _probe_tok_s(probe, "openai", "output_tok_s")
    agg["prefill_tok_s"] = _probe_tok_s(probe, "native", "prefill_tok_s")

    # 품질(축 overall 평균)
    q_overall = [
        q["overall"]
        for q in model_result["quality"].values()
        if isinstance(q, dict) and q.get("overall") is not None
    ]
    agg["quality_overall"] = round(metrics.mean(q_overall), 3) if q_overall else None
    return agg


def _probe_tok_s(probe: dict[str, Any] | None, section: str, key: str) -> float | None:
    if not probe:
        return None
    vals = [v[key] for v in probe.get(section, {}).values() if isinstance(v, dict) and key in v]
    return round(metrics.mean(vals), 2) if vals else None


# ── 출력 ──────────────────────────────────────────────────────────
_TABLE_COLS = [
    ("model", "모델"),
    ("e2e_p50_s", "e2e p50(s)"),
    ("e2e_p90_s", "e2e p90(s)"),
    ("decode_tok_s", "decode tok/s"),
    ("verified_rate", "verified율"),
    ("judge_failures", "judge실패(평균)"),
    ("json_failures", "JSON실패(평균)"),
    ("disability_route_correct", "장해라우팅정확"),
    ("block_correct", "차단정확"),
    ("draft_saved", "저장율"),
    ("quality_overall", "품질(1~5)"),
]


def _fmt(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.3g}"
    return str(v)


def _write_json(path: Path, obj: Any) -> None:
    """블로킹 JSON 쓰기(async 경로에서 asyncio.to_thread로 격리)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def write_reports(
    out_dir: Path,
    per_model: list[dict[str, Any]],
    retrieval_result: dict[str, Any] | None,
    all_raw: list[dict[str, Any]],
    args: argparse.Namespace,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "raw_runs.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in all_raw), encoding="utf-8"
    )
    (out_dir / "summary.json").write_text(
        json.dumps(
            {"models": per_model, "retrieval": retrieval_result, "config": vars(args)},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    # CSV
    keys = [c[0] for c in _TABLE_COLS]
    csv_lines = [",".join(keys)]
    csv_lines += [",".join(_fmt(m.get(k)) for k in keys) for m in per_model]
    (out_dir / "summary.csv").write_text("\n".join(csv_lines), encoding="utf-8")

    # Markdown
    md: list[str] = ["# 모델 선정 벤치마크 결과\n"]
    if retrieval_result is not None:
        r = retrieval_result
        if r.get("n_labeled"):
            md.append(
                f"**검색 정확성(임베딩 축, 챗 LLM 무관)** — 라벨 {r['n_labeled']}건: "
                f"Recall@{args.top_k}={_fmt(r.get(f'recall@{args.top_k}'))}, "
                f"MRR={_fmt(r.get('mrr'))}, nDCG@{args.top_k}={_fmt(r.get(f'ndcg@{args.top_k}'))}, "
                f"Hit@1={_fmt(r.get('hit@1'))}\n"
            )
        else:
            md.append(
                f"**검색 정확성** — 라벨된 골드 0건(미라벨 {r.get('n_unlabeled', 0)}). "
                "`--discover-retrieval`로 후보를 뽑아 cases.RETRIEVAL_GOLD를 채우세요.\n"
            )

    header = "| " + " | ".join(c[1] for c in _TABLE_COLS) + " |"
    sep = "|" + "|".join(["---"] * len(_TABLE_COLS)) + "|"
    md.append(header)
    md.append(sep)
    for m in per_model:
        md.append("| " + " | ".join(_fmt(m.get(c[0])) for c in _TABLE_COLS) + " |")

    md.append(
        "\n> **해석**: 정확도 하드 게이트(JSON실패≈0, 장해라우팅·차단정확 높음, verified율 높음)를 "
        "통과한 모델만 e2e 지연으로 비교하세요. 도메인 라벨(특약 F1·지급률)을 cases.GOLD에 채우면 "
        "applicable_f1 등이 자동 추가됩니다. '-'는 라벨 미보유로 미채점.\n"
    )
    (out_dir / "summary.md").write_text("\n".join(md), encoding="utf-8")
    logger.info("bench.reports.written", out=str(out_dir))


# ── 메인 ──────────────────────────────────────────────────────────
async def main() -> None:
    args = parse_args()
    out_dir = Path(args.out)
    await init_pool()
    try:
        if not args.skip_preflight:
            n_policy = await _preflight()
            if n_policy == 0:
                logger.error(
                    "bench.preflight.no_policy_chunks",
                    hint="tempVectorDB에 약관 임베딩 적재 필요. --skip-preflight로 무시 가능",
                )
                return
            logger.info("bench.preflight.ok", policy_chunks=n_policy)

        if args.embedding_model:
            logger.warning(
                "bench.embedding_override",
                model=args.embedding_model,
                warn="임베딩 모델을 바꾸면 적재 벡터공간과 불일치 — tempVectorDB 재적재 필요",
            )
            settings.embedding_model = args.embedding_model

        # 검색 골드 라벨링 지원 모드
        if args.discover_retrieval:
            cand = await retrieval.discover(RETRIEVAL_GOLD, top_k=args.top_k)
            path = out_dir / "retrieval_candidates.json"
            await asyncio.to_thread(_write_json, path, cand)
            logger.info("bench.discover.written", out=str(path))
            return

        if not args.models:
            logger.error("bench.no_models", hint="--models gemma3:12b,exaone3.5:7.8b 형식으로 지정")
            return

        judge = args.judge_model.strip()
        instrument.install(judge_model=judge or None)
        if not judge:
            logger.warning(
                "bench.judge.not_pinned",
                warn="Judge가 후보 모델로 채점됨(자기채점 편향). --judge-model 권장",
            )

        # 검색 정확성(임베딩 축) — 1회
        retrieval_result = await retrieval.score_retrieval(RETRIEVAL_GOLD, top_k=args.top_k)
        if not retrieval_result.get("n_labeled"):
            logger.warning(
                "bench.retrieval.unlabeled", hint="--discover-retrieval 후 RETRIEVAL_GOLD 채우기"
            )

        app = build_graph()
        models = [m.strip() for m in args.models.split(",") if m.strip()]
        per_model: list[dict[str, Any]] = []
        all_raw: list[dict[str, Any]] = []
        for model in models:
            result = await run_model(app, model, args)
            for run in result["raw_runs"]:
                all_raw.append({"model": model, **{k: v for k, v in run.items() if k != "report"}})
            per_model.append(aggregate_model(result))

        write_reports(out_dir, per_model, retrieval_result, all_raw, args)
        for m in per_model:
            logger.info(
                "bench.model.summary",
                model=m["model"],
                e2e_p90_s=m.get("e2e_p90_s"),
                decode_tok_s=m.get("decode_tok_s"),
                verified_rate=m.get("verified_rate"),
                judge_failures=m.get("judge_failures"),
                quality=m.get("quality_overall"),
            )
    finally:
        instrument.uninstall()
        await close_pool()
        await close_rag_pool()


if __name__ == "__main__":
    asyncio.run(main())
