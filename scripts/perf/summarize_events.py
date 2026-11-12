"""성능 계측 이벤트 집계 — JSON 로그에서 단계별 소요 시간 분포(n·p50·p95·max)를 낸다.

워커가 남기는 계측 이벤트(``docs/perf/README.md`` §2)를 로그 수집 경로와 무관하게 집계한다.
입력은 한 줄에 JSON 1건인 로그(운영 JSONRenderer 출력 — ``kubectl logs`` 덤프·로컬 실행 출력)다.
JSON이 아닌 줄과 계측 이벤트가 아닌 줄은 건너뛴다.

사용법:
    uv run python scripts/perf/summarize_events.py ocr-worker.log report-worker.log
    kubectl logs deploy/ocr-worker --since=24h | uv run python scripts/perf/summarize_events.py -
    uv run python scripts/perf/summarize_events.py --json out.json *.log   # 실험 기록용

출력 그룹(이벤트 → 묶는 키 → 값):
    ocr.job.finished          → (전체)                  → duration_ms
    ocr.stage.finished        → stage                   → duration_ms
    report.graph.finished     → (전체)·노드별            → total_ms · node_ms.<노드>
    ai_client.chat.finished   → graph_node              → duration_ms·output_tokens
    ai_client.embed.finished  → graph_node              → duration_ms
    sqs.message.finished      → queue                   → handle_ms·queue_wait_ms
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, TextIO


def percentile(values: list[float], q: float) -> float:
    """선형 보간 백분위수(numpy 기본 방식과 같다). ``values``는 비어 있지 않아야 한다."""
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower, upper = math.floor(position), math.ceil(position)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def iter_events(lines: Iterable[str]) -> Iterator[dict[str, Any]]:
    """JSON 객체 줄만 골라 낸다(콘솔 렌더링·비 JSON 줄은 건너뛴다)."""
    for line in lines:
        text = line.strip()
        if not text.startswith("{"):
            continue
        try:
            record = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and "event_type" in record:
            yield record


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def collect(events: Iterable[dict[str, Any]]) -> dict[str, list[float]]:
    """이벤트를 ``"<이벤트>|<그룹>|<값 이름>"`` 키별 수치 목록으로 모은다."""
    series: dict[str, list[float]] = defaultdict(list)

    def add(key: str, value: object) -> None:
        number = _number(value)
        if number is not None:
            series[key].append(number)

    for event in events:
        kind = event["event_type"]
        status = event.get("status")
        if kind == "ocr.job.finished":
            add(f"ocr.job|all({status})|duration_ms", event.get("duration_ms"))
        elif kind == "ocr.stage.finished":
            add(f"ocr.stage|{event.get('stage')}|duration_ms", event.get("duration_ms"))
        elif kind == "report.graph.finished":
            add("report.graph|all|total_ms", event.get("total_ms"))
            add("report.graph|all|wall_ms", event.get("wall_ms"))
            add("report.graph|all|outside_nodes_ms", event.get("outside_nodes_ms"))
            for node, ms in (event.get("node_ms") or {}).items():
                add(f"report.graph|{node}|ms", ms)
        elif kind == "ai_client.chat.finished":
            node = event.get("graph_node") or "-"
            add(f"llm.chat|{node}|duration_ms", event.get("duration_ms"))
            add(f"llm.chat|{node}|output_tokens", event.get("output_tokens"))
        elif kind == "ai_client.embed.finished":
            node = event.get("graph_node") or "-"
            add(f"embed|{node}|duration_ms", event.get("duration_ms"))
        elif kind == "sqs.message.finished":
            queue = f"{event.get('queue')}({event.get('outcome')})"
            add(f"sqs|{queue}|handle_ms", event.get("handle_ms"))
            add(f"sqs|{queue}|queue_wait_ms", event.get("queue_wait_ms"))
    return series


def summarize(series: dict[str, list[float]]) -> list[dict[str, Any]]:
    """키별 n·p50·p95·max(정수 반올림)."""
    rows = []
    for key in sorted(series):
        values = series[key]
        event, group, metric = key.split("|", 2)
        rows.append(
            {
                "event": event,
                "group": group,
                "metric": metric,
                "n": len(values),
                "p50": round(percentile(values, 0.5)),
                "p95": round(percentile(values, 0.95)),
                "max": round(max(values)),
            }
        )
    return rows


def _open_inputs(paths: list[str]) -> Iterator[TextIO]:
    for path in paths:
        if path == "-":
            yield sys.stdin
        else:
            with Path(path).open(encoding="utf-8") as handle:
                yield handle


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="+", help="JSON 로그 파일(표준입력은 -)")
    parser.add_argument("--json", type=Path, help="집계 결과를 JSON으로도 저장(실험 기록용)")
    args = parser.parse_args()

    series: dict[str, list[float]] = defaultdict(list)
    for handle in _open_inputs(args.paths):
        for key, values in collect(iter_events(handle)).items():
            series[key].extend(values)
    rows = summarize(series)

    header = f"{'event':<14}{'group':<34}{'metric':<16}{'n':>6}{'p50':>10}{'p95':>10}{'max':>10}"
    print(header)
    print("-" * len(header))
    for row in rows:
        print(
            f"{row['event']:<14}{row['group']:<34}{row['metric']:<16}"
            f"{row['n']:>6}{row['p50']:>10}{row['p95']:>10}{row['max']:>10}"
        )
    if args.json:
        args.json.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
