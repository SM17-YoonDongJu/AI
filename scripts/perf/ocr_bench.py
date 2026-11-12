"""OCR 파이프라인 벤치 — 고정 샘플을 운영 코드(OcrPipeline)로 반복 처리해 단계별 시간을 잰다.

S3·DB·SQS만 대역으로 바꾸고(로컬 파일 읽기·저장 캡처·발행 무시) 나머지는 운영 경로 그대로다
(PaddleOCR·NER·마스킹·검은블록·재OCR 검증). 그래서 ``db_save``·``publish``·``upload`` 단계는
대역 시간이라 운영을 대표하지 않는다 — 나머지 단계를 본다.

결과(``<out>/<label>/``):
    events.jsonl        계측 이벤트(JSON 1줄 1건, 워밍업 포함 — bench_phase로 구분)
    summary.json        측정(measure) 이벤트의 단계별 n·p50·p95·max
    fingerprints.json   문서별 결과 지문(마스킹 텍스트·라인·엔티티·검은블록 PNG 해시)
                        — 개선안이 마스킹 결과를 바꾸지 않았는지(품질 가드레일) 비교한다
    meta.json           조건(라벨·코드 버전·CPU·스레드·NER·VLM·반복 수)

사용법:
    # 로컬(상대 비교) — prod t3.large(2 vCPU)에 맞춰 스레드를 2로 묶는다
    OMP_NUM_THREADS=2 OCR_CPU_THREADS=2 USE_NER=true uv run --extra ocr --extra ner \\
      python scripts/perf/ocr_bench.py samples out/perf --label baseline
    # VLM(vLLM)을 부르지 않는다(비VLM 단계 기준선) — 연결 거부로 즉시 OCR 엔진 폴백
    ... --no-vlm
    # 두 실행의 결과 지문 비교
    uv run python scripts/perf/ocr_bench.py --compare out/perf/baseline out/perf/o1
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import platform
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

_CONTENT_TYPES = {
    ".pdf": "application/pdf",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
}
# 연결이 즉시 거부되는 주소 — VLM 호출이 타임아웃을 기다리지 않고 바로 OCR 엔진 결과로 폴백한다.
_NO_VLM_URL = "http://127.0.0.1:9"


def _sha(value: object) -> str:
    raw = (
        value
        if isinstance(value, bytes)
        else json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()
    )
    return hashlib.sha256(raw).hexdigest()[:16]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("samples", nargs="?", type=Path, help="샘플 문서 디렉터리(pdf·jpg·png)")
    parser.add_argument("out", nargs="?", type=Path, help="결과 상위 디렉터리")
    parser.add_argument("--label", default="baseline", help="조건 이름(결과 하위 디렉터리)")
    parser.add_argument("--reps", type=int, default=3, help="측정 반복 수(문서셋 전체 기준)")
    parser.add_argument("--warmup", type=int, default=1, help="측정 전 워밍업 문서 수(모델 로드)")
    parser.add_argument("--no-vlm", action="store_true", help="VLM을 부르지 않는다(즉시 폴백)")
    parser.add_argument("--compare", nargs=2, type=Path, metavar=("A", "B"), help="지문 비교")
    return parser.parse_args()


def _compare(a: Path, b: Path) -> int:
    """두 실행의 문서별 결과 지문을 비교한다. 다르면 1을 돌려준다(품질 가드레일 위반)."""
    left = json.loads((a / "fingerprints.json").read_text(encoding="utf-8"))
    right = json.loads((b / "fingerprints.json").read_text(encoding="utf-8"))
    diffs = 0
    for doc in sorted(set(left) | set(right)):
        if left.get(doc) != right.get(doc):
            diffs += 1
            fields = sorted(
                key
                for key in set(left.get(doc) or {}) | set(right.get(doc) or {})
                if (left.get(doc) or {}).get(key) != (right.get(doc) or {}).get(key)
            )
            print(f"DIFF {doc}: {', '.join(fields)}")
    print(f"{len(set(left) | set(right))}개 문서 중 {diffs}개 다름")
    return 1 if diffs else 0


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)  # 진행 로그를 파일로 바로 흘린다(nohup 실행)
    args = _parse_args()
    if args.compare:
        return _compare(*args.compare)
    if args.samples is None or args.out is None:
        print("samples와 out이 필요하다", file=sys.stderr)
        return 2

    # 설정은 import 시점에 읽히므로 core 모듈보다 먼저 env를 정한다. ENV=dev면 JSON 로그다.
    os.environ.setdefault("ENV", "dev")
    os.environ.setdefault("LOG_LEVEL", "INFO")
    if args.no_vlm:
        os.environ["VLM_BASE_URL"] = _NO_VLM_URL
    here = Path(__file__).resolve()
    sys.path.insert(0, str(here.parents[2] / "src"))
    sys.path.insert(0, str(here.parent))
    out_dir: Path = args.out / args.label
    out_dir.mkdir(parents=True, exist_ok=True)
    return asyncio.run(_run(args, out_dir))


async def _run(args: argparse.Namespace, out_dir: Path) -> int:
    import structlog
    from summarize_events import collect, iter_events, summarize

    from core.config import get_settings
    from core.contracts import OcrJob
    from core.logging import configure_logging, get_logger
    from ocr_worker.__main__ import _with_memory_release
    from ocr_worker.ocr import OcrProcessor
    from ocr_worker.pipeline import OcrPipeline

    settings = get_settings()
    events_path = out_dir / "events.jsonl"

    # 운영과 같은 JSON 포맷으로 이벤트 파일에 쓴다. 콘솔(stderr)엔 경고 이상만.
    configure_logging()
    root = logging.getLogger()
    console = root.handlers[0]
    console.setLevel(logging.WARNING)
    file_handler = logging.FileHandler(events_path, mode="w", encoding="utf-8")
    file_handler.setFormatter(console.formatter)
    root.addHandler(file_handler)
    logger = get_logger("ocr_bench")

    # 숨김 파일(macOS AppleDouble "._x.pdf" 등)은 문서가 아니다 — 확장자만 보면 섞인다.
    docs = sorted(
        p
        for p in args.samples.iterdir()
        if p.suffix.lower() in _CONTENT_TYPES and not p.name.startswith(".")
    )
    if not docs:
        print(f"샘플 없음: {args.samples}", file=sys.stderr)
        return 2

    async def fetch_local(path: str) -> bytes:
        return await asyncio.to_thread(Path(path).read_bytes)

    uploaded: dict[str, str] = {}

    async def capture_upload(key: str, data: bytes, _content_type: str) -> None:
        uploaded[key] = _sha(data)

    async def noop_delete(_key: str) -> None:
        return None

    class _NullPool:
        """DB 대역 — 멱등 조회는 '처음 보는 작업', 쓰기는 무시한다(저장은 _save가 캡처)."""

        async def fetchrow(self, *_args: Any) -> None:
            return None

        async def fetchval(self, *_args: Any) -> None:
            return None

        async def fetch(self, *_args: Any) -> list[Any]:
            return []

        async def execute(self, *_args: Any) -> str:
            return "OK"

    class _NullProducer:
        async def send(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    class _BenchPipeline(OcrPipeline):
        """저장 단계만 캡처로 바꾼 운영 파이프라인."""

        saved: ClassVar[dict[str, Any]] = {}

        async def _save(self, job: Any, record: Any, claim: Any) -> tuple[str, int | None]:
            self.saved[job.job_id] = record
            return str(uuid.uuid4()), None

    processor = OcrProcessor(fetch=fetch_local)
    pipeline = _BenchPipeline(
        pool=_NullPool(),  # type: ignore[arg-type]
        producer=_NullProducer(),  # type: ignore[arg-type]
        processor=processor,
        upload=capture_upload,
        delete=noop_delete,
    )
    # 운영 워커와 같게 문서마다 힙을 OS로 돌려준다(gc + malloc_trim). 빼면 A4 검출 최고점(약
    # 3.9GB)의 잔여가 문서마다 쌓여 운영 메모리 상한(4.5GiB)에서 OOM으로 죽는다(dev 실측).
    handle_job = _with_memory_release(pipeline.handle)

    def make_job(path: Path) -> OcrJob:
        return OcrJob(
            job_id=str(uuid.uuid4()),
            s3_key=str(path),
            content_type=_CONTENT_TYPES[path.suffix.lower()],  # type: ignore[arg-type]
            user_ref="bench",
            report_id=str(uuid.uuid4()),
            attachment_id=str(uuid.uuid4()),
            uploaded_at=datetime.now(UTC),
        )

    async def run_one(path: Path, phase: str, rep: int) -> tuple[float, dict[str, Any] | None]:
        job = make_job(path)
        uploaded.clear()
        started = time.perf_counter()
        with structlog.contextvars.bound_contextvars(
            bench_phase=phase, bench_rep=rep, bench_doc=path.name
        ):
            try:
                await handle_job(job)
                await pipeline.wait_for_pending_deletes()
            except Exception as exc:  # 벤치는 문서 하나 실패로 멈추지 않는다 — 타입만 남긴다
                logger.warning("bench_doc_failed", doc=path.name, error_type=type(exc).__name__)
                return time.perf_counter() - started, None
        elapsed = time.perf_counter() - started
        record = pipeline.saved.pop(job.job_id, None)
        if record is None:
            return elapsed, None
        fingerprint = {
            "doc_type": str(record.doc_type),
            "ocr_quality": record.ocr_quality,
            "masked_text": _sha(record.masked_text),
            "masked_lines": _sha(record.masked_lines),
            "entities": _sha(record.entities),
            "delete_original": record.original_delete_eligible,
            "masked_images": [uploaded[key] for key in sorted(uploaded)],
        }
        return elapsed, fingerprint

    print(f"[{args.label}] 워밍업 {args.warmup}건 (모델 로드 포함 — 측정 제외)")
    for path in docs[: args.warmup]:
        elapsed, _ = await run_one(path, "warmup", 0)
        print(f"  warmup {path.name}: {elapsed:.1f}s")

    fingerprints: dict[str, Any] = {}
    unstable: set[str] = set()
    wall: dict[str, list[float]] = {}
    started_at = datetime.now(UTC)
    for rep in range(1, args.reps + 1):
        for path in docs:
            elapsed, fingerprint = await run_one(path, "measure", rep)
            wall.setdefault(path.name, []).append(elapsed)
            print(f"  rep{rep} {path.name}: {elapsed:.1f}s")
            if fingerprint is None:
                continue
            previous = fingerprints.setdefault(path.name, fingerprint)
            if previous != fingerprint:
                unstable.add(path.name)  # 같은 입력인데 결과가 반복마다 다르다 — 비교 불가 표시
    ended_at = datetime.now(UTC)

    file_handler.flush()
    with events_path.open(encoding="utf-8") as handle:
        measured = [e for e in iter_events(handle) if e.get("bench_phase") == "measure"]
    rows = summarize(collect(measured))
    (out_dir / "summary.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    for name in unstable:
        fingerprints[name] = {"unstable": True}
    (out_dir / "fingerprints.json").write_text(
        json.dumps(fingerprints, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    meta = {
        "label": args.label,
        "version": os.environ.get("BENCH_VERSION") or "local",
        "host": platform.node(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "omp_num_threads": os.environ.get("OMP_NUM_THREADS"),
        "ocr_cpu_threads": settings.ocr_cpu_threads,
        "use_ner": settings.use_ner,
        "vlm": "off" if args.no_vlm else settings.vlm_base_url,
        "docs": [p.name for p in docs],
        "reps": args.reps,
        "warmup": args.warmup,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "unstable_docs": sorted(unstable),
    }
    (out_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"\n[{args.label}] 문서별 벽시계(초) — p50 / max")
    for name, values in sorted(wall.items()):
        ordered = sorted(values)
        print(f"  {name:<40}{ordered[len(ordered) // 2]:>8.1f}{ordered[-1]:>8.1f}")
    print(f"\n[{args.label}] 단계별(ms)")
    for row in rows:
        if row["event"] in ("ocr.job", "ocr.stage"):
            print(
                f"  {row['event']:<10}{row['group']:<18}n={row['n']:<4}"
                f"p50={row['p50']:<8}p95={row['p95']:<8}max={row['max']}"
            )
    if unstable:
        print(f"\n⚠ 반복마다 결과가 달라진 문서: {sorted(unstable)}")
    print(f"\n결과: {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
