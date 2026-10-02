"""Streaming ETL orchestrator: line-by-line extraction, text analytics, and checkpointing."""

from __future__ import annotations

import asyncio
import csv
import json
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nexus.pipeline.worker import WorkerPool, WorkItem, WorkResult


@dataclass(slots=True)
class PipelineStats:
    name: str
    total: int = 0
    processed: int = 0
    failed: int = 0
    retries: int = 0
    resumed: int = 0
    elapsed_seconds: float = 0.0

    @property
    def throughput(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.processed / self.elapsed_seconds


class Pipeline:
    """Three-stage streaming ETL pipeline with bounded memory and checkpoint resume."""

    def __init__(
        self,
        source: Path,
        sink: Path,
        workers: int = 8,
        max_attempts: int = 3,
        fail_on: str | None = None,
        resume: bool = False,
    ) -> None:
        self.source = source
        self.sink = sink
        self.workers = workers
        self.max_attempts = max_attempts
        self.fail_on = fail_on
        self.resume = resume

        self.checkpoint_path = self.sink.with_suffix(".checkpoint.json")
        self.stats = PipelineStats(name="nexus-etl")
        self.results: list[WorkResult[dict[str, Any]]] = []
        self._progress_cb: Callable[[WorkResult[dict[str, Any]]], None] | None = None
        self._items: list[WorkItem[dict[str, Any]]] | None = None
        self._resumed_ids: set[int] = set()

        if self.resume and self.checkpoint_path.exists():
            try:
                data = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
                self._resumed_ids = set(data.get("completed_ids", []))
            except Exception:
                self._resumed_ids = set()

    @property
    def total_items(self) -> int:
        return len(self._items) if self._items is not None else 0

    @property
    def resumed_count(self) -> int:
        return len(self._resumed_ids)

    def prepare(self) -> int:
        if self._items is None:
            self._items = list(self._iter_extract())
        self.stats.total = len(self._items)
        return len(self._items)

    def on_progress(self, cb: Callable[[WorkResult[dict[str, Any]]], None]) -> None:
        self._progress_cb = cb

    def _iter_extract(self) -> Iterator[WorkItem[dict[str, Any]]]:
        suffix = self.source.suffix.lower()
        if suffix == ".csv":
            with self.source.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    yield WorkItem(id=i, payload=dict(row))
        elif suffix == ".jsonl":
            with self.source.open("r", encoding="utf-8") as f:
                idx = 0
                for line in f:
                    s = line.strip()
                    if s:
                        yield WorkItem(id=idx, payload=json.loads(s))
                        idx += 1
        elif suffix == ".json":
            text = self.source.read_text(encoding="utf-8")
            parsed = json.loads(text)
            rows = parsed if isinstance(parsed, list) else [parsed]
            for i, row in enumerate(rows):
                yield WorkItem(id=i, payload=row)
        else:
            raise ValueError(f"Unsupported source format: {suffix}")

    async def _transform(self, item: WorkItem[dict[str, Any]]) -> WorkResult[dict[str, Any]]:
        row = dict(item.payload)
        await asyncio.sleep(0.01)

        raw_str = json.dumps(row)
        if self.fail_on and self.fail_on in raw_str:
            raise RuntimeError(f"Forced failure trigger matched for item {item.id}")

        text = str(row.get("content_markdown") or row.get("text") or row.get("content") or "")
        words = text.split()
        word_count = len(words)
        reading_time = round(max(0.1, word_count / 200.0), 1) if word_count > 0 else 0.0
        summary = text[:280].strip().replace("\n", " ") + ("..." if len(text) > 280 else "")

        row["word_count"] = word_count
        row["reading_time_mins"] = reading_time
        row["summary"] = summary
        row["processed"] = True

        return WorkResult(item_id=item.id, ok=True, value=row)

    def _load(self) -> None:
        mode = "a" if (self.resume and self.sink.exists()) else "w"
        self.results.sort(key=lambda r: r.item_id)
        ok = [r for r in self.results if r.ok and r.value is not None]
        with self.sink.open(mode, encoding="utf-8") as f:
            for r in ok:
                f.write(json.dumps(r.value, ensure_ascii=False) + "\n")

        failed = [r for r in self.results if not r.ok]
        if failed:
            err_path = self.sink.with_suffix(".errors.jsonl")
            err_mode = "a" if (self.resume and err_path.exists()) else "w"
            with err_path.open(err_mode, encoding="utf-8") as f:
                for r in failed:
                    f.write(json.dumps({"item_id": r.item_id, "error": r.error}) + "\n")

        all_completed = self._resumed_ids.union(r.item_id for r in self.results)
        self.checkpoint_path.write_text(
            json.dumps({"completed_ids": sorted(all_completed), "count": len(all_completed)}),
            encoding="utf-8",
        )

    async def run(self) -> PipelineStats:
        t0 = time.monotonic()
        self.prepare()
        items = self._items or []
        items_to_process = [it for it in items if it.id not in self._resumed_ids]

        pool: WorkerPool[dict[str, Any], dict[str, Any]] = WorkerPool(
            name="nexus-workers",
            size=self.workers,
            processor=self._transform,
            max_attempts=self.max_attempts,
        )
        pool.on_result = self._collect
        await pool.run()

        async def feeder() -> None:
            for item in items_to_process:
                await pool.submit(item)
            await pool.close()

        await asyncio.gather(feeder(), pool.join())
        await asyncio.sleep(0)

        self.stats.elapsed_seconds = max(0.001, time.monotonic() - t0)
        self.stats.processed = sum(1 for r in self.results if r.ok) + len(self._resumed_ids)
        self.stats.failed = sum(1 for r in self.results if not r.ok)
        self.stats.retries = pool.stats_retries
        self.stats.resumed = len(self._resumed_ids)
        self._load()
        return self.stats

    def _collect(self, result: WorkResult[dict[str, Any]]) -> None:
        self.results.append(result)
        if self._progress_cb is not None:
            self._progress_cb(result)

    def report(self) -> dict[str, Any]:
        return {
            "source": str(self.source),
            "sink": str(self.sink),
            "workers": self.workers,
            "processed": self.stats.processed,
            "failed": self.stats.failed,
            "retries": self.stats.retries,
            "resumed": self.stats.resumed,
            "elapsed_s": round(self.stats.elapsed_seconds, 2),
            "throughput_items_s": round(self.stats.throughput, 2),
        }
