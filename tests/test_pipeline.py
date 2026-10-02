"""Tests for streaming ETL pipeline: WorkerPool, retry backoff, and checkpoint resume."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nexus.pipeline.orchestrator import Pipeline
from nexus.pipeline.worker import WorkerPool, WorkItem, WorkResult


@pytest.mark.asyncio
async def test_worker_pool_success() -> None:
    async def square_processor(item: WorkItem[int]) -> WorkResult[int]:
        return WorkResult(item_id=item.id, ok=True, value=item.payload * item.payload)

    results: list[WorkResult[int]] = []
    pool: WorkerPool[int, int] = WorkerPool(
        name="test-pool", size=3, processor=square_processor, max_attempts=2
    )
    pool.on_result = results.append
    await pool.run()

    for i in range(5):
        await pool.submit(WorkItem(id=i, payload=i))
    await pool.close()
    await pool.join()

    assert len(results) == 5
    assert all(r.ok for r in results)
    square_map = {r.item_id: r.value for r in results}
    assert square_map[3] == 9
    assert square_map[4] == 16


@pytest.mark.asyncio
async def test_worker_pool_retry_and_failure() -> None:
    attempts: dict[int, int] = {}

    async def flaky_processor(item: WorkItem[int]) -> WorkResult[int]:
        attempts[item.id] = attempts.get(item.id, 0) + 1
        if attempts[item.id] < 3:
            raise ValueError(f"Transient error on item {item.id}")
        return WorkResult(item_id=item.id, ok=True, value=item.payload * 10)

    results: list[WorkResult[int]] = []
    pool: WorkerPool[int, int] = WorkerPool(
        name="flaky-pool", size=2, processor=flaky_processor, max_attempts=3
    )
    pool.on_result = results.append
    await pool.run()

    await pool.submit(WorkItem(id=1, payload=5))
    await pool.close()
    await pool.join()

    assert len(results) == 1
    assert results[0].ok is True
    assert results[0].value == 50
    assert attempts[1] == 3
    assert pool.stats_retries == 2


@pytest.mark.asyncio
async def test_pipeline_streaming_and_checkpoint(tmp_path: Path) -> None:
    source = tmp_path / "raw_docs.jsonl"
    sink = tmp_path / "enriched.jsonl"

    raw_data = [
        {
            "url": "https://nexus.io/intro",
            "content_markdown": "# Introduction\n\nNexus is an autonomous data platform.",
        },
        {
            "url": "https://nexus.io/guide",
            "content_markdown": "# Guide\n\nComprehensive streaming ETL with checkpointing.",
        },
        {
            "url": "https://nexus.io/faq",
            "content_markdown": "# FAQ\n\nFrequently asked questions and troubleshooting.",
        },
    ]

    with source.open("w", encoding="utf-8") as f:
        for r in raw_data:
            f.write(json.dumps(r) + "\n")

    # Run 1: process all records
    pipe = Pipeline(source=source, sink=sink, workers=2, resume=False)
    stats = await pipe.run()

    assert stats.total == 3
    assert stats.processed == 3
    assert stats.failed == 0
    assert sink.exists()

    enriched_lines = [json.loads(line) for line in sink.read_text(encoding="utf-8").splitlines()]
    assert len(enriched_lines) == 3
    assert "word_count" in enriched_lines[0]
    assert "reading_time_mins" in enriched_lines[0]
    assert "summary" in enriched_lines[0]
    assert enriched_lines[0]["word_count"] > 0

    # Run 2: resume should recognize completed items from checkpoint
    pipe_resume = Pipeline(source=source, sink=sink, workers=2, resume=True)
    resume_stats = await pipe_resume.run()

    assert resume_stats.resumed == 3
    assert resume_stats.processed == 3
