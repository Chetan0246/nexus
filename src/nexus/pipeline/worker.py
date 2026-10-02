"""Bounded async worker pool with jittered exponential backoff and retry policy."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


@dataclass(slots=True)
class WorkItem(Generic[T]):
    id: int
    payload: T


@dataclass(slots=True)
class WorkResult(Generic[R]):
    item_id: int
    ok: bool
    value: R | None = None
    error: str | None = None
    attempts: int = 1


def exponential_backoff(attempt: int, base: float = 0.05, ceiling: float = 4.0) -> float:
    return min(ceiling, (base * (2 ** (attempt - 1)))) + random.uniform(0, 0.05)


class WorkerPool(Generic[T, R]):
    """Bounded worker pool that executes an async processor function over WorkItems."""

    def __init__(
        self,
        name: str,
        size: int,
        processor: Callable[[WorkItem[T]], Awaitable[WorkResult[R]]],
        max_attempts: int = 3,
        retry_delay: Callable[[int], float] = exponential_backoff,
    ) -> None:
        self.name = name
        self.size = size
        self.processor = processor
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay

        self._queue: asyncio.Queue[WorkItem[T] | None] = asyncio.Queue(maxsize=size * 4)
        self._tasks: list[asyncio.Task[None]] = []
        self._results: list[WorkResult[R]] = []
        self.stats_retries = 0
        self.on_result: Callable[[WorkResult[R]], None] | None = None

    @property
    def results(self) -> list[WorkResult[R]]:
        return self._results

    async def run(self) -> None:
        self._tasks = [
            asyncio.create_task(self._worker(i)) for i in range(self.size)
        ]

    async def submit(self, item: WorkItem[T]) -> None:
        await self._queue.put(item)

    async def close(self) -> None:
        for _ in range(self.size):
            await self._queue.put(None)

    async def join(self) -> None:
        await asyncio.gather(*self._tasks)

    async def _worker(self, worker_id: int) -> None:
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.task_done()
                break
            try:
                result = await self._process_with_retries(item)
                self._results.append(result)
                if self.on_result is not None:
                    self.on_result(result)
            finally:
                self._queue.task_done()

    async def _process_with_retries(self, item: WorkItem[T]) -> WorkResult[R]:
        attempt = 1
        last_error = ""
        while attempt <= self.max_attempts:
            try:
                res = await self.processor(item)
                res.attempts = attempt
                return res
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt == self.max_attempts:
                    break
                self.stats_retries += 1
                await asyncio.sleep(self.retry_delay(attempt))
                attempt += 1

        return WorkResult(
            item_id=item.id,
            ok=False,
            error=last_error or "unknown failure",
            attempts=attempt,
        )
