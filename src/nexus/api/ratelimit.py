"""Sliding-window rate limiter per client IP."""

from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status

from nexus.config import get_settings


class SlidingWindowLimiter:
    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window = window_seconds
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def _prune(self, key: str, now: float) -> None:
        q = self._hits[key]
        while q and now - q[0] >= self.window:
            q.popleft()

    def hit(self, key: str) -> tuple[bool, int]:
        now = time.monotonic()
        self._prune(key, now)
        q = self._hits[key]
        if len(q) >= self.max_requests:
            retry_after = max(1, int(self.window - (now - q[0])) + 1)
            return False, retry_after
        q.append(now)
        return True, 0


_settings = get_settings()
limiter = SlidingWindowLimiter(
    _settings.rate_limit_requests, _settings.rate_limit_window_seconds
)


def rate_limit(request: Request) -> None:
    client_ip = request.client.host if request.client else "anonymous"
    allowed, retry_after = limiter.hit(client_ip)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(retry_after)},
        )


RateLimitDep = Depends(rate_limit)
RateLimited = Annotated[None, Depends(rate_limit)]
