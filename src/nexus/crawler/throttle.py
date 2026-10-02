"""Per-host throttling and domain filtering."""

from __future__ import annotations

import asyncio
import urllib.parse
from collections import defaultdict


class HostThrottle:
    """Enforces polite per-host spacing between HTTP requests."""

    def __init__(self, default_delay: float = 1.0) -> None:
        self.default_delay = default_delay
        self._delays: dict[str, float] = {}
        self._last_hit: dict[str, float] = defaultdict(float)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def set_delay(self, host: str, delay: float) -> None:
        self._delays[host.lower()] = delay

    def get_delay(self, host: str) -> float:
        return self._delays.get(host.lower(), self.default_delay)

    async def wait(self, url: str) -> None:
        host = urllib.parse.urlsplit(url).netloc.lower()
        delay = self.get_delay(host)
        if delay <= 0:
            return

        async with self._locks[host]:
            loop = asyncio.get_running_loop()
            now = loop.time()
            elapsed = now - self._last_hit[host]
            if elapsed < delay:
                await asyncio.sleep(delay - elapsed)
            self._last_hit[host] = loop.time()


class DomainFilter:
    """Enforces allowed domain scope for crawling."""

    def __init__(self, seed_hosts: set[str], follow_external: bool = False) -> None:
        self.allowed_hosts = {h.lower() for h in seed_hosts}
        self.follow_external = follow_external

    def allows(self, url: str) -> bool:
        if self.follow_external:
            return True
        host = urllib.parse.urlsplit(url).netloc.lower()
        if not host:
            return False
        for allowed in self.allowed_hosts:
            if host == allowed or host.endswith(f".{allowed}"):
                return True
        return False
