"""Log entry generation, ring buffer, and Rich markup rendering."""

from __future__ import annotations

import random
from collections import Counter, deque
from dataclasses import dataclass
from datetime import UTC, datetime

LEVELS = ("DEBUG", "INFO", "WARN", "ERROR")
LEVEL_WEIGHTS = (0.20, 0.65, 0.10, 0.05)
LEVEL_COLORS = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARN": "yellow",
    "ERROR": "bold red",
}


@dataclass(slots=True)
class LogEntry:
    timestamp: datetime
    level: str
    service: str
    message: str

    def render(self) -> str:
        color = LEVEL_COLORS.get(self.level, "white")
        ts = self.timestamp.strftime("%H:%M:%S")
        return (
            f"[dim]{ts}[/dim] "
            f"[{color}][{self.level:<5}][/{color}] "
            f"[bold magenta]{self.service:<10}[/bold magenta] {self.message}"
        )


class LogFeed:
    """Ring buffer of log lines, with weighted random log generation."""

    SERVICES = ("crawler", "pipeline", "api", "auth", "ws-hub", "db")
    TEMPLATES = {
        "crawler": [
            "Fetched {url} ({bytes} bytes, 200 OK)",
            "Extracted {links} links from {url}",
            "robots.txt parsed for {host}",
            "Rate-limiting host {host} for {delay}s",
        ],
        "pipeline": [
            "Transformed WorkItem #{id} (word_count={words})",
            "Sinking {count} items to output.jsonl",
            "Checkpoint flushed: {count} items completed",
            "Worker #{worker} retrying item #{id}",
        ],
        "api": [
            "POST /docs 201 Created ({ms}ms)",
            "GET /docs/search?q={query} 200 OK ({ms}ms)",
            "GET /health 200 OK",
            "Rate limiter hit: client={ip}",
        ],
        "auth": [
            "User registered: {email}",
            "User logged in: {email}",
            "Token refreshed for user #{id}",
            "Session revoked: token_id={id}",
        ],
        "ws-hub": [
            "Client connected to room '{room}'",
            "Broadcasted message to {count} peers in '{room}'",
            "Client disconnected from room '{room}'",
        ],
        "db": [
            "Executing async transaction",
            "Connection pool: active={active}, idle={idle}",
            "Flushed ORM session",
        ],
    }

    def __init__(self, buffer_size: int = 400, seed: int | None = None) -> None:
        self.entries: deque[LogEntry] = deque(maxlen=buffer_size)
        self._rng = random.Random(seed)

    def generate(self) -> LogEntry:
        level = self._rng.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
        service = self._rng.choice(self.SERVICES)
        template = self._rng.choice(self.TEMPLATES[service])

        data = {
            "url": "https://docs.python.org/3/library",
            "bytes": self._rng.randint(1200, 48000),
            "links": self._rng.randint(2, 35),
            "host": "docs.python.org",
            "delay": round(self._rng.uniform(0.5, 2.0), 1),
            "id": self._rng.randint(100, 9999),
            "words": self._rng.randint(80, 2400),
            "count": self._rng.randint(10, 500),
            "worker": self._rng.randint(1, 8),
            "ms": round(self._rng.uniform(1.2, 45.0), 1),
            "query": "asyncio",
            "ip": f"192.168.1.{self._rng.randint(2, 254)}",
            "email": "dev@nexus.local",
            "room": "global-feed",
            "active": self._rng.randint(1, 10),
            "idle": self._rng.randint(2, 20),
        }
        msg = template.format(**data)
        entry = LogEntry(
            timestamp=datetime.now(UTC),
            level=level,
            service=service,
            message=msg,
        )
        self.entries.append(entry)
        return entry

    def level_stats(self) -> dict[str, int]:
        counts = Counter(e.level for e in self.entries)
        return {lvl: counts[lvl] for lvl in LEVELS}
