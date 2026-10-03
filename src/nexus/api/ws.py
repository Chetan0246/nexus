"""In-process async pub/sub connection hub for WebSocket rooms."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass(eq=False)
class Subscriber:
    queue: asyncio.Queue[str] = field(default_factory=lambda: asyncio.Queue(maxsize=500))


class ConnectionHub:
    """Fan-out message bus keyed by room name."""

    def __init__(self) -> None:
        self._rooms: dict[str, set[Subscriber]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, room: str) -> Subscriber:
        sub = Subscriber()
        async with self._lock:
            self._rooms.setdefault(room, set()).add(sub)
        return sub

    async def unsubscribe(self, room: str, sub: Subscriber) -> None:
        async with self._lock:
            room_set = self._rooms.get(room)
            if room_set is not None:
                room_set.discard(sub)
                if not room_set:
                    self._rooms.pop(room, None)

    async def publish(self, room: str, message: str) -> int:
        async with self._lock:
            subs = list(self._rooms.get(room, ()))
        for sub in subs:
            if sub.queue.full():
                try:
                    sub.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                sub.queue.put_nowait(message)
            except asyncio.QueueFull:
                pass
        return len(subs)

    def room_count(self) -> dict[str, int]:
        return {room: len(subs) for room, subs in self._rooms.items()}
