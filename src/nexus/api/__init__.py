"""Nexus API package: FastAPI backend, WebSocket pub/sub, and JWT auth."""

from nexus.api.server import create_app

__all__ = ["create_app"]
