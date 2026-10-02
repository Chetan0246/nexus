"""Configuration settings loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "nexus"
    app_env: str = "dev"
    secret_key: str = "nexus-super-secret-dev-key-change-in-production-12345"
    jwt_algorithm: str = "HS256"
    access_token_ttl_minutes: int = 30
    database_url: str = "sqlite+aiosqlite:///nexus.db"
    cors_origins: str = "*"

    rate_limit_requests: int = 120
    rate_limit_window_seconds: float = 60.0

    @property
    def cors_origin_list(self) -> list[str]:
        if self.cors_origins.strip() == "*":
            return ["*"]
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
