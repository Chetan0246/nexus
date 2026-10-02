"""Pydantic v2 schemas for API requests, responses, and events."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field


# ------------------------------------------------------------------ auth ---
class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    role: str = "user"


class UserRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    email: EmailStr
    role: str = "user"
    is_active: bool
    created_at: datetime


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str | None = None
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    refresh_token: str


# ------------------------------------------------------------- documents ---
class DocumentCreate(BaseModel):
    url: str
    host: str
    title: str | None = None
    content_markdown: str | None = None
    text_len: int = 0
    word_count: int = 0
    reading_time_mins: float = 0.0
    summary: str | None = None
    status_code: int = 200


class DocumentRead(DocumentCreate):
    model_config = ConfigDict(from_attributes=True)

    id: int
    owner_id: int | None = None
    created_at: datetime


class DocumentList(BaseModel):
    items: list[DocumentRead]
    total: int
    page: int
    page_size: int


# ---------------------------------------------------------------- events ---
class WSInMessage(BaseModel):
    room: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    text: str = Field(min_length=1, max_length=2000)


class WSOutMessage(BaseModel):
    room: str
    user_id: int | str
    text: str
    ts: datetime
