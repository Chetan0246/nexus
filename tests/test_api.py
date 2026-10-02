"""Tests for REST and WebSocket API: Auth flow, Document CRUD, and Health endpoint."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from nexus.models import User


@pytest.mark.asyncio
async def test_health_endpoint(client: AsyncClient) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["app"] == "nexus"
    assert "uptime_seconds" in data


@pytest.mark.asyncio
async def test_auth_registration_and_login(client: AsyncClient) -> None:
    # 1. Register new user
    reg_resp = await client.post(
        "/auth/register",
        json={"email": "researcher@nexus.ai", "password": "StrongSecretPass123!"},
    )
    assert reg_resp.status_code == 201
    reg_data = reg_resp.json()
    assert reg_data["email"] == "researcher@nexus.ai"
    assert reg_data["role"] == "user"

    # 2. Prevent duplicate email
    dup_resp = await client.post(
        "/auth/register",
        json={"email": "researcher@nexus.ai", "password": "AnotherPassword456!"},
    )
    assert dup_resp.status_code == 409

    # 3. Successful login
    login_resp = await client.post(
        "/auth/login",
        json={"email": "researcher@nexus.ai", "password": "StrongSecretPass123!"},
    )
    assert login_resp.status_code == 200
    tokens = login_resp.json()
    assert "access_token" in tokens
    assert "refresh_token" in tokens

    # 4. Access /auth/me with Bearer token
    headers = {"Authorization": f"Bearer {tokens['access_token']}"}
    me_resp = await client.get("/auth/me", headers=headers)
    assert me_resp.status_code == 200
    assert me_resp.json()["email"] == "researcher@nexus.ai"

    # 5. Rotate token via /auth/refresh
    refresh_resp = await client.post(
        "/auth/refresh",
        json={"refresh_token": tokens["refresh_token"]},
    )
    assert refresh_resp.status_code == 200
    new_tokens = refresh_resp.json()
    assert "access_token" in new_tokens

    # 6. Logout and revoke token
    logout_resp = await client.post(
        "/auth/logout",
        headers=headers,
        json={"refresh_token": tokens["refresh_token"]},
    )
    assert logout_resp.status_code == 204


@pytest.mark.asyncio
async def test_documents_crud_and_search(
    client: AsyncClient, test_user: User, auth_headers: dict[str, str]
) -> None:
    # 1. Create document
    doc_payload = {
        "url": "https://nexus.dev/docs/intro",
        "host": "nexus.dev",
        "title": "Getting Started with Nexus",
        "content_markdown": "# Nexus Guide\n\nHigh-performance ingestion and ETL.",
        "text_len": 46,
        "word_count": 8,
        "reading_time_mins": 0.1,
        "summary": "Nexus Guide high performance ingestion.",
    }
    create_resp = await client.post("/docs", headers=auth_headers, json=doc_payload)
    assert create_resp.status_code == 201
    created_doc = create_resp.json()
    doc_id = created_doc["id"]
    assert created_doc["title"] == "Getting Started with Nexus"

    # 2. Retrieve document by ID
    get_resp = await client.get(f"/docs/{doc_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == doc_id

    # 3. Search documents
    search_resp = await client.get("/docs?q=Getting")
    assert search_resp.status_code == 200
    search_data = search_resp.json()
    assert search_data["total"] >= 1
    assert any(d["id"] == doc_id for d in search_data["items"])

    # 4. Delete document
    del_resp = await client.delete(f"/docs/{doc_id}", headers=auth_headers)
    assert del_resp.status_code == 204

    # 5. Verify 404 after deletion
    not_found = await client.get(f"/docs/{doc_id}")
    assert not_found.status_code == 404
