"""Tests for end-to-end Ingestion and Typer CLI execution."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from nexus.cli import app
from nexus.crawler.engine import CrawledPage

runner = CliRunner()


def test_cli_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "Nexus Platform" in result.stdout


def test_cli_init_db() -> None:
    result = runner.invoke(app, ["init-db"])
    assert result.exit_code == 0
    assert "[OK] Database schema initialized successfully." in result.stdout


def test_end_to_end_ingest_flow() -> None:
    mock_pages = [
        CrawledPage(
            url="https://test.nexus.io/articles/autonomous-agents",
            host="test.nexus.io",
            status=200,
            title="Autonomous Agents & Distributed Systems",
            text_len=120,
            content_markdown="# Autonomous Agents\n\nBuilding self-healing pipelines and engines.",
            content_type="text/html",
            links=["https://test.nexus.io/about"],
        )
    ]

    with patch("nexus.crawler.engine.Crawler.run", AsyncMock(return_value=mock_pages)):
        result = runner.invoke(
            app,
            ["ingest", "https://test.nexus.io", "--max-pages", "1", "--max-depth", "1"],
        )
        assert result.exit_code == 0
        assert "Ingested 1 Documents into Nexus DB" in result.stdout
        assert "Autonomous Agents" in result.stdout
        assert "Distributed Systems" in result.stdout
