"""Tests for Crawler module: HostThrottle, DomainFilter, RobotsCache, and Engine."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nexus.crawler.engine import CrawlConfig, Crawler
from nexus.crawler.robots import HostRules, RobotsCache
from nexus.crawler.throttle import DomainFilter, HostThrottle


def test_domain_filter() -> None:
    df = DomainFilter(seed_hosts={"example.com", "sub.example.com"}, follow_external=False)
    assert df.allows("https://example.com/page1")
    assert df.allows("https://sub.example.com/api")
    assert not df.allows("https://malicious.org/phish")

    df_ext = DomainFilter(seed_hosts={"example.com"}, follow_external=True)
    assert df_ext.allows("https://external.org/test")


@pytest.mark.asyncio
async def test_host_throttle() -> None:
    throttle = HostThrottle(default_delay=0.05)
    t0 = asyncio.get_event_loop().time()
    await throttle.wait("https://example.com")
    await throttle.wait("https://example.com")
    t1 = asyncio.get_event_loop().time()
    assert (t1 - t0) >= 0.04


def test_crawler_url_normalization() -> None:
    raw = "HTTPS://Example.COM:443/docs/index.html?ref=1#top"
    norm = Crawler.normalize(raw)
    assert norm == "https://example.com:443/docs/index.html?ref=1"

    assert Crawler.is_crawlable("https://example.com/page")
    assert not Crawler.is_crawlable("https://example.com/image.png")
    assert not Crawler.is_crawlable("ftp://example.com/file.txt")


@pytest.mark.asyncio
async def test_robots_cache() -> None:
    fake_robots = (
        "User-agent: *\n"
        "Disallow: /\n"
        "Allow: /$\n"
        "Allow: /home$\n"
        "Allow: /public/\n"
        "Disallow: /admin/\n"
        "Crawl-delay: 2\n"
    )

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.text = AsyncMock(return_value=fake_robots)

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    rc = RobotsCache(session=mock_session, user_agent="NexusEngine/1.0")
    rules = await rc.for_url("https://example.com")

    assert rules.is_allowed("/") is True
    assert rules.is_allowed("/home") is True
    assert rules.is_allowed("/public/page") is True
    assert rules.is_allowed("/secret") is False
    assert rules.is_allowed("/admin/secrets") is False
    assert rules.crawl_delay == 2.0


@pytest.mark.asyncio
async def test_crawler_html_to_markdown() -> None:
    html = """
    <!DOCTYPE html>
    <html>
      <head><title>Nexus Architecture</title></head>
      <body>
        <nav><a href="/home">Home</a></nav>
        <article>
          <h1>Nexus Engine</h1>
          <p>Autonomous data intelligence and streaming ETL platform.</p>
          <a href="/docs/guide">Documentation</a>
        </article>
      </body>
    </html>
    """

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.headers = {"Content-Type": "text/html; charset=utf-8"}
    mock_resp.read = AsyncMock(return_value=html.encode("utf-8"))
    mock_resp.url = "https://nexus-test.io"
    mock_content = AsyncMock()
    mock_content.read = AsyncMock(return_value=html.encode("utf-8"))
    mock_resp.content = mock_content

    mock_get = MagicMock()
    mock_get.return_value.__aenter__.return_value = mock_resp

    cfg = CrawlConfig(
        start_urls=["https://nexus-test.io"],
        max_pages=5,
        max_depth=1,
        concurrency=1,
        delay=0.01,
    )
    crawler = Crawler(cfg)

    with (
        patch("aiohttp.ClientSession.get", mock_get),
        patch(
            "nexus.crawler.robots.RobotsCache.for_url",
            AsyncMock(return_value=HostRules(fetched_ok=False)),
        ),
    ):
        pages = await crawler.run()

    assert len(pages) >= 1
    page = pages[0]
    assert page.title == "Nexus Architecture"
    assert page.content_markdown is not None
    assert "Nexus Engine" in page.content_markdown
    assert "https://nexus-test.io/docs/guide" in page.links
