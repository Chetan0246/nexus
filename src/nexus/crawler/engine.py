"""Autonomous async crawler engine with Markdown extraction and BFS frontier."""

from __future__ import annotations

import asyncio
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import aiohttp
from bs4 import BeautifulSoup

from nexus.crawler.robots import RobotsCache
from nexus.crawler.throttle import DomainFilter, HostThrottle

USER_AGENT = "NexusEngine/1.0 (+autonomous-research-agent; python-aiohttp)"
MAX_PAGE_BYTES = 3 * 1024 * 1024  # 3 MiB safety cap

SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".pdf", ".zip", ".gz", ".tar", ".mp3", ".mp4", ".avi", ".mov",
    ".woff", ".woff2", ".ttf", ".eot", ".css", ".js",
}


@dataclass(slots=True)
class CrawledPage:
    url: str
    host: str
    status: int | None
    title: str | None
    text_len: int
    content_markdown: str | None
    content_type: str | None
    links: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass(slots=True)
class CrawlConfig:
    start_urls: list[str]
    max_pages: int = 50
    max_depth: int = 3
    concurrency: int = 8
    delay: float = 0.5
    timeout: float = 15.0
    follow_external: bool = False
    ignore_robots: bool = False


@dataclass(slots=True)
class CrawlReport:
    fetched: int = 0
    failed: int = 0
    skipped_robots: int = 0
    skipped_domain: int = 0
    duplicates: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fetched": self.fetched,
            "failed": self.failed,
            "skipped_robots": self.skipped_robots,
            "skipped_domain": self.skipped_domain,
            "duplicates": self.duplicates,
        }


class Crawler:
    """Polite asynchronous crawler yielding CrawledPage records."""

    def __init__(self, config: CrawlConfig) -> None:
        self.config = config
        self.report = CrawlReport()
        self.throttle = HostThrottle(default_delay=config.delay)
        self.domain_filter = DomainFilter(
            {urllib.parse.urlsplit(u).netloc for u in config.start_urls},
            follow_external=config.follow_external,
        )
        self._session: aiohttp.ClientSession | None = None
        self._robots: RobotsCache | None = None
        self._seen: set[str] = set()
        self._pages: list[CrawledPage] = []
        self._progress_cb: Callable[[CrawledPage, int], None] | None = None

    def on_progress(self, cb: Callable[[CrawledPage, int], None]) -> None:
        self._progress_cb = cb

    @staticmethod
    def normalize(url: str) -> str:
        p = urllib.parse.urlsplit(url)
        path = p.path or "/"
        return urllib.parse.urlunsplit(
            (p.scheme.lower(), p.netloc.lower(), path, p.query, "")
        )

    @staticmethod
    def is_crawlable(url: str) -> bool:
        p = urllib.parse.urlsplit(url)
        if p.scheme not in ("http", "https"):
            return False
        ext = urllib.parse.urlsplit(url).path.lower()
        return not any(ext.endswith(e) for e in SKIP_EXTENSIONS)

    async def run(self) -> list[CrawledPage]:
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        connector = aiohttp.TCPConnector(
            limit=self.config.concurrency, ttl_dns_cache=300
        )

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"User-Agent": USER_AGENT},
        ) as session:
            self._session = session
            self._robots = RobotsCache(session, USER_AGENT)
            queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()

            for url in self.config.start_urls:
                norm = self.normalize(url)
                if self.domain_filter.allows(norm) and self.is_crawlable(norm):
                    queue.put_nowait((norm, 0))
                    self._seen.add(norm)

            workers = [
                asyncio.create_task(self._worker(queue))
                for _ in range(self.config.concurrency)
            ]
            await queue.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        return self._pages

    async def _worker(self, queue: asyncio.Queue[tuple[str, int]]) -> None:
        while True:
            url, depth = await queue.get()
            try:
                await self._process(url, depth, queue)
            finally:
                queue.task_done()

    async def _process(
        self, url: str, depth: int, queue: asyncio.Queue[tuple[str, int]]
    ) -> None:
        if self.report.fetched + self.report.failed >= self.config.max_pages:
            return

        if not self.config.ignore_robots:
            assert self._robots is not None
            rules = await self._robots.for_url(url)
            path = urllib.parse.urlsplit(url).path or "/"
            if rules.fetched_ok and not rules.is_allowed(path):
                self.report.skipped_robots += 1
                return
            if rules.crawl_delay:
                self.throttle.set_delay(
                    urllib.parse.urlsplit(url).netloc, rules.crawl_delay
                )

        await self.throttle.wait(url)
        assert self._session is not None

        try:
            body: bytes | None = None
            final_url = url
            for attempt in range(2):
                async with self._session.get(url, allow_redirects=True) as resp:
                    if resp.status in (429, 503) and attempt == 0:
                        retry_after = float(resp.headers.get("Retry-After", "1.5"))
                        await asyncio.sleep(min(retry_after, 3.0))
                        continue

                    ctype = resp.headers.get("Content-Type", "")
                    if resp.status != 200 or "text/html" not in ctype:
                        self.report.failed += 1
                        page = CrawledPage(
                            url=str(resp.url),
                            host=urllib.parse.urlsplit(str(resp.url)).netloc,
                            status=resp.status,
                            title=None,
                            text_len=0,
                            content_markdown=None,
                            content_type=ctype,
                        )
                        self._pages.append(page)
                        return
                    raw_body = await resp.read()
                    body = raw_body[:MAX_PAGE_BYTES] if len(raw_body) > MAX_PAGE_BYTES else raw_body
                    final_url = str(resp.url)
                    break

            if body is None:
                return

            title, text_len, links, markdown = self._extract(body, final_url)
            page = CrawledPage(
                url=final_url,
                host=urllib.parse.urlsplit(final_url).netloc,
                status=200,
                title=title,
                text_len=text_len,
                content_markdown=markdown,
                content_type="text/html",
                links=links,
            )
            self._pages.append(page)
            self.report.fetched += 1

            if self._progress_cb:
                self._progress_cb(page, self.report.fetched)

            # enqueue links
            if depth + 1 <= self.config.max_depth:
                for link in links:
                    norm = self.normalize(link)
                    if norm in self._seen:
                        self.report.duplicates += 1
                        continue
                    self._seen.add(norm)
                    if not self.domain_filter.allows(norm):
                        self.report.skipped_domain += 1
                        continue
                    if not self.is_crawlable(norm):
                        continue
                    if self.report.fetched + self.report.failed >= self.config.max_pages:
                        break
                    queue.put_nowait((norm, depth + 1))

        except aiohttp.ClientError as exc:
            self.report.failed += 1
            self.report.errors.append(f"{url}: {exc}")
            page = CrawledPage(
                url=url,
                host=urllib.parse.urlsplit(url).netloc,
                status=None,
                title=None,
                text_len=0,
                content_markdown=None,
                content_type=None,
                error=str(exc),
            )
            self._pages.append(page)

    @staticmethod
    def _extract(body: bytes, base_url: str) -> tuple[str | None, int, list[str], str]:
        soup = BeautifulSoup(body, "lxml")
        title = soup.title.string.strip() if soup.title and soup.title.string else None

        links: list[str] = []
        for anchor in soup.find_all("a", href=True):
            absolute = urllib.parse.urljoin(base_url, anchor["href"])
            if urllib.parse.urlsplit(absolute).scheme in ("http", "https"):
                links.append(absolute.split("#")[0])

        noise = [
            "script", "style", "noscript", "template", "svg",
            "nav", "footer", "header", "aside"
        ]
        for tag in soup(noise):
            tag.decompose()

        for i in range(1, 7):
            for h in soup.find_all(f"h{i}"):
                h.string = f"\n{'#' * i} {h.get_text().strip()}\n"

        for li in soup.find_all("li"):
            li.string = f"\n- {li.get_text().strip()}"

        raw_text = soup.get_text("\n", strip=True)
        text_len = len(raw_text)

        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        markdown = "\n\n".join(lines)
        if title and not markdown.startswith("#"):
            markdown = f"# {title}\n\n{markdown}"

        return title, text_len, links, markdown
