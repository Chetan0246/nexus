"""Nexus unified CLI: crawler, pipeline, serve, monitor, and end-to-end ingest."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from nexus import __version__
from nexus.config import get_settings

app = typer.Typer(
    name="nexus",
    help="Nexus: Autonomous Web Ingestion, Streaming ETL, REST/WS API & Operations Monitor",
    no_args_is_help=True,
    add_completion=False,
)
console = Console()


@app.command(name="version")
def version_cmd() -> None:
    """Print Nexus version and environment configuration."""
    settings = get_settings()
    console.print(
        Panel.fit(
            f"[bold cyan]Nexus Platform[/bold cyan] v{__version__}\n"
            f"[dim]Environment:[/dim] [yellow]{settings.app_env}[/yellow]\n"
            f"[dim]Database:[/dim] {settings.database_url}\n"
            f"[dim]Token TTL:[/dim] {settings.access_token_ttl_minutes}m access / 7d refresh",
            title="System Info",
            border_style="cyan",
        )
    )


@app.command(name="init-db")
def init_db_cmd() -> None:
    """Initialize database tables and schema."""
    from nexus.db import init_db

    async def _init() -> None:
        await init_db()

    with console.status("[bold green]Creating database tables..."):
        asyncio.run(_init())
    console.print("[bold green][OK][/bold green] Database schema initialized successfully.")


@app.command(name="crawl")
def crawl_cmd(
    urls: Annotated[list[str], typer.Argument(help="Starting URLs to crawl")],
    max_pages: Annotated[int, typer.Option("--max-pages", "-p", help="Max pages")] = 20,
    max_depth: Annotated[int, typer.Option("--max-depth", "-d", help="Max depth")] = 2,
    concurrency: Annotated[int, typer.Option("--concurrency", "-c", help="Concurrency")] = 4,
    delay: Annotated[float, typer.Option("--delay", help="Per-host delay seconds")] = 0.5,
    timeout: Annotated[float, typer.Option("--timeout", help="HTTP timeout seconds")] = 10.0,
    output: Annotated[Path | None, typer.Option("--output", "-o", help="Sink JSONL file")] = None,
    follow_external: Annotated[
        bool, typer.Option("--follow-external", help="Follow out-of-domain links")
    ] = False,
) -> None:
    """Run polite asynchronous web crawler and extract clean Markdown."""
    from nexus.crawler.engine import CrawlConfig, Crawler

    cfg = CrawlConfig(
        start_urls=urls,
        max_pages=max_pages,
        max_depth=max_depth,
        concurrency=concurrency,
        delay=delay,
        timeout=timeout,
        follow_external=follow_external,
    )
    crawler = Crawler(cfg)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("[cyan]Crawling web pages...", total=max_pages)

        def on_page(_p: object, count: int) -> None:
            progress.update(task, completed=count)

        crawler.on_progress(on_page)
        pages = asyncio.run(crawler.run())

    table = Table(title="Nexus Crawl Results", show_lines=True)
    table.add_column("Status", justify="center", style="bold")
    table.add_column("Title", style="white")
    table.add_column("URL", style="dim", overflow="fold")
    table.add_column("Markdown Len", justify="right")

    for page in pages:
        st_color = "green" if (page.status or 0) < 400 else "red"
        st_str = f"[{st_color}]{page.status or 'ERR'}[/{st_color}]"
        table.add_row(st_str, page.title or "(untitled)", page.url, str(page.text_len))

    console.print(table)

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as f:
            for page in pages:
                rec = {
                    "url": page.url,
                    "host": page.host,
                    "status": page.status,
                    "title": page.title,
                    "text_len": page.text_len,
                    "content_markdown": page.content_markdown,
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        console.print(
            f"[green][OK][/green] Wrote {len(pages)} crawled pages to [bold]{output}[/bold]"
        )


@app.command(name="pipeline")
def pipeline_cmd(
    source: Annotated[Path, typer.Argument(help="Source file (.jsonl, .json, or .csv)")],
    sink: Annotated[Path, typer.Argument(help="Sink output file (.jsonl)")],
    workers: Annotated[int, typer.Option("--workers", "-w", help="Worker pool size")] = 4,
    max_attempts: Annotated[int, typer.Option("--max-attempts", help="Retry attempts")] = 3,
    resume: Annotated[bool, typer.Option("--resume/--no-resume", help="Resume checkpoint")] = False,
    fail_on: Annotated[
        str | None, typer.Option("--fail-on", help="String trigger to test retries")
    ] = None,
) -> None:
    """Run streaming ETL pipeline with worker pool, text analytics, and checkpointing."""
    from nexus.pipeline.orchestrator import Pipeline

    if not source.exists():
        console.print(f"[bold red]Error:[/bold red] Source file '{source}' not found.")
        raise typer.Exit(code=1)

    pipe = Pipeline(
        source=source,
        sink=sink,
        workers=workers,
        max_attempts=max_attempts,
        resume=resume,
        fail_on=fail_on,
    )
    total = pipe.prepare()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("[magenta]Processing ETL pipeline...", total=total)

        def on_prog(_res: object) -> None:
            progress.advance(task)

        pipe.on_progress(on_prog)
        stats = asyncio.run(pipe.run())

    summary = Table(title="Pipeline Execution Summary", border_style="magenta")
    summary.add_column("Metric", style="bold cyan")
    summary.add_column("Value", style="bold white")

    summary.add_row("Total Items", str(stats.total))
    summary.add_row("Processed (OK)", f"[green]{stats.processed}[/green]")
    summary.add_row("Failed", f"[red]{stats.failed}[/red]")
    summary.add_row("Retries Encountered", str(stats.retries))
    summary.add_row("Resumed from Checkpoint", str(stats.resumed))
    summary.add_row("Duration", f"{stats.elapsed_seconds:.2f}s")
    summary.add_row("Throughput", f"{stats.throughput:.1f} items/s")

    console.print(summary)
    console.print(f"[green][OK][/green] Saved enriched output to [bold]{sink}[/bold]")


@app.command(name="serve")
def serve_cmd(
    host: Annotated[str, typer.Option("--host", "-h", help="Bind host")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", "-p", help="Bind port")] = 8000,
    reload: Annotated[bool, typer.Option("--reload", help="Enable live auto-reload")] = False,
    workers: Annotated[int, typer.Option("--workers", "-w", help="Number of uvicorn workers")] = 1,
) -> None:
    """Launch the Nexus FastAPI REST and WebSocket intelligence server."""
    import uvicorn

    console.print(
        Panel.fit(
            f"[bold green]Starting Nexus API Server[/bold green]\n"
            f"[dim]Endpoints:[/dim] http://{host}:{port}\n"
            f"[dim]API Docs:[/dim]  http://{host}:{port}/swagger\n"
            f"[dim]Live WS:[/dim]   ws://{host}:{port}/ws/operations",
            title="Nexus Service",
            border_style="green",
        )
    )
    uvicorn.run("nexus.api.server:app", host=host, port=port, reload=reload, workers=workers)


@app.command(name="monitor")
def monitor_cmd(
    simulated: Annotated[bool, typer.Option("--simulated", help="Simulate metrics")] = False,
    interval: Annotated[float, typer.Option("--interval", help="Refresh rate seconds")] = 0.5,
) -> None:
    """Launch live Textual operations dashboard with sparklines and log streaming."""
    from nexus.monitor.dashboard import DashboardApp

    app_instance = DashboardApp(tick_interval=interval, simulated=simulated)
    app_instance.run()


@app.command(name="ingest")
def ingest_cmd(
    urls: Annotated[list[str], typer.Argument(help="Starting URLs to ingest")],
    max_pages: Annotated[int, typer.Option("--max-pages", "-p", help="Max pages")] = 10,
    max_depth: Annotated[int, typer.Option("--max-depth", "-d", help="Crawl depth")] = 1,
    concurrency: Annotated[int, typer.Option("--concurrency", "-c", help="Crawl concurrency")] = 4,
    delay: Annotated[float, typer.Option("--delay", help="Per-host crawl delay")] = 0.3,
    output: Annotated[
        Path | None, typer.Option("--output", "-o", help="Optional backup JSONL sink")
    ] = None,
) -> None:
    """Full end-to-end ingestion: Crawl -> ETL Analytics -> DB Persistence."""
    from sqlalchemy import select

    from nexus.crawler.engine import CrawlConfig, Crawler
    from nexus.db import AsyncSessionLocal, init_db
    from nexus.models import Document

    async def _run_ingest() -> list[dict[str, object]]:
        await init_db()

        # Step 1: Crawl
        cfg = CrawlConfig(
            start_urls=urls,
            max_pages=max_pages,
            max_depth=max_depth,
            concurrency=concurrency,
            delay=delay,
        )
        crawler = Crawler(cfg)
        pages = await crawler.run()

        saved_records: list[dict[str, object]] = []

        # Step 2: Stream through analytics and persist into database
        async with AsyncSessionLocal() as session:
            for page in pages:
                if not page.content_markdown and not page.title:
                    continue

                text = page.content_markdown or ""
                words = text.split()
                word_count = len(words)
                reading_time = round(max(0.1, word_count / 200.0), 1) if word_count > 0 else 0.0
                summary = (
                    text[:250].strip().replace("\n", " ") + ("..." if len(text) > 250 else "")
                )

                # Upsert into DB
                existing = await session.scalar(select(Document).where(Document.url == page.url))
                if existing is not None:
                    existing.title = page.title
                    existing.content_markdown = page.content_markdown
                    existing.text_len = page.text_len
                    existing.word_count = word_count
                    existing.reading_time_mins = reading_time
                    existing.summary = summary
                    existing.status_code = page.status or 200
                    doc_obj = existing
                else:
                    doc_obj = Document(
                        url=page.url,
                        host=page.host,
                        title=page.title,
                        content_markdown=page.content_markdown,
                        text_len=page.text_len,
                        word_count=word_count,
                        reading_time_mins=reading_time,
                        summary=summary,
                        status_code=page.status or 200,
                    )
                    session.add(doc_obj)

                await session.flush()
                await session.refresh(doc_obj)
                saved_records.append(
                    {
                        "id": doc_obj.id,
                        "url": doc_obj.url,
                        "title": doc_obj.title or "(untitled)",
                        "words": doc_obj.word_count,
                        "read_time": doc_obj.reading_time_mins,
                        "summary": doc_obj.summary or "",
                    }
                )

            await session.commit()
        return saved_records

    with console.status("[bold green]Ingesting targets (Crawl -> ETL -> DB)..."):
        records = asyncio.run(_run_ingest())

    table = Table(title=f"Ingested {len(records)} Documents into Nexus DB", show_lines=True)
    table.add_column("DB ID", justify="center", style="bold cyan")
    table.add_column("Title", style="white")
    table.add_column("Words", justify="right")
    table.add_column("Read Time", justify="right")
    table.add_column("Summary Preview", style="dim", overflow="fold")

    for rec in records:
        table.add_row(
            str(rec["id"]),
            str(rec["title"]),
            str(rec["words"]),
            f"{rec['read_time']}m",
            str(rec["summary"])[:80],
        )

    console.print(table)

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        console.print(f"[green][OK][/green] Exported backup to [bold]{output}[/bold]")


if __name__ == "__main__":
    app()
