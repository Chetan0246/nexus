"""Textual dashboard application: live gauges, sparklines, log tail, and level counts."""

from __future__ import annotations

import argparse
import sys

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, RichLog, Static

from nexus.monitor.logs import LogEntry, LogFeed
from nexus.monitor.metrics import (
    PSUTIL_AVAILABLE,
    Metric,
    SimulatedSource,
    SystemSource,
    default_metrics,
)

LEVEL_FILTERS = ("ALL", "DEBUG", "INFO", "WARN", "ERROR")


class Gauge(Static):
    """Metric gauge cell: value, level-colored bar, sparkline, and min/max/avg."""

    metric: Metric
    value: float = 0.0

    def render(self) -> str:
        cfg = self.metric.config
        width = 20
        ceiling = cfg.crit_above or 100.0
        frac = max(0.0, min(1.0, (self.value or 0.0) / ceiling))
        filled = int(frac * width)
        bar = "█" * filled + "░" * (width - filled)
        color = {"ok": "green", "warn": "yellow", "crit": "bold red"}[self.metric.level()]
        spark = self.metric.sparkline(width=width)
        spark_line = f"\n[dim cyan]{spark}[/dim cyan]" if spark else ""

        return (
            f"[bold]{cfg.name}[/bold]\n"
            f"[{color}]{(self.value or 0.0):7.1f} {cfg.unit}[/{color}]\n"
            f"[{color}]{bar}[/{color}]"
            f"{spark_line}\n"
            f"[dim]avg {self.metric.avg:6.1f}  max {self.metric.max:7.1f}[/dim]"
        )

    def update_metric(self, metric: Metric, value: float) -> None:
        self.metric = metric
        self.value = value
        self.refresh()


class DashboardApp(App[None]):
    """Live operations dashboard powered by Textual."""

    TITLE = "Nexus — Realtime Operations Dashboard"
    CSS = """
    #root { height: 100%; }
    #gauges { height: auto; align: center top; }
    Gauge { width: 1fr; max-width: 34; height: 8; border: round $primary; padding: 0 1; }
    #body { height: 1fr; }
    #logs { width: 2fr; border: round $accent; }
    #side { width: 1fr; }
    #level-table { height: auto; border: round $secondary; }
    """
    BINDINGS = [
        ("p", "toggle_pause", "Pause"),
        ("l", "cycle_log_filter", "Log Filter"),
        ("c", "clear_logs", "Clear Logs"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, tick_interval: float = 0.5, simulated: bool = False) -> None:
        super().__init__()
        self.tick_interval = tick_interval
        self.simulated = simulated or not PSUTIL_AVAILABLE

        if self.simulated:
            self.source = SimulatedSource(seed=42)
        else:
            self.source = SystemSource()

        self.metrics = default_metrics()
        self.log_feed = LogFeed(buffer_size=400, seed=7)
        self.paused = False
        self.filter_idx = 0
        self.gauges: dict[str, Gauge] = {}

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Vertical(id="root"):
            with Horizontal(id="gauges"):
                for name in ("cpu", "mem", "rps", "latency"):
                    gauge = Gauge(id=f"gauge-{name}")
                    gauge.metric = self.metrics[name]
                    self.gauges[name] = gauge
                    yield gauge
            with Horizontal(id="body"):
                yield RichLog(id="logs", highlight=False, markup=True, wrap=True)
                with Vertical(id="side"):
                    table = DataTable(id="level-table")
                    table.add_columns("Level", "Count")
                    yield table
        yield Footer()

    def on_mount(self) -> None:
        mode_label = "simulated" if self.simulated else "live system"
        self.sub_title = mode_label
        self.set_interval(self.tick_interval, self.tick)

    async def tick(self) -> None:
        if self.paused or not self.is_running or self.screen is None:
            return
        try:
            table = self.query_one("#level-table", DataTable)
            log_widget = self.query_one("#logs", RichLog)
        except Exception:
            return

        if self.paused:
            return

        samples = self.source.sample()
        for name, value in samples.items():
            if name in self.metrics:
                self.metrics[name].push(value)
        for name, gauge in self.gauges.items():
            if name in samples:
                gauge.update_metric(self.metrics[name], samples[name])

        entry: LogEntry = self.log_feed.generate()
        stats = self.log_feed.level_stats()
        if table.row_count == 0:
            for level in ("DEBUG", "INFO", "WARN", "ERROR"):
                table.add_row(level, stats[level])
        else:
            for i, level in enumerate(("DEBUG", "INFO", "WARN", "ERROR")):
                table.update_cell_at((i, 1), stats[level])

        current = LEVEL_FILTERS[self.filter_idx]
        if current == "ALL" or entry.level == current:
            log_widget.write(entry.render())

    def action_toggle_pause(self) -> None:
        self.paused = not self.paused
        mode_label = "simulated" if self.simulated else "live system"
        self.sub_title = "PAUSED" if self.paused else mode_label

    def action_cycle_log_filter(self) -> None:
        self.filter_idx = (self.filter_idx + 1) % len(LEVEL_FILTERS)
        current = LEVEL_FILTERS[self.filter_idx]
        log_widget = self.query_one("#logs", RichLog)
        log_widget.clear()
        for entry in self.log_feed.entries:
            if current == "ALL" or entry.level == current:
                log_widget.write(entry.render())
        self.sub_title = f"filter: {current}"

    def action_clear_logs(self) -> None:
        self.query_one("#logs", RichLog).clear()


def main(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Nexus - Realtime Terminal Operations Dashboard"
    )
    parser.add_argument(
        "--simulated", action="store_true", help="Run with simulated telemetry"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Update interval in seconds (default: 0.5)",
    )
    parsed = parser.parse_args(args=args if args is not None else sys.argv[1:])

    app = DashboardApp(tick_interval=parsed.interval, simulated=parsed.simulated)
    app.run()


if __name__ == "__main__":
    main()
