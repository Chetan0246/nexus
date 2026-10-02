"""Tests for Terminal Dashboard monitor: Metric, Sparklines, and LogFeed."""

from __future__ import annotations

from nexus.monitor.logs import LogFeed
from nexus.monitor.metrics import Metric, MetricConfig, SimulatedSource


def test_metric_calculations_and_sparkline() -> None:
    cfg = MetricConfig(name="CPU Usage", unit="%", warn_above=70.0, crit_above=90.0)
    m = Metric(cfg)

    m.push(10.0)
    m.push(50.0)
    m.push(95.0)

    assert m.min == 10.0
    assert m.max == 95.0
    assert round(m.avg, 1) == 51.7
    assert m.level() == "crit"

    # Sparkline check
    spark = m.sparkline(width=5)
    assert len(spark) == 3
    # Check that highest value uses a high block
    assert spark[-1] in "▆▇█"


def test_simulated_telemetry_source() -> None:
    source = SimulatedSource(seed=42)
    sample = source.sample()

    expected_keys = {"cpu", "mem", "net_in", "net_out", "rps", "latency"}
    assert expected_keys.issubset(sample.keys())
    assert 0.0 <= sample["cpu"] <= 100.0
    assert 0.0 <= sample["mem"] <= 100.0


def test_log_feed_and_filtering() -> None:
    feed = LogFeed(buffer_size=20)
    for _ in range(15):
        feed.generate()

    assert len(feed.entries) == 15
    stats = feed.level_stats()
    assert sum(stats.values()) == 15

    for entry in feed.entries:
        rendered = entry.render()
        assert entry.level in rendered
        assert entry.message in rendered
