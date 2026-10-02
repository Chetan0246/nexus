"""System telemetry, simulated data source, ring buffers, and Unicode sparklines."""

from __future__ import annotations

import random
import statistics
import time
from collections import deque
from dataclasses import dataclass
from itertools import count

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None  # type: ignore[assignment]
    PSUTIL_AVAILABLE = False

SPARK_CHARS = (" ", "▂", "▃", "▄", "▅", "▆", "▇", "█")


@dataclass(slots=True)
class MetricConfig:
    name: str
    unit: str
    history: int = 240
    warn_above: float | None = None
    crit_above: float | None = None


class Metric:
    """Bounded rolling time-series with threshold evaluation and sparklines."""

    def __init__(self, config: MetricConfig) -> None:
        self.config = config
        self._values: deque[float] = deque(maxlen=config.history)
        self._seq = count()

    def push(self, value: float) -> None:
        self._values.append(value)

    @property
    def values(self) -> list[float]:
        return list(self._values)

    @property
    def latest(self) -> float | None:
        return self._values[-1] if self._values else None

    @property
    def avg(self) -> float:
        return statistics.fmean(self._values) if self._values else 0.0

    @property
    def min(self) -> float:
        return min(self._values) if self._values else 0.0

    @property
    def max(self) -> float:
        return max(self._values) if self._values else 0.0

    def level(self) -> str:
        v = self.latest
        if v is None:
            return "ok"
        if self.config.crit_above is not None and v > self.config.crit_above:
            return "crit"
        if self.config.warn_above is not None and v > self.config.warn_above:
            return "warn"
        return "ok"

    def sparkline(self, width: int = 16) -> str:
        if not self._values:
            return ""
        vals = list(self._values)[-width:]
        low = min(vals)
        high = max(vals)
        span = high - low
        if span <= 1e-6:
            return SPARK_CHARS[3] * len(vals)
        chars = []
        for v in vals:
            idx = int((v - low) / span * (len(SPARK_CHARS) - 1))
            chars.append(SPARK_CHARS[max(0, min(len(SPARK_CHARS) - 1, idx))])
        return "".join(chars)


class SystemSource:
    """Samples real host hardware telemetry using psutil."""

    def __init__(self) -> None:
        if not PSUTIL_AVAILABLE or psutil is None:
            raise RuntimeError("psutil is not installed")
        self._last_net = psutil.net_io_counters()
        self._last_time = time.monotonic()
        psutil.cpu_percent(interval=None)

    def sample(self) -> dict[str, float]:
        if psutil is None:
            raise RuntimeError("psutil is not installed")
        now = time.monotonic()
        dt = max(0.001, now - self._last_time)
        self._last_time = now

        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory().percent

        net = psutil.net_io_counters()
        net_in = max(0.0, (net.bytes_recv - self._last_net.bytes_recv) / 1024.0 / dt)
        net_out = max(0.0, (net.bytes_sent - self._last_net.bytes_sent) / 1024.0 / dt)
        self._last_net = net

        try:
            conns = float(len(psutil.net_connections(kind="inet")))
        except Exception:
            conns = float(len(psutil.pids()))

        return {
            "cpu": min(100.0, float(cpu)),
            "mem": min(100.0, float(mem)),
            "net_in": round(net_in, 1),
            "net_out": round(net_out, 1),
            "rps": conns,
            "latency": round(max(1.0, cpu * 0.8), 1),
        }


class SimulatedSource:
    """Generates plausible telemetry samples for headless or demo runs."""

    def __init__(self, seed: int | None = None) -> None:
        self._rng = random.Random(seed)
        self._cpu_base = 30.0
        self._tick = 0

    def sample(self) -> dict[str, float]:
        self._tick += 1
        self._cpu_base = min(95.0, max(5.0, self._cpu_base + self._rng.uniform(-5, 5)))
        cpu = self._cpu_base + (20.0 if self._rng.random() < 0.05 else 0.0)
        mem = 50 + self._rng.uniform(-4, 6) + (self._tick % 100) / 30
        net_in = max(0.0, self._rng.gauss(100, 30))
        net_out = max(0.0, self._rng.gauss(60, 20))
        rps = max(0.0, self._rng.gauss(200, 50))
        latency = max(1.0, self._rng.gauss(30 + cpu / 4, 10))

        return {
            "cpu": min(100.0, cpu),
            "mem": min(100.0, mem),
            "net_in": round(net_in, 1),
            "net_out": round(net_out, 1),
            "rps": round(rps, 1),
            "latency": round(latency, 1),
        }


def default_metrics() -> dict[str, Metric]:
    conn_name = "Connections" if PSUTIL_AVAILABLE else "Requests/s"
    conn_unit = "active" if PSUTIL_AVAILABLE else "req/s"
    return {
        "cpu": Metric(MetricConfig("CPU", "%", warn_above=70, crit_above=90)),
        "mem": Metric(MetricConfig("Memory", "%", warn_above=75, crit_above=92)),
        "rps": Metric(MetricConfig(conn_name, conn_unit)),
        "latency": Metric(MetricConfig("Latency", "ms", warn_above=80, crit_above=150)),
        "net_in": Metric(MetricConfig("Net In", "KB/s")),
        "net_out": Metric(MetricConfig("Net Out", "KB/s")),
    }
