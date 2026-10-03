"""Robots.txt compliance engine with group-based longest-match matching."""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass, field

import aiohttp


@dataclass(slots=True)
class RuleGroup:
    user_agents: list[str] = field(default_factory=list)
    allows: list[str] = field(default_factory=list)
    disallows: list[str] = field(default_factory=list)
    crawl_delay: float | None = None


@dataclass(slots=True)
class HostRules:
    groups: list[RuleGroup] = field(default_factory=list)
    crawl_delay: float | None = None
    fetched_ok: bool = False

    @staticmethod
    def _matches(rule: str, path: str) -> bool:
        if not rule:
            return False
        has_end = rule.endswith("$")
        clean = rule[:-1] if has_end else rule
        if not has_end and "*" not in clean:
            return path.startswith(clean)
        pattern = "^" + re.escape(clean).replace(r"\*", ".*")
        if has_end:
            pattern += "$"
        try:
            return bool(re.search(pattern, path))
        except re.error:
            return path.startswith(clean)

    def is_allowed(self, path: str) -> bool:
        if not self.fetched_ok or not self.groups:
            return True

        best_len = -1
        allowed = True

        for group in self.groups:
            for rule in group.allows:
                if self._matches(rule, path) and len(rule) > best_len:
                    best_len = len(rule)
                    allowed = True
            for rule in group.disallows:
                if self._matches(rule, path) and len(rule) > best_len:
                    best_len = len(rule)
                    allowed = False

        return allowed


def _parse_robots(lines: list[str], user_agent: str) -> HostRules:
    current = RuleGroup()
    groups: list[RuleGroup] = []

    for raw_line in lines:
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            if current.user_agents:
                groups.append(current)
                current = RuleGroup()
            continue

        parts = line.split(":", 1)
        if len(parts) != 2:
            continue
        key, val = parts[0].strip().lower(), parts[1].strip()

        if key == "user-agent":
            current.user_agents.append(val.lower())
        elif key == "allow":
            current.allows.append(val)
        elif key == "disallow":
            current.disallows.append(val)
        elif key == "crawl-delay":
            try:
                current.crawl_delay = float(val)
            except ValueError:
                pass

    if current.user_agents:
        groups.append(current)

    matched: list[RuleGroup] = []
    ua_low = user_agent.lower()
    for g in groups:
        if any(target in ua_low for target in g.user_agents if target != "*"):
            matched.append(g)

    if not matched:
        matched = [g for g in groups if "*" in g.user_agents]

    delay = next((g.crawl_delay for g in matched if g.crawl_delay is not None), None)
    return HostRules(groups=matched, crawl_delay=delay, fetched_ok=True)


class RobotsCache:
    """Caches parsed robots.txt per host."""

    def __init__(self, session: aiohttp.ClientSession, user_agent: str) -> None:
        self.session = session
        self.user_agent = user_agent
        self._cache: dict[str, HostRules] = {}

    async def for_url(self, url: str) -> HostRules:
        p = urllib.parse.urlsplit(url)
        host = p.netloc.lower()
        if host in self._cache:
            return self._cache[host]

        robots_url = f"{p.scheme}://{host}/robots.txt"
        try:
            async with self.session.get(robots_url, timeout=5) as resp:
                if resp.status == 200:
                    text = await resp.text(errors="ignore")
                    rules = _parse_robots(text.splitlines(), self.user_agent)
                else:
                    rules = HostRules(fetched_ok=False)
        except Exception:
            rules = HostRules(fetched_ok=False)

        self._cache[host] = rules
        return rules
