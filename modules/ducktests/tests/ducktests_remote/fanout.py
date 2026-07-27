# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Parallel per-host execution and the table it prints.

The batch never aborts on the first failure unless asked to.  When an operator has to
send a single request to whoever administers the machines, a partial list of broken
hosts is worse than useless.
"""

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Optional

OK = "ok"
CHANGED = "changed"
SKIPPED = "skipped"
WARN = "warn"
FAILED = "failed"

_ORDER = {FAILED: 0, WARN: 1, CHANGED: 2, SKIPPED: 3, OK: 4}


@dataclass
class HostResult:
    """Outcome of one per-host operation."""

    host: str
    status: str = OK
    message: str = ""
    detail: str = ""
    duration: float = 0.0
    data: Optional[Any] = field(default=None)

    @property
    def ok(self):
        """:return: True unless the operation failed."""
        return self.status != FAILED


class FanoutAborted(Exception):
    """Raised when ``--fail-fast`` cut a batch short."""


def fanout(hosts, operation, *, jobs=16, fail_fast=False):
    """
    Apply ``operation(host)`` across ``hosts`` in a bounded thread pool.

    :param operation: callable returning a :class:`HostResult` (or a ``(status, message)``
        tuple, or None for plain success).
    :param jobs: pool size.
    :param fail_fast: stop scheduling further hosts after the first failure.
    :return: results in inventory order.
    """
    hosts = list(hosts)
    if not hosts:
        return []
    results = [None] * len(hosts)
    stop = {"flag": False}

    def wrapped(index, host):
        if stop["flag"]:
            return HostResult(_host_name(host), SKIPPED, "skipped after earlier failure")
        started = time.monotonic()
        try:
            outcome = operation(host)
        except Exception as ex:  # noqa: BLE001 - per-host isolation is the whole point
            outcome = HostResult(_host_name(host), FAILED, _short(ex), detail=repr(ex))
        outcome = _normalise(outcome, host)
        outcome.duration = time.monotonic() - started
        if fail_fast and outcome.status == FAILED:
            stop["flag"] = True
        results[index] = outcome
        return outcome

    with ThreadPoolExecutor(max_workers=max(1, int(jobs))) as pool:
        list(pool.map(lambda pair: wrapped(*pair), list(enumerate(hosts))))

    return [r for r in results if r is not None]


def _normalise(outcome, host):
    name = _host_name(host)
    if outcome is None:
        return HostResult(name, OK)
    if isinstance(outcome, HostResult):
        return outcome
    if isinstance(outcome, tuple):
        status, message = (list(outcome) + [""])[:2]
        return HostResult(name, status, message)
    return HostResult(name, OK, str(outcome))


def _host_name(host):
    return getattr(host, "host", None) or str(host)


def _short(ex):
    text = str(ex).strip().splitlines()
    return text[0][:160] if text else ex.__class__.__name__


def render_table(results, *, verbose=False, headers=("HOST", "STATUS", "TIME", "DETAIL")):
    """:return: an aligned table of fan-out results, failures first within equal status."""
    if not results:
        return "(no hosts)"
    rows = [(r.host, r.status, "%.1fs" % r.duration, r.message or "") for r in results]
    widths = [max(len(str(row[i])) for row in ([headers] + rows)) for i in range(4)]
    lines = ["  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)).rstrip()]
    lines.append("  ".join("-" * widths[i] for i in range(4)))
    for row in rows:
        lines.append("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)).rstrip())
    if verbose:
        for result in results:
            if result.detail:
                lines.append("")
                lines.append("--- %s ---" % result.host)
                lines.append(result.detail.rstrip())
    else:
        for result in results:
            if result.status == FAILED and result.detail:
                lines.append("")
                lines.append("--- %s ---" % result.host)
                lines.append(result.detail.rstrip())
    return "\n".join(lines)


def summarise(results):
    """:return: a compact ``9 ok, 2 failed`` style summary line."""
    counts = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    ordered = sorted(counts.items(), key=lambda kv: _ORDER.get(kv[0], 9))
    return ", ".join("%d %s" % (count, status) for status, count in ordered) or "nothing to do"


def any_failed(results):
    """:return: True when at least one host failed."""
    return any(r.status == FAILED for r in results)
