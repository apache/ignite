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
Live progress for the transfers that take minutes.

Sending 3.5 GB to twelve machines is a normal deploy, and a terminal that prints nothing
for six minutes is indistinguishable from one that has hung.  This renders what every
host is doing, from the same fan-out threads that do the work.

Two modes and no third:

* **live** - a redrawn block on a terminal, one row per host plus a total.
* **plain** - one aggregate line every ``plain_interval`` seconds, for a log file, a CI
  job, ``--verbose`` (where a redrawn block would fight with the traced command lines)
  and anything else that is not a terminal.

The aggregate is the mean of the per-host fractions, never a byte total against a
predicted one: rsync sends only what differs, so a denominator of "the whole
distribution times the host count" would stall at 12% and finish there.  Each host
contributes an equal share of the bar, scaled by how far along it is.
"""

import os
import shutil
import sys
import threading
import time

WAITING = "waiting"
SENDING = "sending"
DONE = "done"

_UP = "\033[%dA"
_CLEAR_LINE = "\033[2K"
_HIDE_CURSOR = "\033[?25l"
_SHOW_CURSOR = "\033[?25h"


def human_bytes(size):
    """:return: ``1.4 GB``; the same shape deploy prints elsewhere."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return "%.0f %s" % (value, unit) if unit in ("B", "KB") else "%.1f %s" % (value, unit)
        value /= 1024
    return "%.1f TB" % value


def human_duration(seconds):
    """:return: ``4:12`` or ``1:04:12``."""
    seconds = int(max(0, seconds))
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return "%d:%02d:%02d" % (hours, minutes, secs)
    return "%d:%02d" % (minutes, secs)


class _HostState:  # pylint: disable=too-few-public-methods
    """What one host is doing, and how far along it is."""

    def __init__(self, name):
        self.name = name
        self.phase = WAITING
        self.sent = 0
        self.total = 0
        self.fraction = 0.0
        self.started = None
        self.message = ""

    @property
    def rate(self):
        """:return: bytes per second since this host started, or 0."""
        if not self.started or self.sent <= 0:
            return 0
        elapsed = time.monotonic() - self.started
        return self.sent / elapsed if elapsed > 0 else 0


class Progress:
    """
    A thread-safe progress display shared by every host in a fan-out.

    Nothing here raises: a display that breaks a deploy would be worse than no display,
    so a stream that cannot be written to simply stops being written to.
    """

    def __init__(self, hosts, *, stream=None, live=None, redactor=None,
                 interval=0.25, plain_interval=15.0, max_rows=12, unicode_bar=None):
        self.stream = stream if stream is not None else sys.stderr
        self.hosts = {name: _HostState(name) for name in hosts}
        self.order = list(hosts)
        self.redactor = redactor
        self.interval = interval
        self.plain_interval = plain_interval
        self.max_rows = max_rows
        self.live = is_a_terminal(self.stream) if live is None else bool(live)
        self.blocks = _unicode_bar(self.stream) if unicode_bar is None else bool(unicode_bar)
        self.title = ""
        self._lock = threading.Lock()
        self._thread = None
        self._stop = threading.Event()
        self._drawn = 0
        self._started = time.monotonic()
        self._last_plain = 0.0

    # -- lifecycle ---------------------------------------------------------------

    def start(self, title=""):
        """Begin redrawing. Safe to call when the display is disabled."""
        self.title = title
        self._started = time.monotonic()
        self._last_plain = time.monotonic()
        if self._thread is not None:
            return self
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="dtr-progress", daemon=True)
        self._thread.start()
        return self

    def close(self):
        """Stop redrawing and leave the terminal as it was found."""
        if self._thread is None:
            return
        self._stop.set()
        self._thread.join(timeout=2 * self.interval + 1)
        self._thread = None
        if self.live:
            self._write(self._erase() + _SHOW_CURSOR)

    def __enter__(self):
        return self.start()

    def __exit__(self, *_exc):
        self.close()
        return False

    # -- updates, called from the fan-out threads --------------------------------

    def phase(self, host, phase):
        """Record what ``host`` is doing now: sending, extracting, swapping."""
        with self._lock:
            state = self.hosts.get(host)
            if state is None:
                return
            state.phase = phase
            if state.started is None:
                state.started = time.monotonic()

    def sent(self, host, sent, total=None, fraction=None):
        """
        Record transferred bytes for ``host``.

        :param fraction: 0..1 when the sender knows better than ``sent/total`` - rsync
            reports its own percentage, which accounts for files it decided not to send.
        """
        with self._lock:
            state = self.hosts.get(host)
            if state is None:
                return
            if state.started is None:
                state.started = time.monotonic()
            state.phase = SENDING
            state.sent = max(state.sent, int(sent or 0))
            if total:
                state.total = int(total)
            if fraction is not None:
                state.fraction = min(1.0, max(0.0, float(fraction)))
            elif state.total:
                state.fraction = min(1.0, state.sent / float(state.total))

    def done(self, host, message=""):
        """Mark ``host`` finished, however it finished."""
        with self._lock:
            state = self.hosts.get(host)
            if state is None:
                return
            state.phase = DONE
            state.fraction = 1.0
            state.message = message

    # -- rendering ---------------------------------------------------------------

    def _loop(self):
        while not self._stop.is_set():
            self._tick()
            self._stop.wait(self.interval)
        self._tick(final=True)

    def _tick(self, final=False):
        if self.live:
            self._write(self._frame())
            return
        now = time.monotonic()
        if final or now - self._last_plain >= self.plain_interval:
            self._last_plain = now
            line = self.aggregate_line()
            if line:
                self._write(line + "\n")

    def _frame(self):
        lines = self.rows() + [self.aggregate_line()]
        out = [_HIDE_CURSOR]
        if self._drawn:
            out.append(_UP % self._drawn)
        for line in lines:
            out.append(_CLEAR_LINE + self._fit(line) + "\n")
        for _ in range(max(0, self._drawn - len(lines))):
            out.append(_CLEAR_LINE + "\n")
        overshoot = max(0, self._drawn - len(lines))
        if overshoot:
            out.append(_UP % overshoot)
        self._drawn = len(lines)
        return "".join(out)

    def _erase(self):
        if not self._drawn:
            return ""
        return (_UP % self._drawn) + (_CLEAR_LINE + "\n") * self._drawn + (_UP % self._drawn)

    def rows(self):
        """:return: one rendered line per host worth showing."""
        with self._lock:
            states = [self.hosts[name] for name in self.order]
        active = [s for s in states if s.phase not in (DONE, WAITING)]
        # Finished hosts leave the block: they are already counted in the total line, and
        # on a large cluster they would otherwise crowd out the hosts still working.
        candidates = active or states
        shown = candidates[:self.max_rows]
        width = max((len(s.name) for s in states), default=4)
        rows = [self._row(state, width) for state in shown]
        hidden = len(candidates) - len(shown)
        if hidden > 0:
            rows.append("  ... %d more host(s)" % hidden)
        return rows

    def _row(self, state, width):
        name = state.name.ljust(width)
        if state.phase == DONE:
            return "  %s  %s %s" % (name, self._bar(1.0), state.message or DONE)
        if state.phase == WAITING:
            return "  %s  %s %s" % (name, self._bar(0.0), WAITING)
        if state.phase != SENDING:
            return "  %s  %s %s" % (name, self._bar(state.fraction), state.phase)
        detail = "%3d%%  %s" % (round(state.fraction * 100), human_bytes(state.sent))
        if state.total:
            detail += " / %s" % human_bytes(state.total)
        if state.rate:
            detail += "  %s/s" % human_bytes(state.rate)
        return "  %s  %s %s" % (name, self._bar(state.fraction), detail)

    def aggregate_line(self):
        """:return: the one line that also stands on its own in a log file."""
        with self._lock:
            states = [self.hosts[name] for name in self.order]
        if not states:
            return ""
        finished = sum(1 for s in states if s.phase == DONE)
        fraction = sum(s.fraction for s in states) / len(states)
        moved = sum(s.sent for s in states)
        width = max((len(s.name) for s in states), default=5)
        text = "  %s  %s %3d%%  %d/%d host(s)  %s sent  %s" % (
            "total".ljust(width), self._bar(fraction), round(fraction * 100), finished,
            len(states), human_bytes(moved),
            human_duration(time.monotonic() - self._started))
        return text if self.live else " ".join(text.split())

    def _bar(self, fraction, width=24):
        filled, empty = ("█", "░") if self.blocks else ("#", ".")
        done = int(round(min(1.0, max(0.0, fraction)) * width))
        return filled * done + empty * (width - done)

    def _fit(self, line):
        columns = terminal_width(self.stream)
        return line if len(line) <= columns else line[:max(0, columns - 1)]

    def _write(self, text):
        if not text:
            return
        if self.redactor is not None:
            text = self.redactor.redact(text)
        try:
            self.stream.write(text)
            self.stream.flush()
        except (OSError, ValueError):
            # A closed or broken stream must never take a deploy down with it.
            self.live = False


class NullProgress:
    """The same surface, doing nothing. Callers never test for None."""

    live = False

    def start(self, title=""):
        """:return: self, so ``with`` reads the same either way."""
        return self

    def close(self):
        """Nothing to close."""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def phase(self, host, phase):
        """Ignore the update."""

    def sent(self, host, sent, total=None, fraction=None):
        """Ignore the update."""

    def done(self, host, message=""):
        """Ignore the update."""


def is_a_terminal(stream):
    """:return: True when ``stream`` can be redrawn in place."""
    try:
        if not stream.isatty():
            return False
    except (AttributeError, ValueError):
        return False
    return os.environ.get("TERM") != "dumb"


def terminal_width(stream, default=100):
    """:return: usable columns, falling back when the size cannot be read."""
    try:
        columns = shutil.get_terminal_size().columns
    except (OSError, ValueError):
        return default
    del stream
    return columns if columns and columns > 20 else default


def _unicode_bar(stream):
    encoding = (getattr(stream, "encoding", "") or "").lower()
    return "utf" in encoding
