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
Demo breakpoints: freezing a scenario at a named point so a live audience can be shown the
cluster in that exact state.

Ducktape runs the test inside the ``ducker01`` container with stdin on /dev/null, so a test
cannot read a keypress. What it can do is share files with the host: ``ducker-ignite`` bind
mounts the whole Ignite repository into every container, so a control directory below the
repository root is visible to the test and to the host at the same time.

The protocol over that directory is one sided on purpose - the test is the only party that
deletes anything, so no step can race with the host:

    - the test publishes ``paused.txt`` (a rendered banner) and ``paused.json`` (the same
      content as data) and then blocks;
    - the host creates ``continue-<seq>``, ``continue-all`` or ``abort``;
    - the test consumes that file, removes it along with its own status files, and proceeds.

``docker/demo_console.py`` is the host side of it, but nothing depends on it: reading
``paused.txt`` and touching ``continue-<seq>`` by hand works just as well.

Globals:

    demo_pause - absent or false disables every breakpoint (the default, so tests are
    unaffected in CI); true or "*" stops at all of them; a list or a comma separated string
    stops only at the named ones.

    demo_pause_timeout_sec - how long a single breakpoint may hold the scenario before it
    resumes on its own, 1800 by default.

    demo_pause_dir - control directory, ``<repository root>/.ducktests-demo`` by default.
"""

import json
import os
import time

# globals:
DEMO_PAUSE = "demo_pause"
DEMO_PAUSE_TIMEOUT_SEC = "demo_pause_timeout_sec"
DEMO_PAUSE_DIR = "demo_pause_dir"

DEFAULT_TIMEOUT_SEC = 1800

CONTROL_DIR_NAME = ".ducktests-demo"

STATUS_TXT = "paused.txt"
STATUS_JSON = "paused.json"
CONTINUE_PREFIX = "continue-"
CONTINUE_ALL = "continue-all"
ABORT = "abort"

# The control directory is polled rather than watched: it is a bind mount shared with the
# host, where inotify is not dependable.
POLL_SEC = .5

# A paused test is silent, and ducktape kills a test client that stays silent for too long
# (--test-runner-timeout). The heartbeat keeps the client alive and timestamps the demo in
# the test log.
HEARTBEAT_SEC = 15

# Every breakpoint name matches.
ALL = "*"

_WIDTH = 100


def repo_root():
    """
    :return: Path of the Ignite repository root, derived from this module's own location
             (``<root>/modules/ducktests/tests/ignitetest/utils/pause.py``), so that a fork
             checked out elsewhere resolves its own root.
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), *[os.pardir] * 5))


def default_control_dir():
    """
    :return: Path of the control directory shared between the test and the host.
    """
    return os.path.join(repo_root(), CONTROL_DIR_NAME)


def continue_file(seq):
    """
    :return: Name of the file that resumes the breakpoint with the given sequence number.
    """
    return f"{CONTINUE_PREFIX}{seq}"


def parse_selector(value):
    """
    Interprets the ``demo_pause`` global.

    :return: None when demo pausing is disabled, :data:`ALL` to stop at every breakpoint,
             or the set of breakpoint names to stop at.
    """
    if value is None or value is False:
        return None

    if value is True:
        return ALL

    if isinstance(value, (list, tuple, set, frozenset)):
        names = {str(name).strip() for name in value}
        names.discard("")

        return names or None

    if isinstance(value, str):
        text = value.strip().lower()

        if text in ("", "false", "no", "off", "0"):
            return None

        if text in (ALL, "all", "true", "yes", "on", "1"):
            return ALL

        names = {name.strip() for name in value.split(",")}
        names.discard("")

        return names or None

    return ALL if value else None


def _fmt_duration(seconds):
    """
    :return: Duration as mm:ss, or hh:mm:ss once it no longer fits.
    """
    seconds = max(int(seconds), 0)

    if seconds >= 3600:
        return f"{seconds // 3600}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"

    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def _node_addr(node):
    """
    :return: The node's routable address when it adds anything to the name the banner already
             carries - under ducker the two are the same string.

    Deliberately not resolved to an IP: a name that does not resolve costs a DNS round trip
    per node, and a breakpoint that takes seconds to print its banner defeats the point. The
    NETWORK section resolves addresses where they actually matter.
    """
    addr = node.account.externally_routable_ip

    return "" if addr == node.account.hostname else f"[{addr}]"


def _node_state(service, node):
    """
    :return: Liveness of the node as far as its service can tell, "?" when the probe itself
             failed - a breakpoint must never fail the scenario it is only observing.
    """
    alive = getattr(service, "alive", None)

    if alive is None:
        return ""

    try:
        return "UP" if alive(node) else "DOWN"
    except Exception:  # pylint: disable=broad-except
        return "?"


class DemoPause:
    """
    Holds a scenario at named breakpoints.

    Disabled unless the ``demo_pause`` global says otherwise, in which case :meth:`pause` is
    a plain return and nothing is written anywhere.
    """
    def __init__(self, logger, test_globals, test_name, control_dir=None):
        self.logger = logger
        self.test_name = test_name

        self.names = parse_selector(test_globals.get(DEMO_PAUSE))
        self.timeout_sec = float(test_globals.get(DEMO_PAUSE_TIMEOUT_SEC, DEFAULT_TIMEOUT_SEC))

        self.control_dir = control_dir or test_globals.get(DEMO_PAUSE_DIR) or default_control_dir()

        self.seq = 0

        self._started_at = time.monotonic()
        self._prepared = False
        self._continue_all = False

    @property
    def enabled(self):
        """
        :return: Whether any breakpoint of this test can stop the scenario.
        """
        return self.names is not None and not self._continue_all

    def pause(self, name, describers=(), services=()):
        """
        Blocks the scenario at the named breakpoint until the host resumes it.

        :param name: Breakpoint name, matched against the ``demo_pause`` global.
        :param describers: Objects exposing ``describe() -> list of str``, each contributing
               a section to the banner. The first line of a section is its title.
        :param services: Services to list in the banner, normally the test's whole registry.
        """
        if not self._stops_at(name):
            return

        self._prepare()

        self.seq += 1

        banner = self._render(name, describers, services)

        self._publish(name, banner)

        self.logger.info(f"Demo breakpoint reached [seq={self.seq}, name={name}, dir={self.control_dir}]")

        self._await_resume(name)

    def _stops_at(self, name):
        if not self.enabled:
            return False

        return self.names == ALL or name in self.names

    def _prepare(self):
        """
        Creates the control directory and clears anything a previous run left behind: a
        stale resume file would skip the very first breakpoint of this one.
        """
        if self._prepared:
            return

        os.makedirs(self.control_dir, exist_ok=True)

        for name in os.listdir(self.control_dir):
            if name.startswith(CONTINUE_PREFIX) or name.startswith(STATUS_TXT) \
                    or name.startswith(STATUS_JSON) or name == ABORT:
                self._remove(name)

        self._prepared = True

    def _render(self, name, describers, services):
        """
        :return: The banner as a list of lines.
        """
        elapsed = f" t+{_fmt_duration(time.monotonic() - self._started_at)} since test start"
        auto = f"auto-continue in {_fmt_duration(self.timeout_sec)} "

        lines = [
            "=" * _WIDTH,
            f" PAUSED {self.seq}   {name}",
            f" test  {self.test_name}",
            (elapsed + auto.rjust(max(_WIDTH - len(elapsed), 1))).rstrip(),
        ]

        for section in [self._services_section(services)] + [self._section(d) for d in describers]:
            if section:
                lines.append("-" * _WIDTH)
                lines.extend(section)

        lines.append("-" * _WIDTH)
        lines.extend(self._hints_section(services))

        lines.append("-" * _WIDTH)
        lines.append(" continue: [Enter] in the demo console")
        lines.append(f"           or  touch {os.path.join(self.control_dir, continue_file(self.seq))}")
        lines.append("=" * _WIDTH)

        return lines

    def _section(self, describer):
        try:
            return list(describer.describe())
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.warn(f"Demo breakpoint describer failed [describer={describer}, error={ex}]")

            return []

    @staticmethod
    def _services_section(services):
        lines = ["SERVICES"]

        for service in services:
            for node in service.nodes:
                who = service.who_am_i(node)
                addr = _node_addr(node)

                lines.append(f"  {who:<58} {addr:<17} {_node_state(service, node)}".rstrip())

        if len(lines) == 1:
            lines.append("  (none)")

        return lines

    @staticmethod
    def _hints_section(services):
        """
        Node logs live only on the nodes while the test runs - ducktape copies them into the
        results directory at teardown - so every hint goes through the node container.
        """
        hosts = sorted({node.account.hostname for service in services for node in service.nodes})

        log_dir = "/mnt/service/logs"
        config_file = "/mnt/service/config/ignite-config.xml"

        for service in services:
            try:
                log_dir = getattr(service, "log_dir", None) or log_dir
                config_file = getattr(service, "config_file", None) or config_file
            except Exception:  # pylint: disable=broad-except
                pass

            break

        # Node paths, joined by hand: they are always POSIX, os.path.join is not when the
        # control machine happens to be Windows.
        ignite_log = f"{log_dir.rstrip('/')}/ignite*.log"
        console_log = f"{log_dir.rstrip('/')}/console.log"

        return [
            f" nodes    {' '.join(hosts) if hosts else '(none)'}",
            " shell    ./docker/ducker-ignite ssh <node>",
            f" logs     docker exec <node> bash -c \"tail -n 50 {ignite_log}\"",
            f" console  docker exec <node> tail -n 50 {console_log}",
            f" config   docker exec <node> cat {config_file}",
        ]

    def _publish(self, name, banner):
        """
        Publishes the breakpoint for the host, banner first as text and then as data: both
        files are replaced atomically so the console never reads a half written one.
        """
        text = "\n".join(banner) + "\n"

        self._write(STATUS_TXT, text)

        self._write(STATUS_JSON, json.dumps({
            "seq": self.seq,
            "name": name,
            "test": self.test_name,
            "elapsed_sec": round(time.monotonic() - self._started_at, 1),
            "timeout_sec": self.timeout_sec,
            "banner": banner
        }, indent=2))

    def _await_resume(self, name):
        deadline = time.monotonic() + self.timeout_sec
        heartbeat = time.monotonic() + HEARTBEAT_SEC

        while True:
            if self._exists(ABORT):
                self._consume(ABORT)

                raise AssertionError(f"Demo aborted at breakpoint [seq={self.seq}, name={name}]")

            if self._exists(CONTINUE_ALL):
                self._continue_all = True

                self._consume(CONTINUE_ALL)
                self.logger.info(f"Demo resumed, remaining breakpoints skipped [seq={self.seq}, name={name}]")

                return

            if self._exists(continue_file(self.seq)):
                self._consume(continue_file(self.seq))
                self.logger.info(f"Demo resumed [seq={self.seq}, name={name}]")

                return

            now = time.monotonic()

            if now >= deadline:
                self._consume()
                self.logger.warn(f"Demo breakpoint timed out after {self.timeout_sec}s, resuming "
                                 f"[seq={self.seq}, name={name}]")

                return

            if now >= heartbeat:
                heartbeat = now + HEARTBEAT_SEC

                self.logger.info(f"Still paused at demo breakpoint [seq={self.seq}, name={name}, "
                                 f"waiting={_fmt_duration(self.timeout_sec - (deadline - now))}]")

            time.sleep(POLL_SEC)

    def _consume(self, *names):
        """
        Clears the published breakpoint along with the resume files that ended it.
        """
        for name in names + (STATUS_TXT, STATUS_JSON):
            self._remove(name)

    def _exists(self, name):
        return os.path.exists(os.path.join(self.control_dir, name))

    def _remove(self, name):
        try:
            os.remove(os.path.join(self.control_dir, name))
        except OSError:
            pass

    def _write(self, name, content):
        path = os.path.join(self.control_dir, name)
        tmp = path + ".tmp"

        with open(tmp, "w", encoding="utf-8") as file:
            file.write(content)

        os.replace(tmp, path)
