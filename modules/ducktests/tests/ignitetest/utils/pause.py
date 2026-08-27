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

The directory itself, and the file protocol over it, are
:class:`ignitetest.utils.pause_control.ControlDir`. This module holds only what those files
*mean*: which breakpoint stops the scenario, what the banner says, and that ``abort`` ends
the test.

Between breakpoints both sides sweep the directory for files an earlier run left behind,
which would otherwise skip the next breakpoint: the test at its first one (see
:meth:`DemoPause._prepare`), the console at startup - and only while nothing is published, so
a resume file meant for a breakpoint that is currently held is never swept.

``docker/demo_console.py`` is the host side of it, but nothing depends on it: reading
``paused.txt`` and touching ``continue-<seq>`` by hand works just as well, where ``<seq>`` is
the breakpoint number shown in the banner's ``PAUSED <seq>`` line.

Globals:

    demo_pause - absent or false disables every breakpoint (the default, so tests are
    unaffected in CI); true or "*" stops at all of them; a list or a comma separated string
    stops only at the named ones, matched case insensitively.

    demo_pause_timeout_sec - how long a single breakpoint may hold the scenario before it
    resumes on its own, 600 by default. Capped by what is left of ducktape's
    ``--test-runner-timeout``, see :meth:`DemoPause._budgeted_timeout`.

    demo_pause_dir - control directory, ``<repository root>/.ducktests-demo`` by default.
"""

import os
import time

from ignitetest.services.utils.path import IgnitePathAware
from ignitetest.utils.pause_control import ABORT, CONTINUE_ALL, ControlDir, continue_file, default_control_dir, \
    repo_root

# globals:
DEMO_PAUSE = "demo_pause"
DEMO_PAUSE_TIMEOUT_SEC = "demo_pause_timeout_sec"
DEMO_PAUSE_DIR = "demo_pause_dir"

# Well below ducktape's own --test-runner-timeout (1800s), which a breakpoint must not
# outsit - see _budgeted_timeout().
DEFAULT_TIMEOUT_SEC = 600

# Kept free of the runner budget, so that resuming a breakpoint at the very last moment
# still leaves the scenario time to reach its next event.
RUNNER_TIMEOUT_MARGIN_SEC = 60

# Timestamps the demo in the test log, so that a run can be read back afterwards and it is
# visible that the scenario is held rather than stuck. Deliberately NOT a keepalive towards
# ducktape: the test logger writes to files and to stdout only, while the runner listens for
# zmq events that just the runner client itself emits - _budgeted_timeout() is what keeps a
# paused test within the runner's patience.
HEARTBEAT_SEC = 15

# Every breakpoint name matches.
ALL = "*"

_WIDTH = 100


def parse_selector(value):
    """
    Interprets the ``demo_pause`` global.

    Names are matched case insensitively, both here and in :meth:`DemoPause._stops_at`: the
    global is typed by hand next to a test whose breakpoint names are written in the source.

    :return: None when demo pausing is disabled, :data:`ALL` to stop at every breakpoint,
             or the set of breakpoint names to stop at, lower cased.
    """
    if value is None or value is False:
        return None

    if value is True:
        return ALL

    if isinstance(value, (list, tuple, set, frozenset)):
        names = {str(name).strip().lower() for name in value}
        names.discard("")

        return names or None

    if isinstance(value, str):
        text = value.strip().lower()

        if text in ("", "false", "no", "off", "0"):
            return None

        if text in (ALL, "all", "true", "yes", "on", "1"):
            return ALL

        names = {name.strip() for name in text.split(",")}
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


def _resume_command(control, seq):
    """
    :return: The command that resumes a breakpoint by hand, named where it will be typed.

    Relative to the repository root rather than absolute: the banner is rendered inside
    ``ducker01``, where the repository is mounted at ``/opt/ignite-dev``, and read on the host,
    where that path does not exist - so an absolute one would be a command that cannot work
    for the person it is offered to. Both sides see the same repository through the bind
    mount, which makes the relative form the one string that holds for both, and it is the
    form README documents.
    """
    path = control.file(continue_file(seq))

    try:
        relative = os.path.relpath(path, repo_root())
    except ValueError:
        # A control directory on another Windows drive than the repository: nothing shared to
        # be relative to, so the full path is all there is to offer.
        relative = path

    if relative.startswith(os.pardir) or relative == path:
        return f"touch {path}"

    return f"touch {relative.replace(os.sep, '/')}   (from the repository root)"


def _node_host(node):
    """
    :return: Hostname of the node, None when it carries no account to read one from.
    """
    return getattr(getattr(node, "account", None), "hostname", None)


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


def _node_line(service, node):
    """
    :return: The node's line of the SERVICES section, degraded to the name that can be read
             without asking the service when the service itself cannot answer for the node -
             ``who_am_i`` goes through ``idx()``, which raises for a node the service no
             longer owns.

    Like :func:`_node_state`, this lets no reading failure out: a breakpoint must never fail
    the scenario it is only observing, least of all while rendering the banner it was added
    for.
    """
    try:
        return f"  {service.who_am_i(node):<58} {_node_addr(node):<17} {_node_state(service, node)}".rstrip()
    except Exception as ex:  # pylint: disable=broad-except
        name = f"{type(service).__name__}-{_node_host(node) or '?'}"

        return f"  {name:<58} ({type(ex).__name__})"


class DemoPause:
    """
    Holds a scenario at named breakpoints.

    Disabled unless the ``demo_pause`` global says otherwise, in which case :meth:`pause` is
    a plain return and nothing is written anywhere.
    """
    def __init__(self, logger, test_globals, test_name, control_dir=None, started_at=None,
                 runner_timeout_sec=None):
        """
        :param started_at: Monotonic timestamp the test itself started at, which is both what
               the banner counts from and what the runner budget is spent from. Defaults to
               now, which is only right when the first breakpoint is the start of the test.
        :param runner_timeout_sec: Ducktape's ``--test-runner-timeout`` in seconds, None when
               unknown, in which case no breakpoint is cut short by it.
        """
        self.logger = logger
        self.test_name = test_name

        self.names = parse_selector(test_globals.get(DEMO_PAUSE))
        self.timeout_sec = float(test_globals.get(DEMO_PAUSE_TIMEOUT_SEC, DEFAULT_TIMEOUT_SEC))
        self.runner_timeout_sec = runner_timeout_sec

        self.control = ControlDir(control_dir or test_globals.get(DEMO_PAUSE_DIR) or default_control_dir())

        self.seq = 0

        # Identifies this run of this test to the host console, which has no other way of
        # telling a breakpoint of the current run from one left published by a run that
        # died while paused: seq alone restarts at 1 for every test.
        self.run = f"{os.getpid()}-{int(time.time())}"

        self._started_at = time.monotonic() if started_at is None else started_at
        self._prepared = False
        self._continue_all = False
        self._unusable = False

    @property
    def enabled(self):
        """
        :return: Whether any breakpoint of this test can stop the scenario.
        """
        return self.names is not None and not self._continue_all and not self._unusable

    def pause(self, name, describers=(), services=()):
        """
        Blocks the scenario at the named breakpoint until the host resumes it.

        A control directory that cannot be used costs the demo and nothing else - see
        :meth:`_give_up`.

        :param name: Breakpoint name, matched against the ``demo_pause`` global.
        :param describers: Objects exposing ``describe() -> list of str``, each contributing
               a section to the banner. The first line of a section is its title.
        :param services: Services to list in the banner, normally the test's whole registry.
        """
        if not self._stops_at(name):
            return

        try:
            self._prepare()

            self.seq += 1

            timeout_sec = self._budgeted_timeout()

            banner = self._render(name, describers, services, timeout_sec)

            self._publish(name, banner, timeout_sec)
        except OSError as ex:
            self._give_up(name, ex)

            return

        self.logger.info(f"Demo breakpoint reached [seq={self.seq}, name={name}, dir={self.control.path}]")

        self._await_resume(name, timeout_sec)

    def _give_up(self, name, error):
        """
        Turns the remaining breakpoints off, after the control directory turned out to be
        unusable.

        A breakpoint observes a scenario; it must not be what ends one. The directory is a
        bind mount of the host repository, so it can be read only, be owned by another user or
        be full - none of which says anything about the cluster under test, and all of which
        would otherwise fail a run that was about to pass. Blocking would be no better than
        raising: a breakpoint whose banner never reached the host would hold the scenario for
        its whole timeout with nothing on screen to resume it.

        Every later breakpoint would fail the same way, so they are dropped here rather than
        reported again at each one.
        """
        self._unusable = True

        self.logger.warn(f"Demo breakpoints disabled, the control directory cannot be used "
                         f"[dir={self.control.path}, error={error}, seq={self.seq}, name={name}, "
                         f"test={self.test_name}]")

    def _stops_at(self, name):
        if not self.enabled:
            return False

        return self.names == ALL or name.strip().lower() in self.names

    def _budgeted_timeout(self):
        """
        :return: How long this breakpoint may actually hold the scenario.

        ``demo_pause_timeout_sec`` is what the demo asks for, the runner budget is what it is
        allowed. Ducktape's runner kills a test client it has received no event from for
        ``--test-runner-timeout`` and takes the whole session down with it, and a paused test
        sends no events - so a breakpoint has to give up while the runner is still waiting.
        The budget is spent from the start of the test rather than from the breakpoint, hence
        a long setup, or a long earlier pause, leaves less of it for this one.
        """
        if self.runner_timeout_sec is None:
            return self.timeout_sec

        left = self.runner_timeout_sec - (time.monotonic() - self._started_at) - RUNNER_TIMEOUT_MARGIN_SEC

        if left >= self.timeout_sec:
            return self.timeout_sec

        self.logger.warn(f"Demo breakpoint held for at most {_fmt_duration(max(left, 0))} instead of the requested "
                         f"{_fmt_duration(self.timeout_sec)}: what is left of ducktape's --test-runner-timeout "
                         f"({_fmt_duration(self.runner_timeout_sec)}) after {_fmt_duration(self.elapsed_sec)} of "
                         f"this test. Raise --test-runner-timeout for a longer demo "
                         f"[seq={self.seq}, test={self.test_name}]")

        return max(left, 0.0)

    @property
    def elapsed_sec(self):
        """
        :return: Seconds since the test started.
        """
        return time.monotonic() - self._started_at

    def _prepare(self):
        """
        Readies the control directory, once per test: a resume file left by a previous run
        would skip the very first breakpoint of this one.
        """
        if self._prepared:
            return

        self.control.prepare()

        self._prepared = True

    def _render(self, name, describers, services, timeout_sec):
        """
        :return: The banner as a list of lines.
        """
        elapsed = f" t+{_fmt_duration(self.elapsed_sec)} since test start"
        auto = f"auto-continue in {_fmt_duration(timeout_sec)} "

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
        lines.append(f"           or  {_resume_command(self.control, self.seq)}")
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
                lines.append(_node_line(service, node))

        if len(lines) == 1:
            lines.append("  (none)")

        return lines

    @staticmethod
    def _hints_section(services):
        """
        Node logs live only on the nodes while the test runs - ducktape copies them into the
        results directory at teardown - so every hint goes through the node container.
        """
        # A node the banner could not name is left out rather than allowed to fail the hints:
        # the commands are offered for the nodes that can be entered, and one unreadable node
        # must not cost the demo the rest of them.
        hosts = sorted({_node_host(node) for service in services for node in service.nodes} - {None})

        log_dir = "/mnt/service/logs"
        config_file = "/mnt/service/config/ignite-config.xml"

        # One set of copy-pasteable commands for a service list that is not homogeneous, so
        # they follow the Ignite services: a ZookeeperService or a KafkaService registered
        # ahead of them - which the discovery and CDC scenarios do - carries paths of its own
        # and would have the banner name a zookeeper.properties for nodes that never had one.
        for service in [s for s in services if isinstance(s, IgnitePathAware)] or list(services):
            try:
                svc_log_dir = getattr(service, "log_dir", None)
                svc_config_file = getattr(service, "config_file", None)
            except Exception:  # pylint: disable=broad-except
                continue

            log_dir = svc_log_dir or log_dir
            config_file = svc_config_file or config_file

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

    def _publish(self, name, banner, timeout_sec):
        """
        Publishes the breakpoint for the host: the banner to print, plus what a reader needs
        to tell this pause from any other.
        """
        self.control.publish(banner, {
            "run": self.run,
            "seq": self.seq,
            "name": name,
            "test": self.test_name,
            "elapsed_sec": round(self.elapsed_sec, 1),
            "timeout_sec": timeout_sec,
            "banner": banner
        })

    def _await_resume(self, name, timeout_sec):
        """
        Holds the scenario until the host resumes the breakpoint, or until it gives up on its
        own.

        What each resume file means lives here rather than in the control directory: it is the
        only part of the protocol that knows there is a scenario to end.
        """
        def still_waiting(left_sec):
            self.logger.info(f"Still paused at demo breakpoint [seq={self.seq}, name={name}, "
                             f"held={_fmt_duration(timeout_sec - left_sec)}, "
                             f"left={_fmt_duration(left_sec)}]")

        taken = self.control.await_any([ABORT, CONTINUE_ALL, continue_file(self.seq)], timeout_sec,
                                       tick=still_waiting, tick_sec=HEARTBEAT_SEC)

        # Whatever ended the wait, the banner describes a breakpoint that is over.
        self.control.clear_status()

        if taken == ABORT:
            raise AssertionError(f"Demo aborted at breakpoint [seq={self.seq}, name={name}]")

        if taken == CONTINUE_ALL:
            self._continue_all = True

            self.logger.info(f"Demo resumed, remaining breakpoints skipped [seq={self.seq}, name={name}]")
        elif taken is None:
            self.logger.warn(f"Demo breakpoint timed out after {timeout_sec}s, resuming "
                             f"[seq={self.seq}, name={name}]")
        else:
            self.logger.info(f"Demo resumed [seq={self.seq}, name={name}]")
