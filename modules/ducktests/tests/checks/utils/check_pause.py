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
Checks demo breakpoints.
"""

import json
import os
import threading
import time
from types import SimpleNamespace

import pytest

from ignitetest.services.utils.path import IgnitePathAware
from ignitetest.utils.pause import ALL, CONTINUE_ALL, ABORT, DemoPause, RUNNER_TIMEOUT_MARGIN_SEC, STATUS_JSON, \
    STATUS_TXT, continue_file, parse_selector


class FakeLogger:
    """
    Collects what a paused test would have logged.
    """
    def __init__(self):
        self.messages = []

    def info(self, msg):
        """Records an info message."""
        self.messages.append(msg)

    def warn(self, msg):
        """Records a warning."""
        self.messages.append(msg)

    debug = info
    error = warn


def _fake_nodes(*hostnames):
    return [SimpleNamespace(account=SimpleNamespace(hostname=host, externally_routable_ip=host))
            for host in hostnames]


class FakeService:
    """
    Stands in for a non-Ignite service of the test registry, e.g. a zookeeper one: it carries
    paths of its own, which the banner must not hand out for Ignite nodes.
    """
    log_dir = "/mnt/service/zk-logs"
    config_file = "/mnt/service/zookeeper.properties"

    def __init__(self, *hostnames):
        self.nodes = _fake_nodes(*hostnames)

    def who_am_i(self, node):
        """Names the node the way a ducktape service does."""
        return f"{self.__class__.__name__}-{node.account.hostname}"


class FakeIgniteService(IgnitePathAware):
    """
    Stands in for an Ignite service, with the real path layout behind it.
    """
    def __init__(self, *hostnames):
        self.nodes = _fake_nodes(*hostnames)

    def who_am_i(self, node):
        """Names the node the way a ducktape service does."""
        return f"{self.__class__.__name__}-{node.account.hostname}"

    @property
    def product(self):
        return "ignite-dev"

    @property
    def globals(self):
        return {}


class FakeRegistry:
    """
    Stands in for ducktape's ServiceRegistry, which is what a test hands the breakpoint: it is
    iterable and nothing else, so a banner may not index it or ask it for a length.
    """
    def __init__(self, *services):
        self._services = services

    def __iter__(self):
        return iter(self._services)


def _pause(control_dir, started_at=None, runner_timeout_sec=None, **test_globals):
    return DemoPause(FakeLogger(), test_globals, "check.CheckPause.check_something", control_dir=str(control_dir),
                     started_at=started_at, runner_timeout_sec=runner_timeout_sec)


def _read_published(control_dir, resume_with=None, timeout_sec=30):
    """
    Reads the published breakpoint while the test blocks on it, the way the host console does,
    and optionally resumes it.

    Polls for the file rather than reading it once after a fixed delay: a breakpoint that is
    only held for a fraction of a second - which is what these checks hold them for - would
    otherwise be a race against the machine the checks happen to run on.

    :return: The dict that is filled in once the breakpoint has been published, and the reader
             to join before reading it.
    """
    published = {}

    def act():
        deadline = time.monotonic() + timeout_sec

        while time.monotonic() < deadline:
            try:
                with open(os.path.join(str(control_dir), STATUS_JSON), encoding="utf-8") as file:
                    published.update(json.load(file))

                break
            except (OSError, ValueError):
                time.sleep(.01)

        if resume_with:
            open(os.path.join(str(control_dir), resume_with), "w").close()

    reader = threading.Thread(target=act, daemon=True)
    reader.start()

    return published, reader


def _resume_with(control_dir, name, delay_sec=.05):
    """
    Creates a resume file from another thread, the way the host does while the test blocks.
    """
    timer = threading.Timer(delay_sec, lambda: open(os.path.join(str(control_dir), name), "w").close())
    timer.daemon = True
    timer.start()

    return timer


def check_selector_parsing():
    """
    Check that every shape the demo_pause global can arrive in is understood: -g passes it as
    a string, -gj as whatever the json holds.
    """
    for disabled in (None, False, "", "false", "off", "0", [], "  "):
        assert parse_selector(disabled) is None, disabled

    for every in (True, "*", "all", "true", "ON", "1"):
        assert parse_selector(every) == ALL, every

    assert parse_selector("split-brain") == {"split-brain"}
    assert parse_selector("split-brain, healed ,") == {"split-brain", "healed"}
    assert parse_selector(["split-brain", "healed"]) == {"split-brain", "healed"}

    # The global is typed by hand, the names live in the test source - the two meet case insensitively.
    assert parse_selector("Split-Brain, HEALED") == {"split-brain", "healed"}
    assert parse_selector(["Split-Brain"]) == {"split-brain"}


def check_names_are_matched_case_insensitively(tmp_path):
    """
    Check that a breakpoint is found however the global spells it.
    """
    demo = _pause(tmp_path, demo_pause="Split-Brain")

    _resume_with(tmp_path, continue_file(1))

    demo.pause("split-brain")

    assert demo.seq == 1, "the global must not have to repeat the case of the name in the test"


def check_disabled_leaves_no_trace(tmp_path):
    """
    Check that without the global a breakpoint is a plain return: it must not block, and it
    must not even create the control directory, since every test carries breakpoints in CI.
    """
    control_dir = tmp_path / "control"

    demo = _pause(control_dir)

    assert not demo.enabled

    demo.pause("split-brain")

    assert not os.path.exists(str(control_dir))
    assert demo.seq == 0


def check_selected_breakpoints_only(tmp_path):
    """
    Check that only the named breakpoints stop the scenario.
    """
    demo = _pause(tmp_path, demo_pause="split-brain")

    demo.pause("cluster-up")
    demo.pause("healed")

    assert demo.seq == 0, "an unnamed breakpoint must not stop the scenario"

    _resume_with(tmp_path, continue_file(1))

    demo.pause("split-brain")

    assert demo.seq == 1


def check_publishes_and_consumes_status(tmp_path):
    """
    Check the published breakpoint - what the host reads - and that the test cleans it up
    once resumed, so a stale banner never outlives the pause it describes.
    """
    demo = _pause(tmp_path, demo_pause=True)

    published, reader = _read_published(tmp_path, resume_with=continue_file(1))

    demo.pause("split-brain", services=[])

    reader.join(30)

    assert published["seq"] == 1
    assert published["run"] == demo.run
    assert published["name"] == "split-brain"
    assert published["test"] == "check.CheckPause.check_something"
    assert any("PAUSED 1   split-brain" in line for line in published["banner"])

    for leftover in (STATUS_JSON, STATUS_TXT, continue_file(1)):
        assert not os.path.exists(str(tmp_path / leftover)), leftover


def check_continue_all_skips_the_rest(tmp_path):
    """
    Check that continue-all resumes the current breakpoint and disables every later one, so
    a demo can be cut short without restarting the scenario.
    """
    demo = _pause(tmp_path, demo_pause=ALL)

    _resume_with(tmp_path, CONTINUE_ALL)

    demo.pause("split-brain")

    assert demo.seq == 1
    assert not demo.enabled

    demo.pause("healed")

    assert demo.seq == 1, "breakpoints after continue-all must not stop the scenario"
    assert not os.path.exists(str(tmp_path / CONTINUE_ALL))


def check_abort_fails_the_test(tmp_path):
    """
    Check that abort ends the scenario through an assertion, so ducktape tears the cluster
    down instead of leaving it running.
    """
    demo = _pause(tmp_path, demo_pause=ALL)

    _resume_with(tmp_path, ABORT)

    with pytest.raises(AssertionError, match="split-brain"):
        demo.pause("split-brain")

    assert not os.path.exists(str(tmp_path / ABORT))
    assert not os.path.exists(str(tmp_path / STATUS_JSON))


def check_stale_resume_file_is_cleared(tmp_path):
    """
    Check that a resume file left by a previous run does not skip the first breakpoint of
    this one - the control directory outlives a test, its contents must not.
    """
    open(str(tmp_path / continue_file(1)), "w").close()
    open(str(tmp_path / STATUS_TXT), "w").close()

    demo = _pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=.3)

    demo.pause("split-brain")

    assert demo.seq == 1
    assert any("timed out" in msg for msg in demo.logger.messages), \
        "the stale file must have been cleared, leaving the breakpoint to time out"


def check_timeout_resumes_on_its_own(tmp_path):
    """
    Check that a forgotten breakpoint gives up rather than holding the scenario until
    ducktape kills it.
    """
    demo = _pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=.3)

    demo.pause("split-brain")

    assert demo.seq == 1
    assert demo.enabled, "a timed out breakpoint must not disable the later ones"
    assert not os.path.exists(str(tmp_path / STATUS_JSON))


def check_timeout_stays_within_the_runner_budget(tmp_path):
    """
    Check that a breakpoint gives up while ducktape's runner is still waiting: it hears
    nothing from a paused test, and killing the client takes the whole session down instead
    of just cutting the demo short. The requested timeout only ever shrinks.
    """
    demo = _pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=3600,
                  runner_timeout_sec=RUNNER_TIMEOUT_MARGIN_SEC + .3)

    demo.pause("split-brain")

    assert demo.seq == 1
    assert any("timed out" in msg for msg in demo.logger.messages), \
        "the breakpoint must not outsit the runner budget it was given"
    assert any("--test-runner-timeout" in msg for msg in demo.logger.messages), \
        "shortening a breakpoint must say what to raise to keep it"


def check_timeout_is_left_alone_within_the_runner_budget(tmp_path):
    """
    Check that the budget only ever caps the requested timeout - a demo that fits must be
    held for exactly as long as it asked for.
    """
    demo = _pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=.3, runner_timeout_sec=1800)

    published, reader = _read_published(tmp_path)

    demo.pause("split-brain")

    reader.join(30)

    assert published["timeout_sec"] == .3
    assert not any("--test-runner-timeout" in msg for msg in demo.logger.messages)


def check_elapsed_is_counted_from_test_start(tmp_path):
    """
    Check that the banner counts from the start of the test rather than from the first
    breakpoint: the setup phase of a multi-node scenario is minutes long, and a demo that
    reports t+00:00 after it hides exactly the part worth showing.
    """
    demo = _pause(tmp_path, started_at=time.monotonic() - 600, demo_pause=ALL, demo_pause_timeout_sec=.3)

    published, reader = _read_published(tmp_path)

    demo.pause("split-brain")

    reader.join(30)

    assert published["elapsed_sec"] >= 600
    assert any("t+10:00 since test start" in line for line in published["banner"])


def check_banner_is_rendered_from_the_service_registry(tmp_path):
    """
    Check that the banner is built by iterating the services alone: what a test passes is
    ducktape's ServiceRegistry, which supports nothing else.
    """
    demo = _pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=.3)

    published, reader = _read_published(tmp_path)

    demo.pause("split-brain", services=FakeRegistry(FakeService("ducker02"), FakeIgniteService("ducker03")))

    reader.join(30)

    banner = "\n".join(published["banner"]).replace("\\", "/")

    assert "FakeService-ducker02" in banner
    assert "FakeIgniteService-ducker03" in banner
    assert "/mnt/service/logs/ignite*.log" in banner, "the hints must still follow the Ignite service"
    assert "ducker02 ducker03" in banner


def check_hints_follow_the_ignite_services():
    """
    Check that the copy-pasteable commands name the Ignite paths even when a service of
    another kind was registered first, as the zookeeper discovery scenarios do.
    """
    # noinspection PyProtectedMember
    hints = "\n".join(DemoPause._hints_section([FakeService("ducker02"),  # pylint: disable=protected-access
                                                FakeIgniteService("ducker03")]))

    # The service paths come from os.path.join, which follows the control machine rather than
    # the nodes - a check that runs on Windows would otherwise see its separators.
    hints = hints.replace("\\", "/")

    assert "/mnt/service/config/ignite-config.xml" in hints
    assert "/mnt/service/logs/ignite*.log" in hints
    assert "zookeeper.properties" not in hints
    assert "zk-logs" not in hints

    # Every node is still offered, whichever service it belongs to.
    assert "ducker02 ducker03" in hints
