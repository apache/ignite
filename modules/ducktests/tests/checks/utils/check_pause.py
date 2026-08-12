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

import pytest

from ignitetest.utils.pause import ALL, CONTINUE_ALL, ABORT, DemoPause, STATUS_JSON, STATUS_TXT, \
    continue_file, parse_selector


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


def _pause(control_dir, **test_globals):
    return DemoPause(FakeLogger(), test_globals, "check.CheckPause.check_something", control_dir=str(control_dir))


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

    published = {}

    def resume():
        with open(str(tmp_path / STATUS_JSON), encoding="utf-8") as file:
            published.update(json.load(file))

        open(str(tmp_path / continue_file(1)), "w").close()

    timer = threading.Timer(.05, resume)
    timer.daemon = True
    timer.start()

    demo.pause("split-brain", services=[])

    assert published["seq"] == 1
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
