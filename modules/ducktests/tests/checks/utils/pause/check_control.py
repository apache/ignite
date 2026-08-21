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
Checks the file protocol a demo breakpoint and the host speak over the control directory:
what a held breakpoint publishes, what resumes it, and what is swept up afterwards.
"""

import os

import pytest

from ignitetest.utils.pause import ALL
from ignitetest.utils.pause_control import ABORT, CONTINUE_ALL, STATUS_JSON, STATUS_TXT, continue_file

from checks.support.demo_pause_control import TEST_NAME, new_demo_pause, published_status, resume_with


def check_publishes_and_consumes_status(tmp_path):
    """
    Check the published breakpoint - what the host reads - and that the test cleans it up
    once resumed, so a stale banner never outlives the pause it describes.
    """
    demo = new_demo_pause(tmp_path, demo_pause=True)

    with published_status(tmp_path, resume=continue_file(1)) as published:
        demo.pause("split-brain", services=[])

    assert published["seq"] == 1
    assert published["run"] == demo.run
    assert published["name"] == "split-brain"
    assert published["test"] == TEST_NAME
    assert any("PAUSED 1   split-brain" in line for line in published["banner"])

    for leftover in (STATUS_JSON, STATUS_TXT, continue_file(1)):
        assert not os.path.exists(str(tmp_path / leftover)), leftover


def check_continue_all_skips_the_rest(tmp_path):
    """
    Check that continue-all resumes the current breakpoint and disables every later one, so
    a demo can be cut short without restarting the scenario.
    """
    demo = new_demo_pause(tmp_path, demo_pause=ALL)

    resume_with(tmp_path, CONTINUE_ALL)

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
    demo = new_demo_pause(tmp_path, demo_pause=ALL)

    resume_with(tmp_path, ABORT)

    with pytest.raises(AssertionError, match="split-brain"):
        demo.pause("split-brain")

    assert not os.path.exists(str(tmp_path / ABORT))
    assert not os.path.exists(str(tmp_path / STATUS_JSON))


def check_a_control_directory_that_cannot_be_made_does_not_fail_the_scenario(tmp_path):
    """
    Check that a control directory which cannot even be created costs the demo and nothing
    else. It is a bind mount of the host repository, so it can be read only or owned by
    another user - neither of which says anything about the cluster under test, and a
    breakpoint must not be what turns a passing run red.
    """
    # A file where the directory should go: the portable way to make os.makedirs fail.
    (tmp_path / "in-the-way").write_text("not a directory", encoding="utf-8")

    demo = new_demo_pause(tmp_path / "in-the-way" / "control", demo_pause=ALL)

    demo.pause("split-brain")

    assert demo.seq == 0, "the scenario must have carried straight on"
    assert not demo.enabled, "and the later breakpoints must not try it again"
    assert any("control directory cannot be used" in msg for msg in demo.logger.messages), demo.logger.messages


def check_a_breakpoint_that_cannot_be_published_is_skipped(tmp_path):
    """
    Check the same where the directory exists but the banner cannot be written. Blocking would
    be no better than raising here: nothing reached the host, so there would be nothing on
    screen to resume, and the scenario would sit there for the whole timeout.
    """
    # A directory where the banner goes makes the write fail wherever these checks run. The
    # sweep cannot remove it either, so it is still in the way when the breakpoint publishes.
    os.mkdir(str(tmp_path / STATUS_TXT))

    demo = new_demo_pause(tmp_path, demo_pause=ALL)

    demo.pause("split-brain")

    assert not demo.enabled
    assert not os.path.exists(str(tmp_path / STATUS_JSON)), "half a breakpoint must not be left published"
    assert any("control directory cannot be used" in msg for msg in demo.logger.messages), demo.logger.messages


def check_stale_resume_file_is_cleared(tmp_path):
    """
    Check that a resume file left by a previous run does not skip the first breakpoint of
    this one - the control directory outlives a test, its contents must not.
    """
    open(str(tmp_path / continue_file(1)), "w").close()
    open(str(tmp_path / STATUS_TXT), "w").close()

    demo = new_demo_pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=.3)

    demo.pause("split-brain")

    assert demo.seq == 1
    assert any("timed out" in msg for msg in demo.logger.messages), \
        "the stale file must have been cleared, leaving the breakpoint to time out"
