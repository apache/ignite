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
Checks which demo breakpoints stop a scenario: how the ``demo_pause`` global is read, and
which of the breakpoints in the test it then selects.
"""

import os

from ignitetest.utils.pause import ALL, continue_file, parse_selector

from checks.support.demo_pause_control import new_demo_pause, resume_with


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
    demo = new_demo_pause(tmp_path, demo_pause="Split-Brain")

    resume_with(tmp_path, continue_file(1))

    demo.pause("split-brain")

    assert demo.seq == 1, "the global must not have to repeat the case of the name in the test"


def check_disabled_leaves_no_trace(tmp_path):
    """
    Check that without the global a breakpoint is a plain return: it must not block, and it
    must not even create the control directory, since every test carries breakpoints in CI.
    """
    control_dir = tmp_path / "control"

    demo = new_demo_pause(control_dir)

    assert not demo.enabled

    demo.pause("split-brain")

    assert not os.path.exists(str(control_dir))
    assert demo.seq == 0


def check_selected_breakpoints_only(tmp_path):
    """
    Check that only the named breakpoints stop the scenario.
    """
    demo = new_demo_pause(tmp_path, demo_pause="split-brain")

    demo.pause("cluster-up")
    demo.pause("healed")

    assert demo.seq == 0, "an unnamed breakpoint must not stop the scenario"

    resume_with(tmp_path, continue_file(1))

    demo.pause("split-brain")

    assert demo.seq == 1
