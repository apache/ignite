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
Checks how long a demo breakpoint may hold a scenario: the timeout the demo asks for, and
ducktape's --test-runner-timeout budget that only ever shortens it.
"""

import os

from ignitetest.utils.pause import ALL, RUNNER_TIMEOUT_MARGIN_SEC, STATUS_JSON

from checks.support.demo_pause_control import new_demo_pause, published_status


def check_timeout_resumes_on_its_own(tmp_path):
    """
    Check that a forgotten breakpoint gives up rather than holding the scenario until
    ducktape kills it.
    """
    demo = new_demo_pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=.3)

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
    demo = new_demo_pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=3600,
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
    demo = new_demo_pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=.3, runner_timeout_sec=1800)

    with published_status(tmp_path) as published:
        demo.pause("split-brain")

    assert published["timeout_sec"] == .3
    assert not any("--test-runner-timeout" in msg for msg in demo.logger.messages)
