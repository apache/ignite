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
Checks the banner a held demo breakpoint renders: how long the scenario has been running, the
nodes it is made of, and the commands offered for looking into them.
"""

import time

from ignitetest.utils.pause import ALL, DemoPause

from checks.support.demo_pause_control import new_demo_pause, published_status
from checks.support.ducktape_doubles import FakeBrokenService, FakeIgniteService, FakeRegistry, FakeService


def check_elapsed_is_counted_from_test_start(tmp_path):
    """
    Check that the banner counts from the start of the test rather than from the first
    breakpoint: the setup phase of a multi-node scenario is minutes long, and a demo that
    reports t+00:00 after it hides exactly the part worth showing.
    """
    demo = new_demo_pause(tmp_path, started_at=time.monotonic() - 600, demo_pause=ALL, demo_pause_timeout_sec=.3)

    with published_status(tmp_path) as published:
        demo.pause("split-brain")

    assert published["elapsed_sec"] >= 600
    assert any("t+10:00 since test start" in line for line in published["banner"])


def check_banner_is_rendered_from_the_service_registry(tmp_path):
    """
    Check that the banner is built by iterating the services alone: what a test passes is
    ducktape's ServiceRegistry, which supports nothing else.
    """
    demo = new_demo_pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=.3)

    with published_status(tmp_path) as published:
        demo.pause("split-brain", services=FakeRegistry(FakeService("ducker02"), FakeIgniteService("ducker03")))

    banner = "\n".join(published["banner"]).replace("\\", "/")

    assert "FakeService-ducker02" in banner
    assert "FakeIgniteService-ducker03" in banner
    assert "/mnt/service/logs/ignite*.log" in banner, "the hints must still follow the Ignite service"
    assert "ducker02 ducker03" in banner


def check_a_service_that_cannot_answer_is_degraded_not_raised(tmp_path):
    """
    Check that a service which cannot answer for its nodes costs the banner those lines and
    nothing more. A breakpoint only observes the cluster, so one that throws while rendering
    would fail the scenario at exactly the point the demo was added to show.
    """
    demo = new_demo_pause(tmp_path, demo_pause=ALL, demo_pause_timeout_sec=.3)

    with published_status(tmp_path) as published:
        demo.pause("split-brain",
                   services=FakeRegistry(FakeBrokenService("ducker02"), FakeIgniteService("ducker03")))

    banner = "\n".join(published["banner"])

    assert "FakeBrokenService-ducker02" in banner, "the node must still be named, without asking its service"
    assert "RuntimeError" in banner, "and what could not be read must say so"
    assert "FakeIgniteService-ducker03" in banner, "a service after the broken one must still be listed"
    assert "ducker02 ducker03" in banner, "and every node must still be offered by the hints"


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
