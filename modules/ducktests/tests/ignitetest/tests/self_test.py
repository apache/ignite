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
This module contains smoke tests that checks that ducktape works as expected
"""
import threading
import time
from collections import namedtuple

from ducktape.mark.resource import cluster
from ducktape.services.background_thread import BackgroundThreadService

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite_execution_exception import IgniteExecutionException
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223
class SelfTest(IgniteTest):
    """
    Self tests
    """

    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    def test_assertion_convertion(self, ignite_version):
        """
        Test to make sure Java assertions are converted to python exceptions
        """
        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version))

        app = IgniteApplicationService(
            self.test_context,
            server_configuration,
            java_class_name="org.apache.ignite.internal.ducktest.tests.smoke_test.AssertionApplication")

        try:
            app.start()
        except IgniteExecutionException as ex:
            assert str(ex) == "Java application execution failed. java.lang.AssertionError"
        else:
            app.stop()
            assert False

    def test_clock_sync(self):
        """
        Tests if clocks are synchronized between nodes.
        """

        service = GetTimeService(self.test_context, self.test_context.cluster.num_available_nodes())

        spreads = []
        for _ in range(10):
            service.reset()
            service.start()
            service.wait(10)

            for clock in service.clocks:
                self.logger.info("NodeClock[%s] = %.3f" % (clock.name, clock.ts))

            service.clocks.sort(key=lambda x: x.ts)
            spreads.append((service.clocks[0], service.clocks[-1]))

        def dist(pair):
            return abs(pair[0].ts - pair[1].ts)

        worst_pair = max(spreads, key=dist)
        max_clock_spread_sec = 0.1
        assert dist(worst_pair) < max_clock_spread_sec, \
            "Clock difference between %s (%.3f) and %s (%.3f) exceeds %.3f s." \
            % (worst_pair[0].name, worst_pair[0].ts, worst_pair[1].name, worst_pair[1].ts, max_clock_spread_sec)


class GetTimeService(BackgroundThreadService):
    """
    Collects clock timestamps from a set of nodes.
    """

    def __init__(self, context, num_nodes):
        super().__init__(context, num_nodes)
        self.clocks = []
        self.start_barrier = threading.Barrier(self.num_nodes)

    def reset(self):
        """
        Reset service so that in could be invoked once again.
        """

        self.clocks.clear()
        self.start_barrier.reset()

    def _worker(self, _, node):
        node.account.ssh("/bin/true")

        self.start_barrier.wait(1)
        start = time.time()

        # Using ssh_capture instead of ssh_output to get output ASAP.
        output = node.account.ssh_capture("date +%s.%3N")
        node_ts = float(output.next())

        # Assumption: ssh connection establishment takes much more time than command output transmission.
        correction = time.time() - start
        NodeTime = namedtuple("NodeTime", ["name", "ts"])
        self.clocks.append(NodeTime(node.name, node_ts - correction))  # list is thread-safe

    def stop_node(self, node):
        pass
