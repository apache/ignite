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
import operator
import threading
import time

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
        service.start()
        service.wait(10)

        service.clocks.sort(key=operator.itemgetter(1))

        for _ in service.clocks:
            self.logger.info("NodeClock[%s] = %d" % (_[0], _[1]))

        max_clock_spread_ms = 100
        min_res = service.clocks[0]
        max_res = service.clocks[-1]
        assert max_res[1] - min_res[1] < max_clock_spread_ms, \
            "Clock difference between %s (%d) and %s (%d) exceeds %d ms." \
            % (min_res[0], min_res[1], max_res[0], max_res[1], max_clock_spread_ms)


class GetTimeService(BackgroundThreadService):
    """
    Collects clock timestamps from a set of nodes.
    """

    def __init__(self, context, num_nodes):
        super().__init__(context, num_nodes)
        self.start_barrier = threading.Barrier(num_nodes)
        self.clocks = []

    def _worker(self, _, node):
        self.start_barrier.wait(1)
        start = time.time()
        delay = 5
        output = node.account.ssh_capture("sleep %d && date +%%s.%%3N" % delay)
        ts = float(output.next())
        correction = time.time() - start - delay
        self.logger.info("Node %s: ts = %8.3f, correction = %8.3f, corrected_ts = %8.3f" % (node.name, ts, correction, ts - correction))
        self.clocks.append((node.name, ts - correction))  # list is thread-safe

    def stop_node(self, node):
        pass
