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
Module contains node rebalance tests.
"""

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.discovery import from_ignite_cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion, LATEST


# pylint: disable=W0223
class AddNodeRebalanceTest(IgniteTest):
    """
    Test basic rebalance scenarios.
    """
    NUM_NODES = 4
    PRELOAD_TIMEOUT = 60
    DATA_AMOUNT = 1000000
    REBALANCE_TIMEOUT = 60

    @cluster(num_nodes=NUM_NODES + 1)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST))
    def test_add_node(self, version):
        """
        Test performs add node rebalance test which consists of following steps:
            * Start cluster.
            * Put data to it via IgniteClientApp.
            * Start one more node and awaits for rebalance to finish.
        """
        ignite_version = IgniteVersion(version)

        self.stage("Start Ignite nodes")

        ignites = IgniteService(self.test_context, num_nodes=self.NUM_NODES - 1, version=ignite_version)

        ignites.start()

        discovery_spi = from_ignite_cluster(ignites)

        self.stage("Starting DataGenerationApplication")

        # This client just put some data to the cache.
        IgniteApplicationService(self.test_context,
                                 java_class_name="org.apache.ignite.internal.ducktest.tests.DataGenerationApplication",
                                 version=ignite_version,
                                 params={"cacheName": "test-cache", "range": self.DATA_AMOUNT},
                                 discovery_spi=discovery_spi,
                                 timeout_sec=self.PRELOAD_TIMEOUT).run()

        ignite = IgniteService(self.test_context, num_nodes=1, version=ignite_version, discovery_spi=discovery_spi)

        self.stage("Starting Ignite node")

        ignite.start()

        start = self.monotonic()

        ignite.await_event("rebalanced=true, wasRebalanced=false",
                           timeout_sec=AddNodeRebalanceTest.REBALANCE_TIMEOUT,
                           from_the_beginning=True,
                           backoff_sec=1)

        data = {"Rebalanced in (sec)": self.monotonic() - start}

        return data
