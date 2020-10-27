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

from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions
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
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_add_node(self, ignite_version):
        """
        Test performs add node rebalance test which consists of following steps:
            * Start cluster.
            * Put data to it via IgniteClientApp.
            * Start one more node and awaits for rebalance to finish.
        """
        node_config = IgniteConfiguration(version=IgniteVersion(ignite_version))

        ignites = IgniteService(self.test_context, config=node_config, num_nodes=self.NUM_NODES - 1)
        ignites.start()

        # This client just put some data to the cache.
        app_config = node_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites))
        IgniteApplicationService(self.test_context, config=app_config,
                                 java_class_name="org.apache.ignite.internal.ducktest.tests.DataGenerationApplication",
                                 params={"cacheName": "test-cache", "range": self.DATA_AMOUNT},
                                 timeout_sec=self.PRELOAD_TIMEOUT).run()

        ignite = IgniteService(self.test_context, node_config._replace(discovery_spi=from_ignite_cluster(ignites)),
                               num_nodes=1)

        ignite.start()

        start = self.monotonic()

        ignite.await_event("rebalanced=true, wasRebalanced=false",
                           timeout_sec=AddNodeRebalanceTest.REBALANCE_TIMEOUT,
                           from_the_beginning=True,
                           backoff_sec=1)

        data = {"Rebalanced in (sec)": self.monotonic() - start}

        return data
