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
This module contains PME free switch tests.
"""

import time

from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST_2_7, V_2_8_0, IgniteVersion


# pylint: disable=W0223
class PmeFreeSwitchTest(IgniteTest):
    """
    Tests PME free switch scenarios.
    """
    NUM_NODES = 3

    @cluster(num_nodes=NUM_NODES + 2)
    @ignite_versions(str(DEV_BRANCH), str(LATEST_2_7))
    def test(self, ignite_version):
        """
        Test PME free scenario (node stop).
        """
        data = {}

        self.stage("Starting nodes")

        config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            caches=[CacheConfiguration(name='test-cache', backups=2, atomicity_mode='TRANSACTIONAL')]
        )

        ignites = IgniteService(self.test_context, config, num_nodes=self.NUM_NODES)

        ignites.start()

        self.stage("Starting long_tx_streamer")

        client_config = config._replace(client_mode=True,
                                        discovery_spi=from_ignite_cluster(ignites, slice(0, self.NUM_NODES - 1)))

        long_tx_streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.pme_free_switch_test.LongTxStreamerApplication",
            params={"cacheName": "test-cache"})

        long_tx_streamer.start()

        self.stage("Starting single_key_tx_streamer")

        single_key_tx_streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.pme_free_switch_test."
                            "SingleKeyTxStreamerApplication",
            params={"cacheName": "test-cache", "warmup": 1000})

        single_key_tx_streamer.start()

        if IgniteVersion(ignite_version) >= V_2_8_0:
            ControlUtility(ignites, self.test_context).disable_baseline_auto_adjust()

        self.stage("Stopping server node")

        ignites.stop_node(ignites.nodes[self.NUM_NODES - 1])

        long_tx_streamer.await_event("Node left topology", 60, from_the_beginning=True)

        time.sleep(30)  # keeping txs alive for 30 seconds.

        self.stage("Stopping long_tx_streamer")

        long_tx_streamer.stop()

        self.stage("Stopping single_key_tx_streamer")

        single_key_tx_streamer.stop()

        data["Worst latency (ms)"] = single_key_tx_streamer.extract_result("WORST_LATENCY")
        data["Streamed txs"] = single_key_tx_streamer.extract_result("STREAMED")
        data["Measure duration (ms)"] = single_key_tx_streamer.extract_result("MEASURE_DURATION")

        return data
