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
from enum import IntEnum

from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions, cluster, ignore_if
from ignitetest.utils.enum import constructible
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, V_2_8_0, IgniteVersion, LATEST
from ignitetest.services.utils.ssl.ssl_params import is_ssl_enabled


@constructible
class LoadType(IntEnum):
    """
    Load type.
    """
    NONE = 0
    EXTRA_CACHES = 1
    LONG_TXS = 2


class PmeFreeSwitchTest(IgniteTest):
    """
    Tests PME free switch scenarios.
    """
    NUM_NODES = 9
    EXTRA_CACHES_AMOUNT = 100

    @cluster(num_nodes=NUM_NODES + 2)
    @ignore_if(lambda version, globals: version < V_2_8_0 and is_ssl_enabled(globals))
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(load_type=[LoadType.NONE, LoadType.EXTRA_CACHES, LoadType.LONG_TXS])
    def test(self, ignite_version, load_type):
        """
        Tests PME-free switch scenario (node stop).
        """
        data = {}

        caches = [CacheConfiguration(name='test-cache', backups=2, atomicity_mode='TRANSACTIONAL')]

        l_type = LoadType.construct_from(load_type)

        # Checking PME (before 2.8) vs PME-free (2.8+) switch duration, but
        # focusing on switch duration (which depends on caches amount) when long_txs is false and
        # on waiting for previously started txs before the switch (which depends on txs duration) when long_txs of true.
        if l_type is LoadType.EXTRA_CACHES:
            for idx in range(1, self.EXTRA_CACHES_AMOUNT):
                caches.append(CacheConfiguration(name="cache-%d" % idx, backups=2, atomicity_mode='TRANSACTIONAL'))

        config = IgniteConfiguration(version=IgniteVersion(ignite_version), caches=caches, cluster_state="INACTIVE")

        num_nodes = self.available_cluster_size - 2

        self.test_context.logger.info("Nodes amount calculated as %d." % num_nodes)

        ignites = IgniteService(self.test_context, config, num_nodes=num_nodes)

        ignites.start()

        if IgniteVersion(ignite_version) >= V_2_8_0:
            ControlUtility(ignites).disable_baseline_auto_adjust()

        ControlUtility(ignites).activate()

        client_config = config._replace(client_mode=True,
                                        discovery_spi=from_ignite_cluster(ignites, slice(0, num_nodes - 1)))

        long_tx_streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.pme_free_switch_test.LongTxStreamerApplication",
            params={"cacheName": "test-cache"},
            startup_timeout_sec=180)

        if l_type is LoadType.LONG_TXS:
            long_tx_streamer.start()

        single_key_tx_streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.pme_free_switch_test."
                            "SingleKeyTxStreamerApplication",
            params={"cacheName": "test-cache", "warmup": 1000},
            startup_timeout_sec=180)

        single_key_tx_streamer.start()

        ignites.stop_node(ignites.nodes[num_nodes - 1])

        single_key_tx_streamer.await_event("Node left topology", 60, from_the_beginning=True)

        if l_type is LoadType.LONG_TXS:
            time.sleep(30)  # keeping txs alive for 30 seconds.

            long_tx_streamer.stop_async()

            single_key_tx_streamer.await_event("Node left topology", 60, from_the_beginning=True)

        single_key_tx_streamer.await_event("APPLICATION_STREAMED", 60)  # waiting for streaming continuation.

        single_key_tx_streamer.stop()

        data["Worst latency (ms)"] = single_key_tx_streamer.extract_result("WORST_LATENCY")
        data["Streamed txs"] = single_key_tx_streamer.extract_result("STREAMED")
        data["Measure duration (ms)"] = single_key_tx_streamer.extract_result("MEASURE_DURATION")
        data["Server nodes"] = num_nodes

        return data
