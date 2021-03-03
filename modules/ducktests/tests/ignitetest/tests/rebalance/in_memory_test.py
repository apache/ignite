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
Module contains in-memory rebalance tests.
"""
from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.rebalance import RebalanceTest
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST


# pylint: disable=W0223
class RebalanceInMemoryTest(RebalanceTest):
    """
    Tests rebalance scenarios in in-memory mode.
    """
    NUM_NODES = 4

    # pylint: disable=too-many-arguments
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(cache_count=[1], entry_count=[2000], entry_size=[300000],
              rebalance_thread_pool_size=[2], rebalance_batch_size=[512 * 1024], rebalance_throttle=[0])
    def test_rebalance_on_node_join(self, ignite_version,
                                    cache_count, entry_count, entry_size,
                                    rebalance_thread_pool_size, rebalance_batch_size, rebalance_throttle):
        """
        Test performs rebalance test which consists of following steps:
            * Start cluster.
            * Put data to it via IgniteClientApp.
            * Triggering a rebalance event (node join) and awaits for rebalance to finish.
        """
        node_count = len(self.test_context.cluster) - 1

        node_config = self.build_node_config(
            ignite_version, node_count, rebalance_thread_pool_size, rebalance_batch_size, rebalance_throttle)

        ignites = IgniteService(self.test_context, config=node_config, num_nodes=node_count - 1)
        ignites.start()

        self.preload_data(node_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
                          cache_count, entry_count, entry_size)

        ignite = IgniteService(self.test_context, node_config._replace(discovery_spi=from_ignite_cluster(ignites)),
                               num_nodes=1)
        ignite.start()

        self.await_rebalance_start(ignite)

        start = self.monotonic()

        self.await_rebalance_complete(ignite)

        return {"Rebalanced in (sec)": self.monotonic() - start}

    # pylint: disable=too-many-arguments
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(cache_count=[1], entry_count=[2000], entry_size=[300000],
              rebalance_thread_pool_size=[2], rebalance_batch_size=[512 * 1024], rebalance_throttle=[0])
    def test_rebalance_on_node_left(self, ignite_version,
                                    cache_count, entry_count, entry_size,
                                    rebalance_thread_pool_size, rebalance_batch_size, rebalance_throttle):
        """
        Test performs rebalance test which consists of following steps:
            * Start cluster.
            * Put data to it via IgniteClientApp.
            * Triggering a rebalance event (node left) and awaits for rebalance to finish.
        """
        node_count = len(self.test_context.cluster) - 1

        node_config = self.build_node_config(
            ignite_version, node_count, rebalance_thread_pool_size, rebalance_batch_size, rebalance_throttle)

        ignites = IgniteService(self.test_context, config=node_config, num_nodes=node_count)
        ignites.start()

        self.preload_data(node_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
                          cache_count, entry_count, entry_size)

        ignites.stop_node(ignites.nodes[len(ignites.nodes) - 1])

        node = self.await_rebalance_start(ignites)

        start = self.monotonic()

        self.await_rebalance_complete(ignites, node)

        return {"Rebalanced in (sec)": self.monotonic() - start}

    @staticmethod
    def build_node_config(ignite_version, node_count,
                          rebalance_thread_pool_size, rebalance_batch_size, rebalance_throttle):
        """
        Builds ignite configuration for cluster of node_count nodes
        """
        return IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(max_size=512 * 1024 * 1024 * node_count)),
            rebalance_thread_pool_size=rebalance_thread_pool_size,
            rebalance_batch_size=rebalance_batch_size,
            rebalance_throttle=rebalance_throttle)
