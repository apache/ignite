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
from ducktape.mark import defaults, matrix

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
    @defaults(cache_count=[1], entry_count=[60000], entry_size=[50000],
              rebalance_thread_pool_size=[2], rebalance_batch_size=[512 * 1024], rebalance_throttle=[0])
    @matrix(entry_count=[2400000], entry_size=[50000],
            rebalance_thread_pool_size=[4, 8, 16])
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
            ignite_version, cache_count, entry_count, entry_size,
            rebalance_thread_pool_size, rebalance_batch_size, rebalance_throttle)

        ignites = IgniteService(self.test_context, config=node_config, num_nodes=node_count - 1)
        ignites.start()

        preload_time = self.preload_data(node_config._replace(
            client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            cache_count, entry_count, entry_size)

        ignite = IgniteService(self.test_context, node_config._replace(discovery_spi=from_ignite_cluster(ignites)),
                               num_nodes=1)
        ignite.start()

        return self.get_rebalance_stats(ignite, preload_time, cache_count, entry_count, entry_size)

    # pylint: disable=too-many-arguments
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(cache_count=[1], entry_count=[60000], entry_size=[50000],
              rebalance_thread_pool_size=[2], rebalance_batch_size=[512 * 1024], rebalance_throttle=[0])
    @matrix(entry_count=[2400000], entry_size=[50000],
            rebalance_thread_pool_size=[4, 8, 16])
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
            ignite_version, cache_count, entry_count, entry_size,
            rebalance_thread_pool_size, rebalance_batch_size, rebalance_throttle)

        ignites = IgniteService(self.test_context, config=node_config, num_nodes=node_count)
        ignites.start()

        preload_time = self.preload_data(node_config._replace(
            client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            cache_count, entry_count, entry_size)

        ignites.stop_node(ignites.nodes[len(ignites.nodes) - 1])

        return self.get_rebalance_stats(ignites, preload_time, cache_count, entry_count, entry_size)

    # pylint: disable=too-many-arguments
    @staticmethod
    def build_node_config(ignite_version, cache_count, entry_count, entry_size,
                          rebalance_thread_pool_size, rebalance_batch_size, rebalance_throttle):
        """
        Builds ignite configuration for cluster of node_count nodes
        """
        return IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(max_size=max(
                    2 * cache_count * entry_count * entry_size,
                    512 * 1024 * 1024))),
            rebalance_thread_pool_size=rebalance_thread_pool_size,
            rebalance_batch_size=rebalance_batch_size,
            rebalance_throttle=rebalance_throttle)

    # pylint: disable=too-many-arguments
    def get_rebalance_stats(self, ignite, preload_time, cache_count, entry_count, entry_size):
        """
        Awaits rebalance start, then awaits rebalance end and measure it's time.
        Finally, returns rebalance statistics.
        """
        node = self.await_rebalance_start(ignite)

        start = self.monotonic()

        self.await_rebalance_complete(ignite, node, cache_count)

        rebalance_time = self.monotonic() - start

        total_data_size_mb = cache_count * entry_count * entry_size / 1000000

        return {"Rebalanced in (sec)": rebalance_time,
                "Preloaded in (sec):": preload_time,
                "Total data size (MB)": total_data_size_mb,
                "Rebalance speed (MB/sec)": total_data_size_mb / rebalance_time,
                "Preload speed (MB/sec)": total_data_size_mb / preload_time}
