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
from enum import IntEnum

from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.rebalance import preload_data, await_rebalance_start, await_rebalance_complete, \
    aggregate_rebalance_stats
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.enum import constructible
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST


@constructible
class TriggerEvent(IntEnum):
    """
    Rebalance trigger event.
    """
    NODE_JOIN = 0
    NODE_LEFT = 1


# pylint: disable=W0223
class RebalanceInMemoryTest(IgniteTest):
    """
    Tests rebalance scenarios in in-memory mode.
    """
    NUM_NODES = 4
    DEFAULT_DATA_REGION_SZ = 512 * 1024 * 1024

    # pylint: disable=too-many-arguments, too-many-locals
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(trigger_event=[TriggerEvent.NODE_JOIN, TriggerEvent.NODE_LEFT],
              backups=[1], cache_count=[1], entry_count=[15_000], entry_size=[50_000],
              rebalance_thread_pool_size=[None], rebalance_batch_size=[None],
              rebalance_batches_prefetch_count=[None], rebalance_throttle=[None])
    def test(self, ignite_version, trigger_event,
             backups, cache_count, entry_count, entry_size,
             rebalance_thread_pool_size, rebalance_batch_size,
             rebalance_batches_prefetch_count, rebalance_throttle):
        """
        Test performs rebalance test which consists of following steps:
            * Start cluster.
            * Put data to it via IgniteClientApp.
            * Triggering a rebalance event and awaits for rebalance to finish.
        """
        node_count = len(self.test_context.cluster) - 1

        node_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(max_size=max(
                    cache_count * entry_count * entry_size * (backups + 1),
                    self.DEFAULT_DATA_REGION_SZ))),
            metric_exporter="org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi",
            rebalance_thread_pool_size=rebalance_thread_pool_size,
            rebalance_batch_size=rebalance_batch_size,
            rebalance_batches_prefetch_count=rebalance_batches_prefetch_count,
            rebalance_throttle=rebalance_throttle)

        ignites = IgniteService(self.test_context, config=node_config,
                                num_nodes=node_count if trigger_event else node_count - 1)
        ignites.start()

        preload_time = preload_data(
            self.test_context,
            node_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            backups, cache_count, entry_count, entry_size)

        if trigger_event:
            ignites.stop_node(ignites.nodes[node_count - 1])
            ignite = ignites
        else:
            ignite = IgniteService(self.test_context, node_config._replace(discovery_spi=from_ignite_cluster(ignites)),
                                   num_nodes=1)
            ignite.start()

        start_node, start_time = await_rebalance_start(ignite)

        end_time = await_rebalance_complete(ignite, start_node, cache_count)

        rebalance_nodes = ignite.nodes[:-1] if trigger_event else ignite.nodes

        stats = aggregate_rebalance_stats(rebalance_nodes, cache_count)

        def speed(dur):
            return (int(stats.received_bytes / dur * 1000 / 1024 / 1024) / 1000.0) if dur else None

        return {
            "Rebalanced in (sec)": (end_time - start_time).total_seconds(),
            "Rebalance nodes": len(rebalance_nodes),
            "Rebalance speed (Total, MiB/sec)": speed((stats.end_time - stats.start_time).total_seconds()),
            "Rebalance speed (Average per node, MiB/sec)": speed(stats.duration),
            "Preloaded in (sec)": preload_time,
            "Preload speed (MiB/sec)":
                int(cache_count * entry_count * entry_size * 1000 / 1024 / 1024 / preload_time) / 1000.0
        }
