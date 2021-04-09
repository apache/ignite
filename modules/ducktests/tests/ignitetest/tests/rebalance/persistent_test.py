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
Module contains persistence rebalance tests.
"""
from ducktape.mark import defaults
from ducktape.utils.util import wait_until

from ignitetest.services.utils.control_utility import ControlUtility, ControlUtilityError
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.tests.rebalance import TriggerEvent, NodeJoinLeftScenario
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST


# pylint: disable=W0223
class PersistentTest(NodeJoinLeftScenario):
    """
    Tests rebalance scenarios in persistent mode.
    """
    NUM_NODES = 4

    # pylint: disable=too-many-arguments, too-many-locals
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[15_000], entry_size=[50_000], preloaders=[1],
              thread_pool_size=[None], batch_size=[None], batches_prefetch_count=[None], throttle=[None])
    def test_node_join(self, ignite_version,
                       backups, cache_count, entry_count, entry_size, preloaders,
                       thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Tests rebalance on node join.
        """
        return self._run_scenario(ignite_version, TriggerEvent.NODE_JOIN,
                                  backups, cache_count, entry_count, entry_size, preloaders,
                                  thread_pool_size, batch_size, batches_prefetch_count, throttle)

    # pylint: disable=too-many-arguments, too-many-locals
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[15_000], entry_size=[50_000], preloaders=[1],
              thread_pool_size=[None], batch_size=[None], batches_prefetch_count=[None], throttle=[None])
    def test_node_left(self, ignite_version,
                       backups, cache_count, entry_count, entry_size, preloaders,
                       thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Tests rebalance on node left.
        """
        return self._run_scenario(ignite_version, TriggerEvent.NODE_LEFT,
                                  backups, cache_count, entry_count, entry_size, preloaders,
                                  thread_pool_size, batch_size, batches_prefetch_count, throttle)

    # pylint: disable=too-many-arguments, too-many-locals
    def _build_config(self, ignite_version, backups, cache_count, entry_count, entry_size,
                      thread_pool_size, batch_size, batches_prefetch_count, throttle):
        return IgniteConfiguration(
            cluster_state="INACTIVE",
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(persistent=True)),
            metric_exporter="org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi",
            rebalance_thread_pool_size=thread_pool_size,
            rebalance_batch_size=batch_size,
            rebalance_batches_prefetch_count=batches_prefetch_count,
            rebalance_throttle=throttle)

    def _start_cluster(self, node_config, num_nodes):
        ignites = super()._start_cluster(node_config, num_nodes)

        ignites.control = ControlUtility(ignites)
        ignites.control.activate()
        ignites.control.enable_baseline_auto_adjust(1000)

        return ignites

    def _do_rebalance_trigger_event(self, trigger_event, ignites, node_config):
        rebalance_nodes = super()._do_rebalance_trigger_event(trigger_event, ignites, node_config)

        if trigger_event:  # TriggerEvent.NODE_LEFT
            left = ignites.nodes[len(ignites.nodes) - 1]
            wait_until(lambda: not ignites.alive(left),
                       timeout_sec=ignites.shutdown_timeout_sec)

        def __bl_changed():
            try:
                return len(ignites.control.baseline()) == len(ignites.nodes) + (1 if not trigger_event else -1)
            except ControlUtilityError:
                return False

        wait_until(__bl_changed, timeout_sec=5)

        if not trigger_event:  # TriggerEvent.NODE_JOIN
            IgniteAwareService.await_event_on_node("Checkpoint finished", rebalance_nodes[0], 3600)

        return rebalance_nodes
