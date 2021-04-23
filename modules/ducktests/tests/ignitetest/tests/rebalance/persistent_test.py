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
# pylint: disable=W0622
from ducktape.errors import TimeoutError
from ducktape.utils.util import wait_until

from ignitetest.services.utils.control_utility import ControlUtility, ControlUtilityError
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.tests.rebalance.in_memory_test import InMemoryTest
from ignitetest.utils.version import IgniteVersion


# pylint: disable=W0223
class PersistentTest(InMemoryTest):
    """
    Tests rebalance scenarios in persistent mode.
    """

    # pylint: disable=too-many-arguments
    def _build_config(self, ignite_version, backups, cache_count, entry_count, entry_size,
                      thread_pool_size, batch_size, batches_prefetch_count, throttle):
        return IgniteConfiguration(
            cluster_state="INACTIVE",
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(persistent=True)),
            metric_exporter="org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi",
            rebalance_batch_size=batch_size,
            rebalance_batches_prefetch_count=batches_prefetch_count,
            rebalance_thread_pool_size=thread_pool_size,
            rebalance_throttle=throttle)

    # pylint: disable=no-member
    def _start_cluster(self, node_config, num_nodes):
        ignites = super()._start_cluster(node_config, num_nodes)

        ignites.control = ControlUtility(ignites)
        ignites.control.activate()
        ignites.control.enable_baseline_auto_adjust(1000)

        return ignites

    def _do_rebalance_trigger_event(self, trigger_event, ignites, node_config):
        rebalance_nodes = super()._do_rebalance_trigger_event(trigger_event, ignites, node_config)

        self.logger.info("trigger_event / not trigger_event: %s / %s" % (str(trigger_event), str(not trigger_event)))

        if trigger_event:  # TriggerEvent.NODE_LEFT
            left = ignites.nodes[len(ignites.nodes) - 1]
            try:
                wait_until(lambda: not ignites.alive(left),
                           timeout_sec=ignites.shutdown_timeout_sec)
            except TimeoutError:
                self.logger.warn("Stopped node %s still is alive after timeout %d"
                                 % (left.name, ignites.shutdown_timeout_sec))

        def __bl_changed():
            try:
                return len(ignites.control.baseline()) == len(ignites.nodes) + (1 if not trigger_event else -1)
            except ControlUtilityError:
                return False

        wait_until(__bl_changed, timeout_sec=5)

        if not trigger_event:  # TriggerEvent.NODE_JOIN
            IgniteAwareService.await_event_on_node(
                "Checkpoint finished \\[cpId=%s" % IgniteAwareService.select_from_log(
                    rebalance_nodes[0],
                    "enable-durability-rebalance-finished",
                    "^.+Checkpoint started \\[checkpointId=([^,]+),.+$"),
                rebalance_nodes[0],
                3600)

        return rebalance_nodes
