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
from time import sleep

from ducktape.mark import matrix

from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.multi_dc.dc_service_manager import CrossDCConfigStore, CrossDCNetworkEmulatorConfig, \
    DCService
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

NUM_NODES = 12


class MultiDCTest(IgniteTest):
    """
    Tests for Multi-DC functionality.
    """
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(cross_dc_latency_ms=[20, 50, 100], partition_time_sec=[0.5, 1, 5], is_node_count_odd=[True, False])
    def test_network_partition(self, ignite_version, cross_dc_latency_ms, partition_time_sec, is_node_count_odd):
        """
        Test Ignite cluster resilience to cross dc network partition
        """
        cross_dc_cfg_store = self._get_cross_dc_cfg_store(delay_ms=cross_dc_latency_ms)
        context = self.test_context

        with DCService.scope(context, cross_dc_cfg_store) as dc_mgr:
            ign_cfg = IgniteConfiguration(version=IgniteVersion(ignite_version))

            dc_1_nodes_num = NUM_NODES // 2
            dc_2_nodes_num = NUM_NODES // 2 - 1 if is_node_count_odd else NUM_NODES // 2

            dc_1 = dc_mgr.start_ignite_service(context, ign_cfg, num_nodes=dc_1_nodes_num, svc_name="dc_1", dc_idx=1)
            dc_2 = dc_mgr.start_ignite_service(context, ign_cfg, num_nodes=dc_2_nodes_num, svc_name="dc_2", dc_idx=2)

            dc_mgr.enable_network_partition(dc_1_idx=1, dc_2_idx=2)

            sleep(partition_time_sec)

            dc_mgr.disable_network_partition(dc_1_idx=1, dc_2_idx=2)

            total_alive = sum(len(ignite.alive_nodes) for ignite in [dc_1, dc_2])

            min_dc_node_count = NUM_NODES // 2 - 1 if is_node_count_odd else NUM_NODES // 2

            assert total_alive >= min_dc_node_count, (f"At least {min_dc_node_count} nodes should be alive! "
                                                      f"[actual={total_alive}]")

            assert dc_1.alive(dc_1.nodes[0]), "Coordinator should remain alive"

            disco_info_dc_1 = dc_1.nodes[0].discovery_info()

            assert disco_info_dc_1.is_coordinator, f"{dc_1.who_am_i(dc_1.nodes[0])} is not a coordinator"

            dc_mgr.stop_service(svc_name="dc_1")
            dc_mgr.stop_service(svc_name="dc_2")

    @staticmethod
    def _get_cross_dc_cfg_store(delay_ms):
        """
        Cross DC configuration store
        """
        cross_dc_cfg_store = CrossDCConfigStore()

        dc1_dc2_config = CrossDCNetworkEmulatorConfig(delay_ms=delay_ms)

        cross_dc_cfg_store.set_cross_dc_config(from_dc=1, to_dc=2, config=dc1_dc2_config)

        return cross_dc_cfg_store
