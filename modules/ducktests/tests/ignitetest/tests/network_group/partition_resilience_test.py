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

from ignitetest.services.ignite import IgniteService
from ignitetest.services.network_group.configuration import NetworkGroupStore, CrossNetworkGroupConfiguration
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.tests.network_group import NetworkGroupAbstractTest
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

NUM_NODES = 12

DC_1_NAME = "DC1"
DC_2_NAME = "DC2"


class MultiDCPartitionResilienceTest(NetworkGroupAbstractTest):
    """
    Tests for cluster network partition resilience in MultiDC
    """
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(cross_dc_latency_ms=[20], partition_time_sec=[30])
    def test_mdc_cluster_partition_resilience(self, ignite_version, cross_dc_latency_ms, partition_time_sec):
        self.configure_network_and_run(ignite_version=ignite_version, cross_dc_latency_ms=cross_dc_latency_ms,
                                       partition_time_sec=partition_time_sec)

    def _configure_network_group_store(self, **kwargs) -> NetworkGroupStore:
        store = super()._configure_network_group_store(**kwargs)

        # tcset expects a time expression with units, not a bare integer.
        dc1_dc2_cfg = CrossNetworkGroupConfiguration(delay=f"{kwargs['cross_dc_latency_ms']}ms")
        store.set_config(DC_1_NAME, DC_2_NAME, dc1_dc2_cfg)

        return store

    def _configure_services(self, **kwargs):
        self.ign_cfg = IgniteConfiguration(version=IgniteVersion(kwargs['ignite_version']))

        dc_1_nodes_num, dc_2_nodes_num = NUM_NODES // 2, NUM_NODES // 2

        self.svc_dc_1 = IgniteService(self.test_context, self.ign_cfg, num_nodes=dc_1_nodes_num,
                                      jvm_opts=[f"-DIGNITE_DATA_CENTER_ID={DC_1_NAME}"])
        self.svc_dc_2 = IgniteService(self.test_context, self.ign_cfg, num_nodes=dc_2_nodes_num,
                                      jvm_opts=[f"-DIGNITE_DATA_CENTER_ID={DC_2_NAME}"])

    def _configure_network_group_registry(self, **kwargs):
        return {
            DC_1_NAME: [self.svc_dc_1],
            DC_2_NAME: [self.svc_dc_2]
        }

    def _run(self, network_mgr, **kwargs):
        for svc in [self.svc_dc_1, self.svc_dc_2]:
            svc.start()

        network_mgr.enable_network_partition(DC_1_NAME, DC_2_NAME)

        sleep(kwargs['partition_time_sec'])

        network_mgr.disable_network_partition(DC_1_NAME, DC_2_NAME)

        self._verify_half_ring_healthy(self.svc_dc_1)
        self._verify_half_ring_healthy(self.svc_dc_2)

        self.svc_dc_1.stop()
        self.svc_dc_2.stop()

    @staticmethod
    def _verify_half_ring_healthy(svc: IgniteService):
        exp_alive_nodes = NUM_NODES // 2
        act_alive_nodes = len(svc.alive_nodes)

        assert act_alive_nodes == exp_alive_nodes, f"{exp_alive_nodes} nodes should be alive! [actual={act_alive_nodes}]"

        coordinator_node = svc.nodes[0]

        assert svc.alive(coordinator_node), "Coordinator node should remain alive"

        disco_info = coordinator_node.discovery_info()

        assert disco_info.is_coordinator, f"{svc.who_am_i(coordinator_node)} is not a coordinator"

        control_utility = ControlUtility(svc)

        cluster_state = control_utility.cluster_state()

        assert "ACTIVE" == cluster_state.state, f"Half-ring state should remain ACTIVE [actual={cluster_state.state}]"

        assert len(cluster_state.baseline) == exp_alive_nodes, \
            f"Half-ring baseline is not expected [exp={exp_alive_nodes}, actual={cluster_state.baseline}]"
