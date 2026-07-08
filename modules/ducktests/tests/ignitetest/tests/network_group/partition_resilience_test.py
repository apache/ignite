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
from ducktape.utils.util import wait_until

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

# How long to wait for the half-ring to reach the expected cluster state
# after the partition heals. Discovery failure detection, ring closure and
# the state transition are asynchronous, so an immediate check is racy.
CLUSTER_STATE_TIMEOUT_SEC = 60


class MultiDCPartitionResilienceTest(NetworkGroupAbstractTest):
    """
    Tests for cluster network partition resilience in MultiDC
    """
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    # NOTE: for the partition to be reliably *detected*, partition_time_sec must
    # exceed discovery failureDetectionTimeout + connectionRecoveryTimeout
    # (~10s + 10s by default). Sub-detection values (e.g. 0.5s) exercise the
    # "partition heals before the cluster reacts" case instead.
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

        self._verify_half_ring_status(self.svc_dc_1, "ACTIVE")
        self._verify_half_ring_status(self.svc_dc_2, "ACTIVE")

        self._verify_cluster_survived()

        self.svc_dc_1.stop()
        self.svc_dc_2.stop()

    def _verify_cluster_survived(self):
        total_alive = sum(len(svc.alive_nodes) for svc in [self.svc_dc_1, self.svc_dc_2])

        assert total_alive == NUM_NODES, f"{NUM_NODES} nodes should be alive! [actual={total_alive}]"

        coordinator_node = self.svc_dc_1.nodes[0]

        assert self.svc_dc_1.alive(coordinator_node), "Coordinator should remain alive"

        disco_info_dc_1 = coordinator_node.discovery_info()

        assert disco_info_dc_1.is_coordinator, f"{self.svc_dc_1.who_am_i(coordinator_node)} is not a coordinator"

    @staticmethod
    def _verify_half_ring_status(svc: IgniteService, status: str, timeout_sec: int = CLUSTER_STATE_TIMEOUT_SEC):
        """
        Polls the half-ring cluster state until it reaches the expected status.
        The state transition after a partition is asynchronous (failure detection,
        ring closure, state change exchange), so a one-shot assertion is racy.
        """
        control_utility = ControlUtility(svc)

        def has_expected_status():
            try:
                return status in control_utility.cluster_state()
            except Exception:
                # control.sh may transiently fail while the ring is re-forming.
                return False

        wait_until(has_expected_status, timeout_sec=timeout_sec, backoff_sec=1,
                   err_msg=f"Half-ring did not reach {status} status within {timeout_sec}s "
                           f"[last known state check failed or mismatched]")
