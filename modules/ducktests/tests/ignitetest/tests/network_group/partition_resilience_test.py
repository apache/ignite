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
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.network_group.configuration import NetworkGroupStore, CrossNetworkGroupConfiguration
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.network_group import NetworkGroupAbstractTest
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

SRV_NODES = 10
CLI_NODES = 2
TOTAL_NODES = SRV_NODES + CLI_NODES

DC_1_NAME = "DC1"
DC_2_NAME = "DC2"

GENERATOR_JAVA_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.mdc.MdcDataGeneratorApplication"
GET_CHECKER_JAVA_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.mdc.MdcDataCheckerApplication"
PUT_CHECKER_JAVA_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.mdc.MdcPutAdmissibilityCheckerApplication"

CACHE_NAME = "replicated"

DATA_CENTER_ATTR = "IGNITE_DATA_CENTER_ID"

class MultiDCPartitionResilienceTest(NetworkGroupAbstractTest):
    """
    Tests for cluster network partition resilience in MultiDC
    """
    @cluster(num_nodes=TOTAL_NODES)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(cross_dc_latency_ms=[20])
    def test_mdc_cluster_partition_resilience(self, ignite_version, cross_dc_latency_ms):
        self.configure_network_and_run(ignite_version=ignite_version, cross_dc_latency_ms=cross_dc_latency_ms)

    def _configure_network_group_store(self, **kwargs) -> NetworkGroupStore:
        store = super()._configure_network_group_store(**kwargs)

        # tcset expects a time expression with units, not a bare integer.
        dc1_dc2_cfg = CrossNetworkGroupConfiguration(delay=f"{kwargs['cross_dc_latency_ms']}ms")
        store.set_config(DC_1_NAME, DC_2_NAME, dc1_dc2_cfg)

        return store

    def _configure_services(self, **kwargs):
        self.ign_cfg = IgniteConfiguration(version=IgniteVersion(kwargs['ignite_version']))

        dc_1_nodes_num, dc_2_nodes_num = SRV_NODES // 2, SRV_NODES // 2

        jvm_opts_dc_1 = [f"-D{DATA_CENTER_ATTR}={DC_1_NAME}"]
        jvm_opts_dc_2 = [f"-D{DATA_CENTER_ATTR}={DC_2_NAME}"]

        self.svc_dc_1 = IgniteService(self.test_context, self.ign_cfg, num_nodes=dc_1_nodes_num, jvm_opts=jvm_opts_dc_1)
        self.svc_dc_2 = IgniteService(self.test_context, self.ign_cfg, num_nodes=dc_2_nodes_num, jvm_opts=jvm_opts_dc_2)

        cli_cfg_dc_1 = self.ign_cfg._replace(client_mode=True, discovery_spi=from_ignite_cluster(self.svc_dc_1))
        cli_cfg_dc_2 = self.ign_cfg._replace(client_mode=True, discovery_spi=from_ignite_cluster(self.svc_dc_2))

        self.app_dc_1 = IgniteApplicationService(self.test_context, cli_cfg_dc_1, jvm_opts=jvm_opts_dc_1)
        self.app_dc_2 = IgniteApplicationService(self.test_context, cli_cfg_dc_2, jvm_opts=jvm_opts_dc_2)

    def _configure_network_group_registry(self, **kwargs):
        return {
            DC_1_NAME: [self.svc_dc_1, self.app_dc_1],
            DC_2_NAME: [self.svc_dc_2, self.app_dc_2]
        }

    def _run(self, network_mgr, **kwargs):
        for svc in [self.svc_dc_1, self.svc_dc_2]:
            svc.start()

        self._populate_cluster_with_data()

        control_utility = ControlUtility(self.svc_dc_1)

        distribution = control_utility.cache_distribution(
            cache_names=CACHE_NAME,
            user_attributes=DATA_CENTER_ATTR)

        self.assert_cross_dc_distribution_by_attribute(
            distribution,
            dc_attr=DATA_CENTER_ATTR,
            expected_dcs=[DC_1_NAME, DC_2_NAME])

        network_mgr.enable_network_partition(DC_1_NAME, DC_2_NAME)

        sleep(10)

        self._verify_half_ring_healthy(self.svc_dc_1)
        self._verify_half_ring_healthy(self.svc_dc_2)

        self._verify_split_brain(self.svc_dc_1, self.svc_dc_2)

        self._check_data_accessible()
        self._check_data_change_access()

        network_mgr.disable_network_partition(DC_1_NAME, DC_2_NAME)

        self.svc_dc_1.stop()
        self.svc_dc_2.stop()

    @staticmethod
    def _verify_half_ring_healthy(svc: IgniteService):
        exp_alive_nodes = SRV_NODES // 2
        act_alive_nodes = len(svc.alive_nodes)

        assert act_alive_nodes == exp_alive_nodes, f"{exp_alive_nodes} nodes should be alive! [actual={act_alive_nodes}]"

        control_utility = ControlUtility(svc)

        cluster_state = control_utility.cluster_state()

        assert "ACTIVE" == cluster_state.state, f"Half-ring state should remain ACTIVE [actual={cluster_state.state}]"

        assert len(cluster_state.baseline) == exp_alive_nodes, \
            f"Half-ring baseline is not expected [exp={exp_alive_nodes}, actual_baseline={cluster_state.baseline}]"

    def _verify_split_brain(self, svc_dc_1: IgniteService, svc_dc_2: IgniteService):
        control_utility_dc_1 = ControlUtility(svc_dc_1)
        cluster_state_dc_1 = control_utility_dc_1.cluster_state()

        control_utility_dc_2 = ControlUtility(svc_dc_2)
        cluster_state_dc_2 = control_utility_dc_2.cluster_state()

        # Check baseline doesn't intersect, each has it's own coordinator

    def _populate_cluster_with_data(self):
        self.app_dc_1.java_class_name = GENERATOR_JAVA_CLASS_NAME
        self.app_dc_2.java_class_name = GENERATOR_JAVA_CLASS_NAME

        self.app_dc_1.params = {"mainDc": DC_1_NAME, "cacheName": CACHE_NAME, "backups": 1, "from": 0, "to": 100}
        self.app_dc_2.params = {"mainDc": DC_1_NAME, "cacheName": CACHE_NAME, "backups": 1, "from": 100, "to": 200}

        self.app_dc_1.start()
        self.app_dc_2.start()

        self.app_dc_1.wait()
        self.app_dc_2.wait()

        self.app_dc_1.stop()
        self.app_dc_2.stop()

    def _check_data_accessible(self):
        self.app_dc_1.java_class_name = GET_CHECKER_JAVA_CLASS_NAME
        self.app_dc_2.java_class_name = GET_CHECKER_JAVA_CLASS_NAME

        self.app_dc_1.params = {"cacheName": CACHE_NAME, "from": 0, "to": 200}
        self.app_dc_2.params = {"cacheName": CACHE_NAME, "from": 0, "to": 200}

        self.app_dc_1.start(clean=False)
        self.app_dc_2.start(clean=False)

        self.app_dc_1.wait()
        self.app_dc_2.wait()

        self.app_dc_1.stop()
        self.app_dc_2.stop()

    def _check_data_change_access(self):
        self.app_dc_1.java_class_name = PUT_CHECKER_JAVA_CLASS_NAME
        self.app_dc_2.java_class_name = PUT_CHECKER_JAVA_CLASS_NAME

        self.app_dc_1.params = {"cacheName": CACHE_NAME, "expectAdmissible": True}
        self.app_dc_2.params = {"cacheName": CACHE_NAME, "expectAdmissible": False}

        self.app_dc_1.start(clean=False)
        self.app_dc_2.start(clean=False)

        self.app_dc_1.wait()
        self.app_dc_2.wait()

        self.app_dc_1.stop()
        self.app_dc_2.stop()

    def assert_cross_dc_distribution_by_attribute(self, distribution, dc_attr, expected_dcs, owning_only=True):
        """
        Asserts that every partition of every cache group has at least one copy in every DC,
        using a node attribute (requested via --user-attributes) as the DC marker.

        :param distribution: CacheDistribution returned by ControlUtility.cache_distribution(),
                             requested with user_attributes=[dc_attr].
        :param dc_attr: Attribute name holding the DC id, e.g. "IGNITE_DATA_CENTER_ID".
        :param expected_dcs: Collection of DC ids that must own a copy of every partition.
        :param owning_only: Count only copies in OWNING state as present.
        """

        def dc_of(copy):
            return copy.user_attributes.get(dc_attr)

        self._assert_cross_dc(distribution, set(expected_dcs), dc_of, owning_only,
                         layout_hint=f"DC attribute: {dc_attr}, expected DCs: {sorted(expected_dcs)}")

    def assert_cross_dc_distribution(self, distribution, dc_addresses, owning_only=True):
        """
        Same assertion, but DC membership is derived from node IP addresses.

        :param dc_addresses: Dict: DC name -> collection of node IP addresses of that DC,
                             e.g. {"DC1": ["192.168.64.9", ...], "DC2": [...]}.
        """
        dc_addresses = {dc: set(addrs) for dc, addrs in dc_addresses.items()}

        def dc_of(copy):
            for dc, addrs in dc_addresses.items():
                if any(addr in addrs for addr in copy.node_addresses):
                    return dc
            return None

        self._assert_cross_dc(distribution, set(dc_addresses), dc_of, owning_only,
                         layout_hint=f"DC layout: { {dc: sorted(addrs) for dc, addrs in dc_addresses.items()} }")

    @staticmethod
    def _assert_cross_dc(distribution, expected_dcs, dc_of, owning_only, layout_hint):
        violations = []

        for group in distribution.groups.values():
            for part, copies in sorted(group.partitions.items()):
                counted = [c for c in copies if not owning_only or c.state == "OWNING"]

                missing = expected_dcs - {dc_of(c) for c in counted}

                if missing:
                    copies_dump = ", ".join(
                        f"{c.node_id}({'P' if c.primary else 'B'},{c.state},dc={dc_of(c)},{c.node_addresses})"
                        for c in copies)

                    violations.append(f"group={group.name}(id={group.group_id}), partition={part}, "
                                      f"missing DCs={sorted(missing)}, copies=[{copies_dump}]")

        assert not violations, \
            "Partition distribution is not cross-DC:\n  " + "\n  ".join(violations) + "\n" + layout_hint
