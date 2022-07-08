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
This module contains manipulating baseline test through control utility.
"""

from ducktape.utils.util import wait_until

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility, ControlUtilityError
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignore_if, ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion, V_2_8_0


class BaselineTests(IgniteTest):
    """
    Tests baseline command
    """
    NUM_NODES = 3

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_baseline_set(self, ignite_version):
        """
        Test baseline set.
        """
        blt_size = self.NUM_NODES - 2
        servers = self.__start_ignite_nodes(ignite_version, blt_size)

        control_utility = ControlUtility(servers)
        control_utility.activate()

        # Check baseline of activated cluster.
        baseline = control_utility.baseline()
        self.__check_baseline_size(baseline, blt_size)
        self.__check_nodes_in_baseline(servers.nodes, baseline)

        # Set baseline using list of consisttent ids.
        new_node = self.__start_ignite_nodes(ignite_version, 1, join_cluster=servers)
        control_utility.set_baseline(servers.nodes + new_node.nodes)
        blt_size += 1

        baseline = control_utility.baseline()
        self.__check_baseline_size(baseline, blt_size)
        self.__check_nodes_in_baseline(new_node.nodes, baseline)

        # Set baseline using topology version.
        new_node = self.__start_ignite_nodes(ignite_version, 1, join_cluster=servers)
        _, version, _ = control_utility.cluster_state()
        control_utility.set_baseline(version)
        blt_size += 1

        baseline = control_utility.baseline()
        self.__check_baseline_size(baseline, blt_size)
        self.__check_nodes_in_baseline(new_node.nodes, baseline)

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_baseline_add_remove(self, ignite_version):
        """
        Test add and remove nodes from baseline.
        """
        blt_size = self.NUM_NODES - 1
        servers = self.__start_ignite_nodes(ignite_version, blt_size)

        control_utility = ControlUtility(servers)

        control_utility.activate()

        # Add node to baseline.
        new_node = self.__start_ignite_nodes(ignite_version, 1, join_cluster=servers)
        control_utility.add_to_baseline(new_node.nodes)
        blt_size += 1

        baseline = control_utility.baseline()
        self.__check_baseline_size(baseline, blt_size)
        self.__check_nodes_in_baseline(new_node.nodes, baseline)

        # Expected failure (remove of online node is not allowed).
        try:
            control_utility.remove_from_baseline(new_node.nodes)

            assert False, "Remove of online node from baseline should fail!"
        except ControlUtilityError:
            pass

        # Remove of offline node from baseline.
        new_node.stop()

        servers.await_event("Node left topology", timeout_sec=30, from_the_beginning=True)

        control_utility.remove_from_baseline(new_node.nodes)
        blt_size -= 1

        baseline = control_utility.baseline()
        self.__check_baseline_size(baseline, blt_size)
        self.__check_nodes_not_in_baseline(new_node.nodes, baseline)

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_activate_deactivate(self, ignite_version):
        """
        Test activate and deactivate cluster.
        """
        servers = self.__start_ignite_nodes(ignite_version, self.NUM_NODES)

        control_utility = ControlUtility(servers)

        control_utility.activate()

        state, _, _ = control_utility.cluster_state()

        assert state.lower() == 'active', 'Unexpected state %s' % state

        control_utility.deactivate()

        state, _, _ = control_utility.cluster_state()

        assert state.lower() == 'inactive', 'Unexpected state %s' % state

    @cluster(num_nodes=NUM_NODES)
    @ignore_if(lambda version, globals: version < V_2_8_0)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_baseline_autoadjust(self, ignite_version):
        """
        Test activate and deactivate cluster.
        """
        blt_size = self.NUM_NODES - 2
        servers = self.__start_ignite_nodes(ignite_version, blt_size)

        control_utility = ControlUtility(servers)
        control_utility.activate()

        # Add node.
        control_utility.enable_baseline_auto_adjust(2000)
        new_node = self.__start_ignite_nodes(ignite_version, 1, join_cluster=servers)
        blt_size += 1

        wait_until(lambda: len(control_utility.baseline()) == blt_size, timeout_sec=5)

        baseline = control_utility.baseline()
        self.__check_nodes_in_baseline(new_node.nodes, baseline)

        # Add node when auto adjust disabled.
        control_utility.disable_baseline_auto_adjust()
        old_topology = control_utility.cluster_state().topology_version
        new_node = self.__start_ignite_nodes(ignite_version, 1, join_cluster=servers)

        wait_until(lambda: control_utility.cluster_state().topology_version != old_topology, timeout_sec=5)
        baseline = control_utility.baseline()
        self.__check_nodes_not_in_baseline(new_node.nodes, baseline)

    @staticmethod
    def __check_nodes_in_baseline(nodes, baseline):
        blset = set(node.consistent_id for node in baseline)

        for node in nodes:
            assert node.consistent_id in blset

    @staticmethod
    def __check_nodes_not_in_baseline(nodes, baseline):
        blset = set(node.consistent_id for node in baseline)

        for node in nodes:
            assert node.consistent_id not in blset

    @staticmethod
    def __check_baseline_size(baseline, size):
        assert len(baseline) == size, 'Unexpected size of baseline %d, %d expected' % (len(baseline), size)

    def __start_ignite_nodes(self, version, num_nodes, timeout_sec=60, join_cluster=None):
        config = IgniteConfiguration(
            cluster_state="INACTIVE",
            version=IgniteVersion(version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(name='persistent', persistence_enabled=True),
                regions=[DataRegionConfiguration(name='in-memory', persistence_enabled=False,
                                                 max_size=100 * 1024 * 1024)]
            )
        )

        if join_cluster:
            config._replace(discovery_spi=from_ignite_cluster(join_cluster))

        servers = IgniteService(self.test_context, config=config, num_nodes=num_nodes, startup_timeout_sec=timeout_sec)

        servers.start()

        return servers
