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
This module contains control.sh utility tests.
"""
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from jinja2 import Template

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility, ControlUtilityError
from ignitetest.utils import version_if
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST_2_8, IgniteVersion, LATEST_2_7, V_2_8_0


# pylint: disable=W0223
class BaselineTests(IgniteTest):
    """
    Tests baseline command
    """
    NUM_NODES = 3

    CONFIG_TEMPLATE = """
        {% if version > "2.9.0" %}
            <property name="clusterStateOnStart" value="INACTIVE"/>
        {%  else %}
            <property name="activeOnStart" value="false"/>
        {% endif %}
        <property name="dataStorageConfiguration">
            <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
                <property name="defaultDataRegionConfiguration">
                    <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                        <property name="persistenceEnabled" value="true"/>
                        <property name="maxSize" value="#{100L * 1024 * 1024}"/>
                    </bean>
                </property>
            </bean>
        </property>
    """

    @staticmethod
    def properties(version):
        """
        Render properties for ignite node configuration.
        """
        return Template(BaselineTests.CONFIG_TEMPLATE) \
            .render(version=version)

    def __init__(self, test_context):
        super().__init__(test_context)
        self.servers = None

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_8))
    @parametrize(version=str(LATEST_2_7))
    def test_baseline_set(self, version):
        """
        Test baseline set.
        """
        blt_size = self.NUM_NODES - 2
        self.servers = self.__start_ignite_nodes(version, blt_size)

        control_utility = ControlUtility(self.servers, self.test_context)
        control_utility.activate()

        # Check baseline of activated cluster.
        baseline = control_utility.baseline()
        self.__check_baseline_size(baseline, blt_size)
        self.__check_nodes_in_baseline(self.servers.nodes, baseline)

        # Set baseline using list of conststent ids.
        new_node = self.__start_ignite_nodes(version, 1)
        control_utility.set_baseline(self.servers.nodes + new_node.nodes)
        blt_size += 1

        baseline = control_utility.baseline()
        self.__check_baseline_size(baseline, blt_size)
        self.__check_nodes_in_baseline(new_node.nodes, baseline)

        # Set baseline using topology version.
        new_node = self.__start_ignite_nodes(version, 1)
        _, version, _ = control_utility.cluster_state()
        control_utility.set_baseline(version)
        blt_size += 1

        baseline = control_utility.baseline()
        self.__check_baseline_size(baseline, blt_size)
        self.__check_nodes_in_baseline(new_node.nodes, baseline)

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_8))
    @parametrize(version=str(LATEST_2_7))
    def test_baseline_add_remove(self, version):
        """
        Test add and remove nodes from baseline.
        """
        blt_size = self.NUM_NODES - 1
        self.servers = self.__start_ignite_nodes(version, blt_size)

        control_utility = ControlUtility(self.servers, self.test_context)

        control_utility.activate()

        # Add node to baseline.
        new_node = self.__start_ignite_nodes(version, 1)
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

        self.servers.await_event("Node left topology", timeout_sec=30, from_the_beginning=True)

        control_utility.remove_from_baseline(new_node.nodes)
        blt_size -= 1

        baseline = control_utility.baseline()
        self.__check_baseline_size(baseline, blt_size)
        self.__check_nodes_not_in_baseline(new_node.nodes, baseline)

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_8))
    @parametrize(version=str(LATEST_2_7))
    def test_activate_deactivate(self, version):
        """
        Test activate and deactivate cluster.
        """
        self.servers = self.__start_ignite_nodes(version, self.NUM_NODES)

        control_utility = ControlUtility(self.servers, self.test_context)

        control_utility.activate()

        state, _, _ = control_utility.cluster_state()

        assert state.lower() == 'active', 'Unexpected state %s' % state

        control_utility.deactivate()

        state, _, _ = control_utility.cluster_state()

        assert state.lower() == 'inactive', 'Unexpected state %s' % state

    @cluster(num_nodes=NUM_NODES)
    @version_if(lambda version: version >= V_2_8_0)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_8))
    def test_baseline_autoadjust(self, version):
        """
        Test activate and deactivate cluster.
        """
        blt_size = self.NUM_NODES - 2
        self.servers = self.__start_ignite_nodes(version, blt_size)

        control_utility = ControlUtility(self.servers, self.test_context)
        control_utility.activate()

        # Add node.
        control_utility.enable_baseline_auto_adjust(2000)
        new_node = self.__start_ignite_nodes(version, 1)
        blt_size += 1

        wait_until(lambda: len(control_utility.baseline()) == blt_size, timeout_sec=5)

        baseline = control_utility.baseline()
        self.__check_nodes_in_baseline(new_node.nodes, baseline)

        # Add node when auto adjust disabled.
        control_utility.disable_baseline_auto_adjust()
        old_topology = control_utility.cluster_state().topology_version
        new_node = self.__start_ignite_nodes(version, 1)

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

    def __start_ignite_nodes(self, version, num_nodes, timeout_sec=180):
        ignite_version = IgniteVersion(version)

        servers = IgniteService(self.test_context, num_nodes=num_nodes, version=ignite_version,
                                properties=self.properties(ignite_version))

        servers.start(timeout_sec=timeout_sec)

        return servers
