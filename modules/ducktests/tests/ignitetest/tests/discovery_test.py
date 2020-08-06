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
Module contains discovery tests.
"""

import random

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from jinja2 import Template

from ignitetest.services.ignite import IgniteService
from ignitetest.services.zk.zookeeper import ZookeeperService
from ignitetest.tests.utils.ignite_test import IgniteTest
from ignitetest.tests.utils.version import DEV_BRANCH, LATEST_2_7


# pylint: disable=W0223
class DiscoveryTest(IgniteTest):
    """
    Test basic discovery scenarious (TCP and Zookeeper).
    1. Start of ignite cluster.
    2. Kill random node.
    3. Wait that survived node detects node failure.
    """
    NUM_NODES = 7

    CONFIG_TEMPLATE = """
        {% if zookeeper_settings %}
        {% with zk = zookeeper_settings %}
        <property name="discoverySpi">
            <bean class="org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi">
                <property name="zkConnectionString" value="{{ zk.connection_string }}"/>
                <property name="sessionTimeout" value="{{ zk.session_timeout or 3000 }}"/>
                <property name="zkRootPath" value="{{ zk.root_path or '/apacheIgnite' }}"/>
                <property name="joinTimeout" value="{{ zk.join_timeout or 10000 }}"/>
            </bean>
        </property>
        {% endwith %}
        {% endif %}
    """

    def __init__(self, test_context):
        super(DiscoveryTest, self).__init__(test_context=test_context)
        self.zk_quorum = None
        self.servers = None

    @staticmethod
    def properties(zookeeper_settings=None):
        """
        :param zookeeper_settings: ZookeperDiscoverySpi settings. If None, TcpDiscoverySpi will be used.
        :return: Rendered node's properties.
        """
        return Template(DiscoveryTest.CONFIG_TEMPLATE) \
            .render(zookeeper_settings=zookeeper_settings)

    def setUp(self):
        pass

    def teardown(self):
        if self.zk_quorum:
            self.zk_quorum.stop()

        if self.servers:
            self.servers.stop()

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_7))
    def test_tcp_not_coordinator_single(self, version):
        """
        Test single-node-failure scenario (not the coordinator) with TcpDiscoverySpi.
        """
        return self.__simulate_nodes_failure(version, nodes_to_kill=1)

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_7))
    def test_tcp_not_coordinator_two(self, version):
        """
        Test two-node-failure scenario (not the coordinator) with TcpDiscoverySpi.
        """
        return self.__simulate_nodes_failure(version, nodes_to_kill=2)

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_7))
    def test_tcp_coordinator(self, version):
        """
        Test coordinator-failure scenario with TcpDiscoverySpi.
        """
        return self.__simulate_nodes_failure(version, True)

    @cluster(num_nodes=NUM_NODES + 3)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_7))
    def test_zk_not_coordinator_single(self, version):
        """
        Test single node failure scenario (not the coordinator) with ZooKeeper.
        """
        return self.__simulate_nodes_failure(version, nodes_to_kill=1, coordinator=False, with_zk=True)

    @cluster(num_nodes=NUM_NODES + 3)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_7))
    def test_zk_not_coordinator_two(self, version):
        """
        Test two-node-failure scenario (not the coordinator) with ZooKeeper.
        """
        return self.__simulate_nodes_failure(version, nodes_to_kill=2, coordinator=False, with_zk=True)

    @cluster(num_nodes=NUM_NODES+3)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_7))
    def test_zk_coordinator(self, version):
        """
        Test coordinator-failure scenario with ZooKeeper.
        """
        return self.__simulate_nodes_failure(version, coordinator=True, with_zk=True)

    # pylint: disable=R0913,R0914
    def __simulate_nodes_failure(self, version, coordinator=False, with_zk=False, nodes_to_kill=1):
        if with_zk:
            self.zk_quorum = ZookeeperService(self.test_context, 3)
            self.stage("Starting ZooKeeper quorum")
            self.zk_quorum.start()
            properties = self.properties(zookeeper_settings={'connection_string': self.zk_quorum.connection_string()})
            self.stage("ZooKeeper quorum started")
        else:
            properties = self.properties()

        self.servers = IgniteService(
            self.test_context,
            num_nodes=self.NUM_NODES,
            modules=["ignite-zookeeper"],
            properties=properties,
            version=version)

        self.stage("Starting ignite cluster")

        start = self.monotonic()
        self.servers.start()
        data = {'Ignite cluster start time (s)': self.monotonic() - start}
        self.stage("Topology is ready")

        if nodes_to_kill > self.servers.num_nodes - 1 or coordinator and nodes_to_kill > 1:
            raise Exception("Too many nodes to kill: " + str(nodes_to_kill))

        if coordinator:
            node_chooser = lambda nodes: \
                next(node for node in nodes if node.discovery_info().node_id == nodes[0].discovery_info().coordinator)
        else:
            node_chooser = lambda nodes: \
                random.sample([n for n in self.servers.nodes if n.discovery_info().node_id !=
                               self.servers.nodes[0].discovery_info().coordinator], nodes_to_kill)

        failed_nodes, survived_node = self.choose_node_to_kill(self.servers.nodes, node_chooser)

        data["nodes"] = [node.node_id() for node in self.servers.nodes]

        disco_infos = []
        for node in self.servers.nodes:
            disco_info = node.discovery_info()
            disco_infos.append({
                "id": disco_info.node_id,
                "consistent_id": disco_info.consistent_id,
                "coordinator": disco_info.coordinator,
                "order": disco_info.order,
                "int_order": disco_info.int_order,
                "is_client": disco_info.is_client
            })

        data["node_disco_info"] = disco_infos

        start = self.monotonic()

        ids_to_wait = [node.discovery_info().node_id for node in failed_nodes]

        self.servers.stop_nodes_async(failed_nodes, clean_shutdown=False)

        for failed_id in ids_to_wait:
            self.stage("Waiting for stopping " + failed_id)

            self.servers.await_event_on_node("Node FAILED: TcpDiscoveryNode \\[id=" + failed_id, survived_node, 60,
                                             from_the_beginning=True)

        data['Node(s) failure detected in time (s)'] = self.monotonic() - start
        data['Nodes failed'] = len(failed_nodes)

        return data

    @staticmethod
    def choose_node_to_kill(nodes, chooser):
        """
        :param nodes: the node set.
        :param chooser: chooser of node(s) to stop.
        :return: Tuple of nodes to stop and survived nodes.
        """
        to_kill = chooser(nodes)

        to_kill = [to_kill] if not isinstance(to_kill, list) else to_kill

        survive = random.choice([node for node in nodes if node not in to_kill])

        return to_kill, survive
