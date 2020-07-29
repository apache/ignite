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
        <property name="clientMode" value="{{ client_mode | lower }}"/>
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
    def properties(client_mode="false", zookeeper_settings=None):
        """
        :param client_mode: If True, renders client configuration.
        :param zookeeper_settings: ZookeperDiscoverySpi settings. If None, TcpDiscoverySpi will be used.
        :return: Rendered node's properties.
        """
        return Template(DiscoveryTest.CONFIG_TEMPLATE) \
            .render(client_mode=client_mode, zookeeper_settings=zookeeper_settings)

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
    def test_tcp(self, version):
        """
        Test basic discovery scenario with TcpDiscoverySpi.
        """
        return self.__basic_test__(version, False)

    @cluster(num_nodes=NUM_NODES + 3)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_7))
    def test_zk(self, version):
        """
        Test basic discovery scenario with ZookeeperDiscoverySpi.
        """
        return self.__basic_test__(version, True)

    def __basic_test__(self, version, with_zk=False):
        if with_zk:
            self.zk_quorum = ZookeeperService(self.test_context, 3)
            self.stage("Starting Zookeper quorum")
            self.zk_quorum.start()
            properties = self.properties(zookeeper_settings={'connection_string': self.zk_quorum.connection_string()})
            self.stage("Zookeper quorum started")
        else:
            properties = self.properties()

        self.servers = IgniteService(
            self.test_context,
            num_nodes=self.NUM_NODES,
            properties=properties,
            version=version)

        self.stage("Starting ignite cluster")

        start = self.monotonic()
        self.servers.start()
        data = {'Ignite cluster start time (s)': self.monotonic() - start}
        self.stage("Topology is ready")

        # Node failure detection
        fail_node, survived_node = self.choose_random_node_to_kill(self.servers)

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

        self.servers.stop_node(fail_node, clean_shutdown=False)

        start = self.monotonic()
        self.servers.await_event_on_node("Node FAILED", random.choice(survived_node), 60, True)

        data['Failure of node detected in time (s)'] = self.monotonic() - start

        return data

    @staticmethod
    def choose_random_node_to_kill(service):
        """
        :param service: Service nodes to process.
        :return: Tuple of random node to kill and survived nodes
        """
        idx = random.randint(0, len(service.nodes) - 1)

        survive = [node for i, node in enumerate(service.nodes) if i != idx]
        kill = service.nodes[idx]

        return kill, survive
