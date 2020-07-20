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

import random

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.zk.zookeeper import ZookeeperService
from ignitetest.version import DEV_BRANCH, LATEST_2_7
from ignitetest.tests.utils.ignite_test import IgniteTest

from jinja2 import Template

from monotonic import monotonic


class DiscoveryTest(IgniteTest):
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
        self.zk = None
        self.servers = None

    @staticmethod
    def properties(client_mode="false", zookeeper_settings=None):
        return Template(DiscoveryTest.CONFIG_TEMPLATE) \
            .render(client_mode=client_mode, zookeeper_settings=zookeeper_settings)

    def setUp(self):
        pass

    def teardown(self):
        if self.zk is not None:
            self.zk.stop()

        if self.servers is not None:
            self.servers.stop()

    def min_cluster_size(self):
        return self.NUM_NODES + 3

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_7))
    def test_tcp(self, version):
        return self.__basic_test__(version, False)

    @cluster(num_nodes=NUM_NODES + 3)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_7))
    def test_zk(self, version):
        return self.__basic_test__(version, True)

    def __basic_test__(self, version, with_zk=False):
        if with_zk:
            self.zk = ZookeeperService(self.test_context, 3)
            self.stage("Starting Zookeper quorum")
            self.zk.start()
            properties = self.properties(zookeeper_settings={'connection_string': self.zk.connection_string()})
            self.stage("Zookeper quorum started")
        else:
            properties = self.properties()

        self.ignite = IgniteService(
            self.test_context,
            num_nodes=self.NUM_NODES,
            properties=properties,
            version=version)

        self.stage("Starting ignite cluster")

        start = monotonic()
        self.ignite.start()
        data = {'Ignite cluster start time (s)': monotonic() - start }
        self.stage("Topology is ready")

        # Node failure detection
        fail_node, survived_node = self.choose_random_node_to_kill(self.ignite)
        self.ignite.stop_node(fail_node, clean_shutdown=False)

        start = monotonic()
        self.ignite.await_event_on_node("Node FAILED", random.choice(survived_node), 60)

        data = {'Failure of node detected in time (s)': monotonic() - start}

        return data

    def choose_random_node_to_kill(self, service):
        idx = random.randint(0, len(service.nodes) - 1)

        survive = [node for i, node in enumerate(service.nodes) if i != idx]
        kill = service.nodes[idx]

        return kill, survive

