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
import re
from datetime import datetime

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from jinja2 import Template

from ignitetest.services.ignite import IgniteAwareService
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.time_utils import epoch_mills
from ignitetest.services.zk.zookeeper import ZookeeperService
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST_2_8


# pylint: disable=W0223
class DiscoveryTest(IgniteTest):
    """
    Test various node failure scenarios (TCP and ZooKeeper).
    1. Start of ignite cluster.
    2. Kill random node.
    3. Wait that survived node detects node failure.
    """
    class Config:
        """
        Configuration for DiscoveryTest.
        """
        def __init__(self, nodes_to_kill=1, kill_coordinator=False, with_load=False):
            self.nodes_to_kill = nodes_to_kill
            self.kill_coordinator = kill_coordinator
            self.with_load = with_load

    NUM_NODES = 7

    FAILURE_DETECTION_TIMEOUT = 2000

    DATA_AMOUNT = 100000

    CONFIG_TEMPLATE = """
    <property name="failureDetectionTimeout" value="{{ failure_detection_timeout }}"/>
    {% if zookeeper_settings %}
        {% with zk = zookeeper_settings %}
        <property name="discoverySpi">
            <bean class="org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi">
                <property name="zkConnectionString" value="{{ zk.connection_string }}"/>
                <property name="zkRootPath" value="{{ zk.root_path or '/apacheIgnite' }}"/>
            </bean>
        </property>
        {% endwith %}
    {% endif %}
    """

    def __init__(self, test_context):
        super().__init__(test_context=test_context)
        self.zk_quorum = None
        self.servers = None
        self.loader = None

    @cluster(num_nodes=NUM_NODES)
    @matrix(ignite_version=[str(DEV_BRANCH), str(LATEST_2_8)],
            kill_coordinator=[False, True],
            nodes_to_kill=[1, 2],
            with_load=[False, True])
    def test_tcp(self, ignite_version, kill_coordinator, nodes_to_kill, with_load):
        """
        Test nodes failure scenario with TcpDiscoverySpi.
        """
        config = DiscoveryTest.Config(nodes_to_kill, kill_coordinator, with_load)

        return self.__simulate_nodes_failure(ignite_version, self.__properties(), None, config)

    @cluster(num_nodes=NUM_NODES + 3)
    @matrix(ignite_version=[str(DEV_BRANCH), str(LATEST_2_8)],
            kill_coordinator=[False, True],
            nodes_to_kill=[1, 2],
            with_load=[False, True])
    def test_zk(self, ignite_version, kill_coordinator, nodes_to_kill, with_load):
        """
        Test node failure scenario with ZooKeeperSpi.
        """
        config = DiscoveryTest.Config(nodes_to_kill, kill_coordinator, with_load)

        self.__start_zk_quorum()

        properties = self.__zk_properties(self.zk_quorum.connection_string())
        modules = ["zookeeper"]

        return self.__simulate_nodes_failure(ignite_version, properties, modules, config)

    def setUp(self):
        pass

    def teardown(self):
        if self.loader:
            self.loader.stop()

        if self.servers:
            self.servers.stop()

        if self.zk_quorum:
            self.zk_quorum.stop()

    def __simulate_nodes_failure(self, version, properties, modules, config):
        if config.nodes_to_kill < 1:
            return {"No nodes to kill": "Nothing to do"}

        self.servers = IgniteService(
            self.test_context,
            num_nodes=self.NUM_NODES - 1,
            modules=modules,
            properties=properties,
            version=version)

        time_holder = self.monotonic()

        self.servers.start()

        data = {'Ignite cluster start time (s)': round(self.monotonic() - time_holder, 1)}

        failed_nodes, survived_node = self.__choose_node_to_kill(config.kill_coordinator, config.nodes_to_kill)

        ids_to_wait = [node.discovery_info().node_id for node in failed_nodes]

        if config.with_load:
            self.__start_loading(version, properties, modules)

        first_terminated = self.servers.stop_nodes_async(failed_nodes, clean_shutdown=False, wait_for_stop=False)

        # Keeps dates of logged node failures.
        logged_timestamps = []

        for failed_id in ids_to_wait:
            self.servers.await_event_on_node(self.__failed_pattern(failed_id), survived_node, 20,
                                             from_the_beginning=True, backoff_sec=0.1)

            _, stdout, _ = survived_node.account.ssh_client.exec_command(
                "grep '%s' %s" % (self.__failed_pattern(failed_id), IgniteAwareService.STDOUT_STDERR_CAPTURE))

            logged_timestamps.append(
                datetime.strptime(re.match("^\\[[^\\[]+\\]", stdout.read().decode("utf-8")).group(),
                                  "[%Y-%m-%d %H:%M:%S,%f]"))

        logged_timestamps.sort(reverse=True)

        self.__store_results(data, logged_timestamps, first_terminated[1])

        data['Nodes failed'] = len(failed_nodes)

        return data

    @staticmethod
    def __store_results(data, logged_timestamps, first_kill_time):
        first_kill_time = epoch_mills(first_kill_time)

        detection_delay = epoch_mills(logged_timestamps[0]) - first_kill_time

        data['Detection of node(s) failure (ms)'] = detection_delay
        data['All detection delays (ms):'] = str([epoch_mills(ts) - first_kill_time for ts in logged_timestamps])

    @staticmethod
    def __failed_pattern(failed_node_id):
        return "Node FAILED: .\\{1,\\}Node \\[id=" + failed_node_id

    def __choose_node_to_kill(self, kill_coordinator, nodes_to_kill):
        assert nodes_to_kill > 0, "No nodes to kill passed. Check the parameters."

        nodes = self.servers.nodes
        coordinator = nodes[0].discovery_info().coordinator
        to_kill = []

        if kill_coordinator:
            to_kill.append(next(node for node in nodes if node.discovery_info().node_id == coordinator))
            nodes_to_kill -= 1

        if nodes_to_kill > 0:
            choice = random.sample([n for n in nodes if n.discovery_info().node_id != coordinator], nodes_to_kill)
            to_kill.extend([choice] if not isinstance(choice, list) else choice)

        survive = random.choice([node for node in self.servers.nodes if node not in to_kill])

        return to_kill, survive

    def __start_loading(self, ignite_version, properties, modules):
        self.loader = IgniteApplicationService(
            self.test_context,
            java_class_name="org.apache.ignite.internal.ducktest.tests.DataGenerationApplication",
            version=ignite_version,
            modules=modules,
            properties=properties,
            params={"cacheName": "test-cache", "range": self.DATA_AMOUNT, "infinite": True})

        self.loader.start()

    def __start_zk_quorum(self):
        self.zk_quorum = ZookeeperService(self.test_context, 3)

        self.zk_quorum.start()

    @staticmethod
    def __properties(zookeeper_settings=None):
        """
        :param zookeeper_settings: ZooKeeperDiscoverySpi settings. If None, TcpDiscoverySpi will be used.
        :return: Rendered node's properties.
        """
        return Template(DiscoveryTest.CONFIG_TEMPLATE) \
            .render(failure_detection_timeout=DiscoveryTest.FAILURE_DETECTION_TIMEOUT,
                    zookeeper_settings=zookeeper_settings)

    @staticmethod
    def __zk_properties(connection_string):
        return DiscoveryTest.__properties(zookeeper_settings={'connection_string': connection_string})
