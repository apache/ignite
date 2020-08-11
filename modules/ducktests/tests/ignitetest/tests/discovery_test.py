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

import os
import random
import re
from datetime import datetime

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from jinja2 import Template

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.time_utils import epoch_mills
from ignitetest.services.zk.zookeeper import ZookeeperService
from ignitetest.tests.utils.ignite_test import IgniteTest
from ignitetest.tests.utils.version import DEV_BRANCH, LATEST_2_7


# pylint: disable=W0223
class DiscoveryTest(IgniteTest):
    """
    Test various node failure scenarios (TCP and ZooKeeper).
    1. Start of ignite cluster.
    2. Kill random node.
    3. Wait that survived node detects node failure.
    """
    NUM_NODES = 7

    FAILURE_DETECTION_TIMEOUT = 2000

    CONFIG_TEMPLATE = """
    <property name="failureDetectionTimeout" value="{{ failure_detection_timeout }}"/>
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
        :param zookeeper_settings: ZooKeeperDiscoverySpi settings. If None, TcpDiscoverySpi will be used.
        :return: Rendered node's properties.
        """
        return Template(DiscoveryTest.CONFIG_TEMPLATE) \
            .render(failure_detection_timeout=DiscoveryTest.FAILURE_DETECTION_TIMEOUT,
                    zookeeper_settings=zookeeper_settings)

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
        data = {'Ignite cluster start time (s)': round(self.monotonic() - start, 1)}
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

        ids_to_wait = [node.discovery_info().node_id for node in failed_nodes]

        self.stage("Stopping " + str(len(failed_nodes)) + " nodes.")

        first_terminated = self.servers.stop_nodes_async(failed_nodes, clean_shutdown=False, wait_for_stop=False)

        self.stage("Waiting for failure detection of " + str(len(failed_nodes)) + " nodes.")

        # Keeps dates of logged node failures.
        last_failure_detected = 0
        logged_timestamps = []

        for failed_id in ids_to_wait:
            pattern = "Node FAILED: " + ("ZookeeperClusterNode" if with_zk else "TcpDiscoveryNode") + " \\[id=" \
                      + failed_id

            self.servers.await_event_on_node(pattern, survived_node, 10, from_the_beginning=True, backoff_sec=0.01)

            last_failure_detected = self.monotonic()

            self.stage("Failure detection measured.")

        for failed_id in ids_to_wait:
            pattern = "Node FAILED: " + ("ZookeeperClusterNode" if with_zk else "TcpDiscoveryNode") + " \\[id=" \
                  + failed_id

            _, stdout, _ = survived_node.account.ssh_client.exec_command("grep '%s' %s" % (pattern, os.path.join(
                IgniteAwareService.PERSISTENT_ROOT, "console.log")))

            logged_timestamps.append(
                datetime.strptime(re.match("^\\[[^\\[]+\\]", stdout.read()).group(), "[%Y-%m-%d %H:%M:%S,%f]"))

        logged_timestamps.sort(reverse=True)

        measured = int((last_failure_detected - first_terminated[0]) * 1000)
        by_log = epoch_mills(logged_timestamps[0]) - epoch_mills(first_terminated[1])

        assert by_log > 0, "Negative node failure detection delay: " + by_log + ". Probably it is a timezone issue."
        assert by_log <= measured, "Value of node failure detection delay taken from by the node log (" + \
                                   str(by_log) + "ms) must be lesser than measured value (" + str(measured) + "ms) " + \
                                   "because watching this event consumes extra time."

        data['Detection of node(s) failure, measured (ms)'] = measured
        data['Detection of node(s) failure, by the log (ms)'] = by_log
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
