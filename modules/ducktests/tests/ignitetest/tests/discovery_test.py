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
import sys
from enum import IntEnum
from datetime import datetime
from time import monotonic
from typing import NamedTuple

from ducktape.mark import matrix
from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteAwareService, IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.communication import TcpCommunicationSpi
from ignitetest.services.utils.ignite_configuration.discovery import from_zookeeper_cluster, from_ignite_cluster, \
    TcpDiscoverySpi
from ignitetest.services.utils.time_utils import epoch_mills
from ignitetest.services.zk.zookeeper import ZookeeperService
from ignitetest.utils import ignite_versions, version_if
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST_2_8, V_2_8_0, IgniteVersion


class ClusterLoad(IntEnum):
    """
    Type of cluster loading.
    """
    NONE = 0
    ATOMIC = 1
    TRANSACTIONAL = 2


class DiscoveryTestConfig(NamedTuple):
    """
    Configuration for DiscoveryTest.
    """
    version: IgniteVersion
    nodes_to_kill: int = 1
    kill_coordinator: bool = False
    load_type: ClusterLoad = ClusterLoad.NONE
    with_zk: bool = False


class DiscoveryTest(IgniteTest):
    """
    Test various node failure scenarios (TCP and ZooKeeper).
    1. Start of ignite cluster.
    2. Kill random node.
    3. Wait that survived node detects node failure.
    """
    NUM_NODES = 7

    TCP_PORT_RANGE = 0

    FAILURE_DETECTION_TIMEOUT = 1000

    DATA_AMOUNT = 5_000_000

    WARMUP_DATA_AMOUNT = 10_000

    NETFILTER_SAVED_SETTINGS = os.path.join(IgniteTest.TEMP_PATH_ROOT, "discovery_test", "netfilter.bak")

    def __init__(self, test_context):
        super().__init__(test_context=test_context)

        self.__netfilter_settings = {}

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(kill_coordinator=[False, True],
            nodes_to_kill=[1, 2],
            load_type=[ClusterLoad.NONE, ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL])
    def test_node_fail_tcp(self, ignite_version, kill_coordinator, nodes_to_kill, load_type):
        """
        Test nodes failure scenario with TcpDiscoverySpi. """

        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), kill_coordinator=kill_coordinator,
                                          nodes_to_kill=nodes_to_kill, load_type=load_type, with_zk=False)

        return self._perform_node_fail_scenario(test_config)

    @cluster(num_nodes=NUM_NODES + 3)
    @version_if(lambda version: version != V_2_8_0)  # ignite-zookeeper package is broken in 2.8.0
    @ignite_versions(str(DEV_BRANCH), str(LATEST_2_8))
    @matrix(kill_coordinator=[False, True],
            nodes_to_kill=[1, 2],
            load_type=[ClusterLoad.NONE, ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL])
    def test_node_fail_zk(self, ignite_version, kill_coordinator, nodes_to_kill, load_type):
        """
        Test node failure scenario with ZooKeeperSpi.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), kill_coordinator=kill_coordinator,
                                          nodes_to_kill=nodes_to_kill, load_type=load_type, with_zk=True)

        return self._perform_node_fail_scenario(test_config)

    def _perform_node_fail_scenario(self, test_config):
        modules = ['zookeeper'] if test_config.with_zk else None

        if test_config.with_zk:
            zk_quorum = start_zookeeper(self.test_context, 3)

            discovery_spi = from_zookeeper_cluster(zk_quorum)
        else:
            discovery_spi = TcpDiscoverySpi(port_range=self.TCP_PORT_RANGE)

        ignite_config = IgniteConfiguration(
            version=test_config.version,
            discovery_spi=discovery_spi,
            communication_spi=TcpCommunicationSpi(port_range=self.TCP_PORT_RANGE),
            failure_detection_timeout=self.FAILURE_DETECTION_TIMEOUT,
            caches=[CacheConfiguration(name='test-cache', backups=1, atomicity_mode='TRANSACTIONAL'
                    if test_config.load_type == ClusterLoad.TRANSACTIONAL else 'ATOMIC')]
        )

        servers, start_servers_sec = start_servers(self.test_context, self.NUM_NODES - 1, ignite_config, modules)

        failed_nodes, survived_node = choose_node_to_kill(servers, test_config.kill_coordinator,
                                                          test_config.nodes_to_kill)

        if test_config.load_type is not ClusterLoad.NONE:
            load_config = ignite_config._replace(client_mode=True) if test_config.with_zk else \
                ignite_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(servers))

            tran_nodes = [n.discovery_info().node_id for n in failed_nodes] \
                if test_config.load_type == ClusterLoad.TRANSACTIONAL else None

            params = {"cacheName": "test-cache",
                      "range": self.DATA_AMOUNT,
                      "warmUpRange": self.WARMUP_DATA_AMOUNT,
                      "targetNodes": tran_nodes,
                      "transactional": bool(tran_nodes)}

            start_load_app(self.test_context, ignite_config=load_config, params=params, modules=modules)

        for node in failed_nodes:
            di = node.discovery_info()
            self.logger.info("Simulating failure of node '%s' (order %d) on '%s'" % (di.node_id, di.order, node.name))

        data = self.simulate_nodes_failure(servers, node_fail_task(ignite_config, test_config), failed_nodes,
                                           survived_node)

        data['Ignite cluster start time (s)'] = start_servers_sec

        return data

    def simulate_nodes_failure(self, servers, kill_node_task, failed_nodes, survived_node):
        """
        Perform node failure scenario
        """
        ids_to_wait = [node.discovery_info().node_id for node in failed_nodes]

        _, first_terminated = servers.exec_on_nodes_async(failed_nodes, kill_node_task)

        for node in failed_nodes:
            self.logger.debug("Netfilter activated on '%s': %s" % (node.name, dump_netfilter_settings(node)))

        # Keeps dates of logged node failures.
        logged_timestamps = []
        data = {}

        for failed_id in ids_to_wait:
            servers.await_event_on_node(failed_pattern(failed_id), survived_node, 30, from_the_beginning=True,
                                        backoff_sec=1)

            _, stdout, _ = survived_node.account.ssh_client.exec_command(
                "grep '%s' %s" % (failed_pattern(failed_id), IgniteAwareService.STDOUT_STDERR_CAPTURE))

            logged_timestamps.append(
                datetime.strptime(re.match("^\\[[^\\[]+\\]", stdout.read().decode("utf-8")).group(),
                                  "[%Y-%m-%d %H:%M:%S,%f]"))

        logged_timestamps.sort(reverse=True)

        first_kill_time = epoch_mills(first_terminated)
        detection_delay = epoch_mills(logged_timestamps[0]) - first_kill_time

        data['Detection of node(s) failure (ms)'] = detection_delay
        data['All detection delays (ms):'] = str([epoch_mills(ts) - first_kill_time for ts in logged_timestamps])
        data['Nodes failed'] = len(failed_nodes)

        return data

    def setup(self):
        IgniteTest.setup(self)

        # Store current network filter settings.

        for node in self.test_context.cluster.nodes:
            self.__netfilter_settings[node.name] = dump_netfilter_settings(node)

            path_to_store = self.NETFILTER_SAVED_SETTINGS

            node.account.ssh_client.exec_command(f"rm -drf {path_to_store} && mkdir -p $(dirname {path_to_store})")

            cmd = "sudo iptables-save | tee " + self.NETFILTER_SAVED_SETTINGS

            exec_error = str(node.account.ssh_client.exec_command(cmd)[2].read(), sys.getdefaultencoding())

            if "Warning: iptables-legacy tables present" in exec_error:
                cmd = "sudo iptables-legacy-save | tee " + self.NETFILTER_SAVED_SETTINGS

                exec_error = str(node.account.ssh_client.exec_command(cmd)[2].read(), sys.getdefaultencoding())

            assert len(exec_error) == 0, "Failed to store iptables rules on '%s': %s" % (node.name, exec_error)

            # assert len(node.account.ssh_client.exec_command("sudo iptables -F")[2].read()) == 0, \
            #     "Failed to clear iptables rules on '" + node.name

            self.logger.debug("Netfilter before launch on '%s': %s" % (node.name, self.__netfilter_settings[node.name]))

    def teardown(self):
        # Restore previous network filter settings.

        cmd = "sudo iptables-restore < " + self.NETFILTER_SAVED_SETTINGS

        errors = []

        for node in self.test_context.cluster.nodes:
            exec_error = str(node.account.ssh_client.exec_command(cmd)[2].read(), sys.getdefaultencoding())

            if len(exec_error) > 0:
                errors.append("Failed to restore iptables rules on '%s': %s" % (node.name, exec_error))
            else:
                restored_settings = dump_netfilter_settings(node)

                if restored_settings != self.__netfilter_settings[node.name]:
                    errors.append("Settings not restored for node '%s'. Restored settings: %s%s Before launch: %s" %
                                  (node.name, restored_settings, os.linesep, self.__netfilter_settings[node.name]))
                else:
                    self.logger.debug("Netfilter after launch on '%s': %s" % (node.name, restored_settings))

        if len(errors) > 0:
            self.logger.error("Failed restoring actions:" + os.linesep + os.linesep.join(errors))

            raise RuntimeError("Unable to restore node states. See the log above.")

        IgniteTest.teardown(self)


def start_zookeeper(test_context, num_nodes):
    """
    Start zookeeper cluster.
    """
    zk_quorum = ZookeeperService(test_context, num_nodes)
    zk_quorum.start()
    return zk_quorum


def start_servers(test_context, num_nodes, ignite_config, modules=None):
    """
    Start ignite servers.
    """
    servers = IgniteService(test_context, config=ignite_config, num_nodes=num_nodes, modules=modules,
                            # mute spam in log.
                            jvm_opts=["-DIGNITE_DUMP_THREADS_ON_FAILURE=false","-Djava.net.preferIPv4Stack=true"])

    start = monotonic()
    servers.start()
    return servers, round(monotonic() - start, 1)


def start_load_app(test_context, ignite_config, params, modules=None):
    """
    Start loader application.
    """
    loader = IgniteApplicationService(
        test_context,
        config=ignite_config,
        java_class_name="org.apache.ignite.internal.ducktest.tests.ContinuousDataLoadApplication",
        modules=modules,
        # mute spam in log.
        jvm_opts=["-DIGNITE_DUMP_THREADS_ON_FAILURE=false"],
        params=params)

    loader.start()


def failed_pattern(failed_node_id):
    """
    Failed node pattern in log
    """
    return "Node FAILED: .\\{1,\\}Node \\[id=" + failed_node_id


def choose_node_to_kill(servers, kill_coordinator, nodes_to_kill):
    """Choose node to kill during test"""
    assert nodes_to_kill > 0, "   No nodes to kill passed. Check the parameters."

    nodes = servers.nodes
    coordinator = nodes[0].discovery_info().coordinator
    to_kill = []

    if kill_coordinator:
        to_kill.append(next(node for node in nodes if node.discovery_info().node_id == coordinator))
        nodes_to_kill -= 1

    if nodes_to_kill > 0:
        choice = random.sample([n for n in nodes if n.discovery_info().node_id != coordinator], nodes_to_kill)
        to_kill.extend([choice] if not isinstance(choice, list) else choice)

    survive = random.choice([node for node in servers.nodes if node not in to_kill])

    return to_kill, survive


def node_fail_task(ignite_config, test_config):
    """
    Creates proper command task to simulate network failure depending on the configurations.
    """
    cm_spi = ignite_config.communication_spi
    dsc_spi = ignite_config.discovery_spi

    cm_ports = str(cm_spi.port) if cm_spi.port_range < 1 else str(cm_spi.port) + ':' + str(
        cm_spi.port + cm_spi.port_range)

    if test_config.with_zk:
        dsc_ports = str(ignite_config.discovery_spi.port)
    else:
        dsc_ports = str(dsc_spi.port) if dsc_spi.port_range < 1 else str(dsc_spi.port) + ':' + str(
            dsc_spi.port + dsc_spi.port_range)

    cmd = f"sudo iptables -A %s -p tcp -m multiport --dport {dsc_ports},{cm_ports} -j DROP"

    return lambda node: (node.account.ssh_client.exec_command(cmd % "INPUT"),
                         node.account.ssh_client.exec_command(cmd % "OUTPUT"))


def dump_netfilter_settings(node):
    """
    Reads current netfilter settings on the node for debugging purposes.
    """
    return str(node.account.ssh_client.exec_command("sudo iptables -L -n")[1].read(), sys.getdefaultencoding())
