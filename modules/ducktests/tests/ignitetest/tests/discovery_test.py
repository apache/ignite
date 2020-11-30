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
import sys
from enum import IntEnum
from time import monotonic
from typing import NamedTuple

from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteAwareService, IgniteService, get_event_time, node_failed_event_pattern
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_zookeeper_cluster, from_ignite_cluster, \
    TcpDiscoverySpi
from ignitetest.services.utils.time_utils import epoch_mills
from ignitetest.services.zk.zookeeper import ZookeeperService, ZookeeperSettings
from ignitetest.utils import ignite_versions, version_if, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, V_2_8_0, IgniteVersion
from ignitetest.utils.enum import constructible


@constructible
class ClusterLoad(IntEnum):
    """
    Type of cluster loading.
    """
    NONE = 0
    ATOMIC = 1
    TRANSACTIONAL = 2


# pylint: disable=R0913
class DiscoveryTestConfig(NamedTuple):
    """
    Configuration for DiscoveryTest.
    """
    version: IgniteVersion
    nodes_to_kill: int = 1
    load_type: ClusterLoad = ClusterLoad.NONE
    sequential_failure: bool = False
    with_zk: bool = False
    failure_detection_timeout: int = 1000


# pylint: disable=W0223, no-member
class DiscoveryTest(IgniteTest):
    """
    Test various node failure scenarios (TCP and ZooKeeper).
    1. Start of ignite cluster.
    2. Kill random node.
    3. Wait that survived node detects node failure.
    """
    MAX_CONTAINERS = 12

    ZOOKEEPER_NODES = 3

    DATA_AMOUNT = 5_000_000

    WARMUP_DATA_AMOUNT = 10_000

    FAILURE_TIMEOUT = 800

    def __init__(self, test_context):
        super().__init__(test_context=test_context)

        self.netfilter_store_path = None

    @cluster(num_nodes=MAX_CONTAINERS)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(nodes_to_kill=[1, 2], failure_detection_timeout=[FAILURE_TIMEOUT],
            load_type=[ClusterLoad.NONE, ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL])
    def test_nodes_fail_not_sequential_tcp(self, ignite_version, nodes_to_kill, load_type, failure_detection_timeout):
        """
        Test nodes failure scenario with TcpDiscoverySpi not allowing nodes to fail in a row.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=nodes_to_kill,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=False,
                                          failure_detection_timeout=failure_detection_timeout)

        return self._perform_node_fail_scenario(test_config)

    @cluster(num_nodes=MAX_CONTAINERS)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(load_type=[ClusterLoad.NONE, ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL],
            failure_detection_timeout=[FAILURE_TIMEOUT])
    def test_2_nodes_fail_sequential_tcp(self, ignite_version, load_type, failure_detection_timeout):
        """
        Test 2 nodes sequential failure scenario with TcpDiscoverySpi.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=2,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=True,
                                          failure_detection_timeout=failure_detection_timeout)

        return self._perform_node_fail_scenario(test_config)

    @cluster(num_nodes=MAX_CONTAINERS)
    @version_if(lambda version: version != V_2_8_0)  # ignite-zookeeper package is broken in 2.8.0
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(nodes_to_kill=[1, 2], failure_detection_timeout=[FAILURE_TIMEOUT],
            load_type=[ClusterLoad.NONE, ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL])
    def test_nodes_fail_not_sequential_zk(self, ignite_version, nodes_to_kill, load_type, failure_detection_timeout):
        """
        Test node failure scenario with ZooKeeperSpi not allowing nodes to fail in a row.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=nodes_to_kill,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=False,
                                          with_zk=True, failure_detection_timeout=failure_detection_timeout)

        return self._perform_node_fail_scenario(test_config)

    @cluster(num_nodes=MAX_CONTAINERS)
    @version_if(lambda version: version != V_2_8_0)  # ignite-zookeeper package is broken in 2.8.0
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(load_type=[ClusterLoad.NONE, ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL],
            failure_detection_timeout=[FAILURE_TIMEOUT])
    def test_2_nodes_fail_sequential_zk(self, ignite_version, load_type, failure_detection_timeout):
        """
        Test node failure scenario with ZooKeeperSpi not allowing to fail nodes in a row.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=2,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=True,
                                          with_zk=True, failure_detection_timeout=failure_detection_timeout)

        return self._perform_node_fail_scenario(test_config)

    def _perform_node_fail_scenario(self, test_config):
        max_containers = len(self.test_context.cluster)

        # One node is required to detect the failure.
        assert max_containers >= 1 + test_config.nodes_to_kill + (
            DiscoveryTest.ZOOKEEPER_NODES if test_config.with_zk else 0) + (
                   0 if test_config.load_type == ClusterLoad.NONE else 1), "Few required containers: " + \
                                                                           str(max_containers) + ". Check the params."

        self.logger.info("Starting on " + str(max_containers) + " maximal containers.")

        results = {}

        modules = ['zookeeper'] if test_config.with_zk else None

        if test_config.with_zk:
            zk_quorum = start_zookeeper(self.test_context, DiscoveryTest.ZOOKEEPER_NODES, test_config)

            discovery_spi = from_zookeeper_cluster(zk_quorum)
        else:
            discovery_spi = TcpDiscoverySpi()

        ignite_config = IgniteConfiguration(
            version=test_config.version,
            discovery_spi=discovery_spi,
            failure_detection_timeout=test_config.failure_detection_timeout,
            caches=[CacheConfiguration(
                name='test-cache',
                backups=1,
                atomicity_mode='TRANSACTIONAL' if test_config.load_type == ClusterLoad.TRANSACTIONAL else 'ATOMIC'
            )]
        )

        # Start Ignite nodes in count less than max_nodes_in_use. One node is erequired for the loader. Some nodes might
        # be needed for ZooKeeper.
        servers, start_servers_sec = start_servers(
            self.test_context, max_containers - DiscoveryTest.ZOOKEEPER_NODES - 1, ignite_config, modules)

        results['Ignite cluster start time (s)'] = start_servers_sec

        failed_nodes = choose_node_to_kill(servers, test_config.nodes_to_kill, test_config.sequential_failure)

        if test_config.load_type is not ClusterLoad.NONE:
            load_config = ignite_config._replace(client_mode=True) if test_config.with_zk else \
                ignite_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(servers))

            tran_nodes = [node_id(n) for n in failed_nodes] \
                if test_config.load_type == ClusterLoad.TRANSACTIONAL else None

            params = {"cacheName": "test-cache",
                      "range": self.DATA_AMOUNT,
                      "warmUpRange": self.WARMUP_DATA_AMOUNT,
                      "targetNodes": tran_nodes,
                      "transactional": bool(tran_nodes)}

            start_load_app(self.test_context, ignite_config=load_config, params=params, modules=modules)

        results.update(self._simulate_and_detect_failure(servers, failed_nodes,
                                                         test_config.failure_detection_timeout * 4))

        return results

    def _simulate_and_detect_failure(self, servers, failed_nodes, timeout):
        """
        Perform node failure scenario
        """
        for node in failed_nodes:
            self.logger.info(
                "Simulating failure of node '%s' (order %d) on '%s'" % (node_id(node), order(node), node.name))

        ids_to_wait = [node_id(n) for n in failed_nodes]

        _, first_terminated = servers.drop_network(failed_nodes)

        # Keeps dates of logged node failures.
        logged_timestamps = []
        data = {}

        start = monotonic()

        for survivor in [n for n in servers.nodes if n not in failed_nodes]:
            for failed_id in ids_to_wait:
                logged_timestamps.append(get_event_time(servers, survivor, node_failed_event_pattern(failed_id),
                                                        timeout=start + timeout - monotonic()))

            self._check_failed_number(failed_nodes, survivor)

        self._check_not_segmented(failed_nodes)

        logged_timestamps.sort(reverse=True)

        data['Detection of node(s) failure (ms)'] = epoch_mills(logged_timestamps[0]) - epoch_mills(first_terminated)
        data['All detection delays (ms):'] = str(
            [epoch_mills(ts) - epoch_mills(first_terminated) for ts in logged_timestamps])
        data['Nodes failed'] = len(failed_nodes)

        return data

    def _check_failed_number(self, failed_nodes, survived_node):
        """Ensures number of failed nodes is correct."""
        cmd = "grep '%s' %s | wc -l" % (node_failed_event_pattern(), IgniteAwareService.CONSOLE_LOG)

        failed_cnt = int(str(survived_node.account.ssh_client.exec_command(cmd)[1].read(), sys.getdefaultencoding()))

        if failed_cnt != len(failed_nodes):
            failed = str(survived_node.account.ssh_client.exec_command(
                "grep '%s' %s" % (node_failed_event_pattern(), IgniteAwareService.CONSOLE_LOG))[1].read(),
                         sys.getdefaultencoding())

            self.logger.warn("Node '%s' (%s) has detected the following failures:%s%s" % (
                survived_node.name, node_id(survived_node), os.linesep, failed))

            raise AssertionError(
                "Wrong number of failed nodes: %d. Expected: %d. Check the logs." % (failed_cnt, len(failed_nodes)))

    def _check_not_segmented(self, failed_nodes):
        """Ensures only target nodes failed"""
        for service in [srv for srv in self.test_context.services if isinstance(srv, IgniteAwareService)]:
            for node in [srv_node for srv_node in service.nodes if srv_node not in failed_nodes]:
                cmd = "grep -i '%s' %s | wc -l" % ("local node segmented", IgniteAwareService.CONSOLE_LOG)

                failed = str(node.account.ssh_client.exec_command(cmd)[1].read(), sys.getdefaultencoding())

                if int(failed) > 0:
                    raise AssertionError(
                        "Wrong node failed (segmented) on '%s'. Check the logs." % node.name)


def start_zookeeper(test_context, num_nodes, test_config):
    """
    Start zookeeper cluster.
    """
    zk_settings = ZookeeperSettings(min_session_timeout=test_config.failure_detection_timeout,
                                    tick_time=test_config.failure_detection_timeout // 3)

    zk_quorum = ZookeeperService(test_context, num_nodes, settings=zk_settings)
    zk_quorum.start()
    return zk_quorum


def start_servers(test_context, num_nodes, ignite_config, modules=None):
    """
    Start ignite servers.
    """
    servers = IgniteService(test_context, config=ignite_config, num_nodes=num_nodes, modules=modules,
                            # mute spam in log.
                            jvm_opts=["-DIGNITE_DUMP_THREADS_ON_FAILURE=false"])

    start = monotonic()
    servers.start(timeout_sec=100)
    return servers, round(monotonic() - start, 1)


def start_load_app(test_context, ignite_config, params, modules=None):
    """
    Start loader application.
    """
    IgniteApplicationService(
        test_context,
        config=ignite_config,
        java_class_name="org.apache.ignite.internal.ducktest.tests.ContinuousDataLoadApplication",
        modules=modules,
        # mute spam in log.
        jvm_opts=["-DIGNITE_DUMP_THREADS_ON_FAILURE=false"],
        params=params).start()


def choose_node_to_kill(servers, nodes_to_kill, sequential):
    """Choose node to kill during test"""
    assert nodes_to_kill > 0, "No nodes to kill passed. Check the parameters."

    idx = random.randint(0, len(servers.nodes)-1)

    to_kill = servers.nodes[idx:] + servers.nodes[:idx-1]

    if not sequential:
        to_kill = to_kill[0::2]

    idx = random.randint(0, len(to_kill) - nodes_to_kill)
    to_kill = to_kill[idx:idx + nodes_to_kill]

    assert len(to_kill) == nodes_to_kill, "Unable to pick up required number of nodes to kill."

    return to_kill


def order(node):
    """Return discovery order of the node."""
    return node.discovery_info().order


def node_id(node):
    """Return node id."""
    return node.discovery_info().node_id
