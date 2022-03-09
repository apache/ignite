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
from enum import IntEnum
from time import monotonic
from typing import NamedTuple

from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteAwareService, IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_aware import node_failed_event_pattern
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_zookeeper_cluster, from_ignite_cluster, \
    TcpDiscoverySpi
from ignitetest.services.utils.time_utils import epoch_mills
from ignitetest.services.zk.zookeeper import ZookeeperService, ZookeeperSettings
from ignitetest.utils import ignite_versions, ignore_if, cluster
from ignitetest.utils.enum import constructible
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, LATEST_2_7, V_2_8_0, V_2_9_0, IgniteVersion


@constructible
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
    load_type: ClusterLoad = ClusterLoad.NONE
    sequential_failure: bool = False
    with_zk: bool = False
    disable_conn_recovery: bool = False
    net_part: IgniteAwareService.NetPart = IgniteService.NetPart.ALL


class DiscoveryTest(IgniteTest):
    """
    Test various node failure scenarios (TCP and ZooKeeper).
    1. Start of ignite cluster.
    2. Kill random node.
    3. Wait that survived node detects node failure.
    """

    # Name of global param to control failure_detection_timeout used.
    GLOBAL_DETECTION_TIMEOUT = "failure_detection_timeout"

    # Default maximum numbers of containers to utilize for the test.
    MAX_CONTAINERS = 12

    ZOOKEEPER_NODES = 3

    DATA_AMOUNT = 5_000_000

    WARMUP_DATA_AMOUNT = 10_000

    DEFAULT_DETECTION_TIMEOUT = 2000

    def __init__(self, test_context):
        super().__init__(test_context=test_context)

        self.netfilter_store_path = None

    @cluster(num_nodes=MAX_CONTAINERS)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(nodes_to_kill=[1, 2], net_part=[IgniteService.NetPart.ALL, IgniteService.NetPart.INPUT],
            load_type=[ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL])
    def test_nonsequential_failure_tcp(self, ignite_version, nodes_to_kill, load_type, net_part: IgniteService.NetPart):
        """
        Test nodes failure scenario with TcpDiscoverySpi not allowing nodes to fail in a row.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=nodes_to_kill,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=False,
                                          net_part=net_part)

        return self._perform_node_fail_scenario(test_config)

    @cluster(num_nodes=MAX_CONTAINERS)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(nodes_to_kill=[1, 2], load_type=[ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL])
    def test_nonsequential_failure_tcp_no_recovery(self, ignite_version, nodes_to_kill, load_type):
        """
        Test nodes failure scenario with TcpDiscoverySpi not allowing nodes to fail in a row with. The connection
        recovery is disabled.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=nodes_to_kill,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=False,
                                          disable_conn_recovery=True)

        return self._perform_node_fail_scenario(test_config)

    @cluster(num_nodes=MAX_CONTAINERS)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(load_type=[ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL],
            net_part=[IgniteService.NetPart.ALL, IgniteService.NetPart.INPUT])
    def test_sequential_failure_tcp(self, ignite_version, load_type, net_part: IgniteService.NetPart):
        """
        Test 2 nodes sequential failure scenario with TcpDiscoverySpi.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=2,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=True,
                                          net_part=net_part)

        return self._perform_node_fail_scenario(test_config)

    @cluster(num_nodes=MAX_CONTAINERS)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(load_type=[ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL])
    def test_sequential_failure_tcp_no_recovery(self, ignite_version, load_type):
        """
        Test 2 nodes sequential failure scenario with TcpDiscoverySpi. The connection recovery is disabled.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=2,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=True,
                                          disable_conn_recovery=True)

        return self._perform_node_fail_scenario(test_config)

    @cluster(num_nodes=MAX_CONTAINERS)
    @ignore_if(lambda version, globals: version == V_2_8_0)  # ignite-zookeeper package is broken in 2.8.0
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(nodes_to_kill=[1, 2], load_type=[ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL])
    def test_nodes_fail_not_sequential_zk(self, ignite_version, nodes_to_kill, load_type):
        """
        Test node failure scenario with ZooKeeperSpi not allowing nodes to fail in a row.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=nodes_to_kill,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=False,
                                          with_zk=True)

        return self._perform_node_fail_scenario(test_config)

    @cluster(num_nodes=MAX_CONTAINERS)
    @ignore_if(lambda version, globals: version == V_2_8_0)  # ignite-zookeeper package is broken in 2.8.0
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(load_type=[ClusterLoad.ATOMIC, ClusterLoad.TRANSACTIONAL])
    def test_2_nodes_fail_sequential_zk(self, ignite_version, load_type):
        """
        Test node failure scenario with ZooKeeperSpi not allowing to fail nodes in a row.
        """
        test_config = DiscoveryTestConfig(version=IgniteVersion(ignite_version), nodes_to_kill=2, with_zk=True,
                                          load_type=ClusterLoad.construct_from(load_type), sequential_failure=True)

        return self._perform_node_fail_scenario(test_config)

    def _perform_node_fail_scenario(self, test_config):
        failure_detection_timeout = self._global_int(self.GLOBAL_DETECTION_TIMEOUT, self.DEFAULT_DETECTION_TIMEOUT)

        cluster_size = self.available_cluster_size

        # One node is required to detect the failure.
        assert cluster_size >= 1 + test_config.nodes_to_kill + (
            self.ZOOKEEPER_NODES if test_config.with_zk else 0), \
            f"Few required containers: {cluster_size}. Check the params."

        self.logger.info("Starting on " + str(cluster_size) + " maximal containers.")
        self.logger.info(f"{self.GLOBAL_DETECTION_TIMEOUT}: {failure_detection_timeout}")

        results = {}

        modules = ['zookeeper'] if test_config.with_zk else None

        if test_config.with_zk:
            zk_quorum = start_zookeeper(self.test_context, self.ZOOKEEPER_NODES, failure_detection_timeout)

            discovery_spi = from_zookeeper_cluster(zk_quorum)
        else:
            discovery_spi = TcpDiscoverySpi()

            if LATEST_2_7 < test_config.version <= V_2_9_0:
                discovery_spi.so_linger = 0

            if test_config.disable_conn_recovery:
                discovery_spi.conn_recovery_timeout = 0

        ignite_config = IgniteConfiguration(
            version=test_config.version,
            discovery_spi=discovery_spi,
            failure_detection_timeout=failure_detection_timeout,
            caches=[CacheConfiguration(
                name='test-cache',
                backups=1,
                atomicity_mode='TRANSACTIONAL' if test_config.load_type == ClusterLoad.TRANSACTIONAL else 'ATOMIC'
            )]
        )

        # Start Ignite nodes in count less than max_nodes_in_use. One node is erequired for the loader. Some nodes might
        # be needed for ZooKeeper.
        servers, start_servers_sec = start_servers(
            self.test_context, cluster_size - self.ZOOKEEPER_NODES - 1,
            ignite_config, modules)

        results['Ignite cluster start time (s)'] = start_servers_sec

        failed_nodes = choose_node_to_kill(servers, test_config.nodes_to_kill, test_config.sequential_failure)

        if test_config.load_type is not ClusterLoad.NONE:
            load_config = ignite_config._replace(client_mode=True) if test_config.with_zk else \
                ignite_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(servers))

            tran_nodes = [servers.node_id(n) for n in failed_nodes] \
                if test_config.load_type == ClusterLoad.TRANSACTIONAL else None

            params = {"cacheName": "test-cache",
                      "range": self.DATA_AMOUNT,
                      "warmUpRange": self.WARMUP_DATA_AMOUNT,
                      "targetNodes": tran_nodes,
                      "transactional": bool(tran_nodes)}

            start_load_app(self.test_context, load_config, params, modules)

        # Detection timeout is 4 * failure_detection_timeout in seconds.
        detection_timeout_sec = 4 * ignite_config.failure_detection_timeout // 1000

        results.update(self._simulate_and_detect_failure(servers, failed_nodes, detection_timeout_sec,
                                                         test_config.net_part))

        return results

    def _simulate_and_detect_failure(self, servers, failed_nodes, event_timeout_sec,
                                     net_part: IgniteAwareService.NetPart):
        """
        Perform node failure scenario
        """
        ids_to_wait = []

        for node in failed_nodes:
            ids_to_wait.append(servers.node_id(node))

            self.logger.info("Simulating failure of node '%s' (ID: %s)." % (node.name, ids_to_wait[-1]))

        _, first_terminated = servers.drop_network(failed_nodes, net_part=net_part)

        # Keeps dates of logged node failures.
        logged_timestamps = []
        data = {}

        for survivor in [n for n in servers.nodes if n not in failed_nodes]:
            for failed_id in ids_to_wait:
                logged_timestamps.append(servers.get_event_time_on_node(survivor, node_failed_event_pattern(failed_id),
                                                                        timeout=event_timeout_sec))

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
        cmd = "grep '%s' %s | wc -l" % (node_failed_event_pattern(), survived_node.log_file)

        failed_cnt = int(IgniteAwareService.exec_command(survived_node, cmd))

        # Cache survivor id, do not read each time.
        surv_id = IgniteApplicationService.node_id(survived_node)

        if failed_cnt != len(failed_nodes):
            failed = IgniteAwareService.exec_command(survived_node, "grep '%s' %s" % (node_failed_event_pattern(),
                                                                                      survived_node.log_file))

            self.logger.warn("Node '%s' (%s) has detected the following failures:%s%s" %
                             (survived_node.name, surv_id, os.linesep, failed))

            raise AssertionError(
                "Wrong number of failed nodes: %d. Expected: %d. Check the logs." % (failed_cnt, len(failed_nodes)))

    def _check_not_segmented(self, failed_nodes):
        """Ensures only target nodes failed"""
        for service in [srv for srv in self.test_context.services if isinstance(srv, IgniteAwareService)]:
            for node in [srv_node for srv_node in service.nodes if srv_node not in failed_nodes]:
                cmd = "grep -i '%s' %s | wc -l" % ("local node segmented", node.log_file)

                failed = IgniteAwareService.exec_command(node, cmd)

                if int(failed) > 0:
                    raise AssertionError(
                        "Wrong node failed (segmented) on '%s'. Check the logs." % node.name)


def start_zookeeper(test_context, num_nodes, failure_detection_timeout):
    """
    Start zookeeper cluster.
    """
    zk_settings = ZookeeperSettings(min_session_timeout=failure_detection_timeout)

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
    servers.start()
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

    idx = random.randint(0, len(servers.nodes) - 1)

    to_kill = servers.nodes[idx:] + servers.nodes[:idx - 1]

    if not sequential:
        to_kill = to_kill[0::2]

    idx = random.randint(0, len(to_kill) - nodes_to_kill)
    to_kill = to_kill[idx:idx + nodes_to_kill]

    assert len(to_kill) == nodes_to_kill, "Unable to pick up required number of nodes to kill."

    return to_kill
