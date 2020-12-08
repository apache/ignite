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
This module contains Cellular Affinity tests.
"""
import math
from enum import IntEnum

from ducktape.mark import matrix
from jinja2 import Template

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteClientConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster, from_zookeeper_cluster, \
    TcpDiscoverySpi
from ignitetest.services.zk.zookeeper import ZookeeperSettings, ZookeeperService
from ignitetest.utils import ignite_versions, version_if, cluster
from ignitetest.utils.enum import constructible
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion, LATEST_2_8


@constructible
class StopType(IntEnum):
    """
    Node stop method type.
    """
    SIGTERM = 0
    SIGKILL = 1
    DROP_NETWORK = 2


@constructible
class DiscoreryType(IntEnum):
    """
    Discovery type.
    """
    ZooKeeper = 0
    TCP = 1


# pylint: disable=W0223
class CellularAffinity(IgniteTest):
    """
    Tests Cellular Affinity scenarios.
    """
    NODES_PER_CELL = 3
    ZOOKEPER_CLUSTER_SIZE = 3

    FAILURE_DETECTION_TIMEOUT = 500
    ZOOKEPER_SESSION_TIMEOUT = FAILURE_DETECTION_TIMEOUT

    ATTRIBUTE = "CELL"

    CACHE_NAME = "test-cache"

    PREPARED_TX_CNT = 500  # possible amount at real cluster under load (per cell).

    CONFIG_TEMPLATE = """
            <property name="cacheConfiguration">
                <list>
                    <bean class="org.apache.ignite.configuration.CacheConfiguration">
                        <property name="affinity">
                            <bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction">
                                <property name="affinityBackupFilter">
                                    <bean class="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test.CellularAffinityBackupFilter">
                                        <constructor-arg value="{{ attr }}"/>
                                    </bean>
                                </property>
                            </bean>
                        </property>
                        <property name="name" value="{{ cacheName }}"/>
                        <property name="backups" value="{{ backups }}"/>
                        <property name="atomicityMode" value="TRANSACTIONAL"/>
                    </bean>
                </list>
            </property>
        """  # noqa: E501

    @staticmethod
    def properties():
        """
        :return: Configuration properties.
        """
        return Template(CellularAffinity.CONFIG_TEMPLATE) \
            .render(
            backups=CellularAffinity.NODES_PER_CELL,  # bigger than cell capacity (to handle single cell useless test)
            attr=CellularAffinity.ATTRIBUTE,
            cacheName=CellularAffinity.CACHE_NAME)

    @cluster(num_nodes=NODES_PER_CELL * 3 + 1)
    @version_if(lambda version: version >= DEV_BRANCH)
    @ignite_versions(str(DEV_BRANCH))
    def test_distribution(self, ignite_version):
        """
        Tests Cellular Affinity scenario (partition distribution).
        """
        cell1 = self.start_cell(ignite_version, ['-D' + CellularAffinity.ATTRIBUTE + '=1'])

        discovery_spi = from_ignite_cluster(cell1)

        cell2 = self.start_cell(ignite_version, ['-D' + CellularAffinity.ATTRIBUTE + '=2'], discovery_spi)
        cell3 = self.start_cell(ignite_version, ['-D' + CellularAffinity.ATTRIBUTE + '=XXX', '-DRANDOM=42'],
                                discovery_spi)

        for cell in [cell1, cell2, cell3]:
            cell.await_started()

        ControlUtility(cell1, self.test_context).activate()

        checker = IgniteApplicationService(
            self.test_context,
            IgniteClientConfiguration(version=IgniteVersion(ignite_version), discovery_spi=from_ignite_cluster(cell1)),
            java_class_name="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test.DistributionChecker",
            params={"cacheName": CellularAffinity.CACHE_NAME,
                    "attr": CellularAffinity.ATTRIBUTE,
                    "nodesPerCell": self.NODES_PER_CELL})

        checker.run()

    # pylint: disable=R0912
    # pylint: disable=R0914
    # pylint: disable=no-member
    @cluster(num_nodes=2 * (NODES_PER_CELL + 1) + 3)  # cell_cnt * (srv_per_cell + cell_streamer) + zookeper_cluster
    @ignite_versions(str(DEV_BRANCH), str(LATEST_2_8))
    @matrix(stop_type=[StopType.DROP_NETWORK, StopType.SIGKILL, StopType.SIGTERM],
            discovery_type=[DiscoreryType.ZooKeeper, DiscoreryType.TCP])
    def test_latency(self, ignite_version, stop_type, discovery_type):
        """
        Tests Cellular switch tx latency.
        """
        cluster_size = len(self.test_context.cluster)

        cells_amount = math.floor((cluster_size - self.ZOOKEPER_CLUSTER_SIZE) / (self.NODES_PER_CELL + 1))

        assert cells_amount >= 2

        self.test_context.logger.info(
            "Cells amount calculated as %d at cluster with %d nodes in total" % (cells_amount, cluster_size))

        data = {}

        discovery_spi = None

        modules = []

        d_type = DiscoreryType.construct_from(discovery_type)

        if d_type is DiscoreryType.ZooKeeper:
            zk_settings = ZookeeperSettings(min_session_timeout=self.ZOOKEPER_SESSION_TIMEOUT)
            zk_quorum = ZookeeperService(self.test_context, self.ZOOKEPER_CLUSTER_SIZE, settings=zk_settings)
            zk_quorum.start()

            modules.append('zookeeper')

            discovery_spi = from_zookeeper_cluster(zk_quorum)

        cell0, prepared_tx_loader1 = self.start_cell_with_prepared_txs(ignite_version, "C0", discovery_spi, modules)

        if d_type is DiscoreryType.TCP:
            discovery_spi = from_ignite_cluster(cell0)

        assert discovery_spi is not None

        loaders = [prepared_tx_loader1]
        nodes = [cell0]

        for cell in range(1, cells_amount):
            node, prepared_tx_loader = \
                self.start_cell_with_prepared_txs(ignite_version, "C%d" % cell, discovery_spi, modules)

            loaders.append(prepared_tx_loader)
            nodes.append(node)

        failed_loader = loaders[1]

        for node in [*nodes, *loaders]:
            node.await_started()

        streamers = []

        for cell in range(0, cells_amount):
            streamers.append(self.start_tx_streamer(ignite_version, "C%d" % cell, discovery_spi, modules))

        for streamer in streamers:  # starts tx streaming with latency record (with some warmup).
            streamer.start_async()

        for streamer in streamers:
            streamer.await_started()

        ControlUtility(cell0, self.test_context).disable_baseline_auto_adjust()  # baseline set.
        ControlUtility(cell0, self.test_context).activate()

        for loader in loaders:
            loader.await_event("ALL_TRANSACTIONS_PREPARED", 180, from_the_beginning=True)

        for streamer in streamers:
            streamer.await_event("WARMUP_FINISHED", 180, from_the_beginning=True)

        # node left with prepared txs.
        with StopType.construct_from(stop_type) as s_type:
            if s_type is StopType.SIGTERM:
                failed_loader.stop_async()
            elif s_type is StopType.SIGKILL:
                failed_loader.kill()
            elif s_type is StopType.DROP_NETWORK:
                failed_loader.drop_network()

        for streamer in streamers:
            streamer.await_event("Node left topology\\|Node FAILED", 60, from_the_beginning=True)

        for streamer in streamers:  # just an assertion that we have PME-free switch.
            streamer.await_event("exchangeFreeSwitch=true", 60, from_the_beginning=True)

        for streamer in streamers:  # waiting for streaming continuation.
            streamer.await_event("APPLICATION_STREAMED", 60)

        for streamer in streamers:  # stops streaming and records results.
            streamer.stop_async()

        for streamer in streamers:
            streamer.await_stopped()

            cell = streamer.params["cell"]

            data["[%s cell %s]" % ("alive" if cell != failed_loader.params["cell"] else "broken", cell)] = \
                "worst_latency=%s, tx_streamed=%s, measure_duration=%s" % (
                    streamer.extract_result("WORST_LATENCY"), streamer.extract_result("STREAMED"),
                    streamer.extract_result("MEASURE_DURATION"))

        return data

    def start_tx_streamer(self, version, cell, discovery_spi, modules):
        """
        Starts transaction streamer.
        """
        return IgniteApplicationService(
            self.test_context,
            IgniteClientConfiguration(version=IgniteVersion(version), properties=self.properties(),
                                      discovery_spi=discovery_spi),
            java_class_name="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test.CellularTxStreamer",
            params={"cacheName": CellularAffinity.CACHE_NAME,
                    "attr": CellularAffinity.ATTRIBUTE,
                    "cell": cell,
                    "warmup": 10000},
            modules=modules, startup_timeout_sec=180)

    def start_cell_with_prepared_txs(self, version, cell_id, discovery_spi, modules):
        """
        Starts cell with prepared transactions.
        """
        nodes = self.start_cell(version, ['-D' + CellularAffinity.ATTRIBUTE + '=' + cell_id], discovery_spi, modules,
                                CellularAffinity.NODES_PER_CELL - 1)

        prepared_tx_streamer = IgniteApplicationService(  # last server node at the cell.
            self.test_context,
            IgniteConfiguration(version=IgniteVersion(version), properties=self.properties(),
                                failure_detection_timeout=self.FAILURE_DETECTION_TIMEOUT,
                                discovery_spi=from_ignite_cluster(nodes) if discovery_spi is None else discovery_spi),
            java_class_name="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test."
                            "CellularPreparedTxStreamer",
            params={"cacheName": CellularAffinity.CACHE_NAME,
                    "attr": CellularAffinity.ATTRIBUTE,
                    "cell": cell_id,
                    "txCnt": CellularAffinity.PREPARED_TX_CNT},
            jvm_opts=['-D' + CellularAffinity.ATTRIBUTE + '=' + cell_id], modules=modules, startup_timeout_sec=180)

        prepared_tx_streamer.start_async()  # starts last server node and creates prepared txs on it.

        return nodes, prepared_tx_streamer

    # pylint: disable=R0913
    def start_cell(self, version, jvm_opts, discovery_spi=None, modules=None, nodes_cnt=NODES_PER_CELL):
        """
        Starts cell.
        """
        ignites = IgniteService(
            self.test_context,
            IgniteConfiguration(version=IgniteVersion(version), properties=self.properties(),
                                cluster_state="INACTIVE",
                                failure_detection_timeout=self.FAILURE_DETECTION_TIMEOUT,
                                discovery_spi=TcpDiscoverySpi() if discovery_spi is None else discovery_spi),
            num_nodes=nodes_cnt, modules=modules, jvm_opts=jvm_opts, startup_timeout_sec=180)

        ignites.start_async()

        return ignites
