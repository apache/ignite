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

from ducktape.mark.resource import cluster
from jinja2 import Template

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteClientConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions, version_if
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion, LATEST_2_8


# pylint: disable=W0223
class CellularAffinity(IgniteTest):
    """
    Tests Cellular Affinity scenarios.
    """
    NUM_NODES = 3

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
    def properties(backups: int = NUM_NODES, attr: str = ATTRIBUTE, cacheName: str = CACHE_NAME):
        """
        :return: Configuration properties.
        """
        return Template(CellularAffinity.CONFIG_TEMPLATE) \
            .render(backups=backups,  # bigger than cell capacity (to handle single cell useless test)
                    attr=attr,
                    cacheName=cacheName)

    @cluster(num_nodes=NUM_NODES * 3 + 1)
    @version_if(lambda version: version >= DEV_BRANCH)
    @ignite_versions(str(DEV_BRANCH))
    def test_distribution(self, ignite_version):
        """
        Tests Cellular Affinity scenario (partition distribution).
        """
        cell1 = start_cell(self.test_context, ignite_version, ['-D' + CellularAffinity.ATTRIBUTE + '=1'])
        start_cell(self.test_context, ignite_version, ['-D' + CellularAffinity.ATTRIBUTE + '=2'],
                        joined_cluster=cell1)
        start_cell(self.test_context, ignite_version, ['-D' + CellularAffinity.ATTRIBUTE + '=XXX', '-DRANDOM=42'],
                        joined_cluster=cell1)

        ControlUtility(cell1, self.test_context).activate()

        checker = IgniteApplicationService(
            self.test_context,
            IgniteClientConfiguration(version=IgniteVersion(ignite_version), discovery_spi=from_ignite_cluster(cell1)),
            java_class_name="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test.DistributionChecker",
            params={"cacheName": CellularAffinity.CACHE_NAME,
                    "attr": CellularAffinity.ATTRIBUTE,
                    "nodesPerCell": self.NUM_NODES})

        checker.run()

    # pylint: disable=R0914
    @cluster(num_nodes=NUM_NODES * (3 + 1))
    @ignite_versions(str(DEV_BRANCH), str(LATEST_2_8))
    def test_latency(self, ignite_version):
        """
        Tests Cellular switch tx latency.
        """
        data = {}

        cell1, prepared_tx_loader1 = self.start_cell_with_prepared_txs(ignite_version, "C1")
        _, prepared_tx_loader2 = self.start_cell_with_prepared_txs(ignite_version, "C2", joined_cluster=cell1)
        _, prepared_tx_loader3 = self.start_cell_with_prepared_txs(ignite_version, "C3", joined_cluster=cell1)

        loaders = [prepared_tx_loader1, prepared_tx_loader2, prepared_tx_loader3]

        failed_loader = prepared_tx_loader3

        tx_streamer1 = self.start_tx_streamer(ignite_version, "C1", joined_cluster=cell1)
        tx_streamer2 = self.start_tx_streamer(ignite_version, "C2", joined_cluster=cell1)
        tx_streamer3 = self.start_tx_streamer(ignite_version, "C3", joined_cluster=cell1)

        streamers = [tx_streamer1, tx_streamer2, tx_streamer3]

        for streamer in streamers:  # starts tx streaming with latency record (with some warmup).
            streamer.start()

        ControlUtility(cell1, self.test_context).disable_baseline_auto_adjust()  # baseline set.
        ControlUtility(cell1, self.test_context).activate()

        for loader in loaders:
            loader.await_event("All transactions prepared", 180, from_the_beginning=True)

        for streamer in streamers:
            streamer.await_event("Warmup finished", 180, from_the_beginning=True)

        failed_loader.stop_async()  # node left with prepared txs.

        for streamer in streamers:
            streamer.await_event("Node left topology\\|Node FAILED", 60, from_the_beginning=True)

        for streamer in streamers:  # just an assertion that we have PME-free switch.
            streamer.await_event("exchangeFreeSwitch=true", 60, from_the_beginning=True)

        for streamer in streamers:  # waiting for streaming continuation.
            streamer.await_event("Application streamed", 60)

        for streamer in streamers:  # stops streaming and records results.
            streamer.stop_async()

        for streamer in streamers:
            streamer.await_stopped()

            cell = streamer.params["cell"]

            data["[%s cell %s]" % ("alive" if cell is not failed_loader.params["cell"] else "broken", cell)] = \
                "worst_latency=%s, tx_streamed=%s, measure_duration=%s" % (
                    streamer.extract_result("WORST_LATENCY"), streamer.extract_result("STREAMED"),
                    streamer.extract_result("MEASURE_DURATION"))

        return data

    def start_tx_streamer(self, version, cell, joined_cluster):
        """
        Starts transaction streamer.
        """
        return IgniteApplicationService(
            self.test_context,
            IgniteClientConfiguration(version=IgniteVersion(version), properties=self.properties(),
                                      discovery_spi=from_ignite_cluster(joined_cluster)),
            java_class_name="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test.CellularTxStreamer",
            params={"cacheName": CellularAffinity.CACHE_NAME,
                    "attr": CellularAffinity.ATTRIBUTE,
                    "cell": cell,
                    "warmup": 10000},
            timeout_sec=180)

    def start_cell_with_prepared_txs(self, version, cell_id, joined_cluster=None):
        """
        Starts cell with prepared transactions.
        """
        nodes = start_cell(self.test_context, version, ['-D' + CellularAffinity.ATTRIBUTE + '=' + cell_id],
                           nodes_cnt=self.NUM_NODES - 1, joined_cluster=joined_cluster)

        prepared_tx_streamer = IgniteApplicationService(  # last server node at the cell.
            self.test_context,
            IgniteConfiguration(version=IgniteVersion(version), properties=self.properties(),
                                discovery_spi=from_ignite_cluster(nodes)),  # Server node.
            java_class_name="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test."
                            "CellularPreparedTxStreamer",
            params={"cacheName": CellularAffinity.CACHE_NAME,
                    "attr": CellularAffinity.ATTRIBUTE,
                    "cell": cell_id,
                    "txCnt": CellularAffinity.PREPARED_TX_CNT},
            jvm_opts=['-D' + CellularAffinity.ATTRIBUTE + '=' + cell_id],
            timeout_sec=180)

        prepared_tx_streamer.start()  # starts last server node and creates prepared txs on it.

        return nodes, prepared_tx_streamer


def start_cell(
        test_context,
        version: str,
        jvm_opts: [],
        nodes_cnt: int = CellularAffinity.NUM_NODES,
        joined_cluster: IgniteService = None,
        backups: int = CellularAffinity.NUM_NODES,
        attr: str = CellularAffinity.ATTRIBUTE,
        cacheName: str = CellularAffinity.CACHE_NAME):

    """
    Starts cell.
    """
    config = IgniteConfiguration(version=IgniteVersion(version),
                                 properties=CellularAffinity.properties(backups, attr, cacheName),
                                 cluster_state="INACTIVE")

    if joined_cluster:
        config = config._replace(discovery_spi=from_ignite_cluster(joined_cluster))

    ignites = IgniteService(test_context, config, num_nodes=nodes_cnt, jvm_opts=jvm_opts)

    ignites.start()

    return ignites
