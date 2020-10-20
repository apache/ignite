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

import os
from ducktape.mark.resource import cluster


from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_persistence import IgnitePersistenceAware, PersistenceAware
from ignitetest.tests.cellular_affinity_test import start_cell
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223
class TwoPhasedRebalancedTest(IgniteTest):
    """
    Tests Cellular Affinity scenarios.
    """
    NUM_NODES = 4

    ATTRIBUTE = "CELL"

    CACHE_NAME = "test-cache"

    @cluster(num_nodes=NUM_NODES * 2 + 2)
    @ignite_versions(str(DEV_BRANCH))
    def two_phased_rebalance_test(self, ignite_version):
        """
        Two-phase rebalancing test case.
        Preparations.
            1. Start 3 cells.
            2. Load data to cache with the mentioned above affinity function and fix PDS size on all nodes.
            3. Delete 80% of data and fix PDS size on all nodes.
        Phase 1.
            1. Stop two nodes in each cell, total a half of all nodes and clean PDS.
            2. Start cleaned node with preservance of consistent id and cell attributes.
            3. Wait for the rebalance to complete.
        Phase 2.
            Run steps 1-3 of Phase 2 on the other half of the cluster.
        Verifications.
            1. Check that PDS size reduced (compare to step 3)
            2. Check data consistency (idle_verify --dump)
        """
        data_storage = DataStorageConfiguration(default=DataRegionConfiguration(persistent=True),
                                                checkpoint_frequency=30000)

        cells = self.start_cells(ignite_version=ignite_version,
                                 cells_cnt=2,
                                 cell_nodes_cnt=self.NUM_NODES,
                                 cache_name=self.CACHE_NAME,
                                 data_storage=data_storage)

        control_utility = ControlUtility(cells[0], self.test_context)
        control_utility.activate()

        client_config = IgniteConfiguration(client_mode=True,
                                            version=IgniteVersion(ignite_version),
                                            discovery_spi=from_ignite_cluster(cells[0]))

        streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.UuidStreamerApplication",
            params={
                "cacheName": "test-cache",
                "iterSize": 500 * 1024
            }
        )

        deleter = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.DeleteDataApplication",
            params={
                "cacheName": "test-cache",
                "iterSize": 400 * 1024
            }
        )

        streamer.start()
        streamer.await_stopped(15 * 60)

        node = cells[0].nodes[0]
        cells[0].await_event_on_node('Checkpoint finished', node, timeout_sec=30)

        pds = self.pds_size(cells)

        self.logger.warn("Step prepare, load data. PDS")
        self.logger.warn(pds)

        deleter.start()
        deleter.await_stopped(timeout_sec=(15 * 60))

        cells[0].await_event_on_node('Checkpoint finished', node, timeout_sec=30)

        control_utility.validate_indexes(check_assert=True)
        dump_1 = control_utility.idle_verify_dump(node=node, return_path=True)
        dump_1 = self.move_dump_to_logs(node, dump_1)

        pds = self.pds_size(cells)

        self.logger.warn("After Delete 80%, PDS")
        self.logger.warn(pds)

        self.stop_clean_idx_node_on_cell(cells, 2, 3)

        self.start_idx_node_on_cell(cells, 2, 3)

        cells[0].await_rebalance(timeout_sec=15 * 60)

        pds = self.pds_size(cells)

        self.logger.warn("After rebalancing complate on nodes 2, 3. PDS")
        self.logger.warn(pds)

        self.stop_clean_idx_node_on_cell(cells, 0, 1)

        self.start_idx_node_on_cell(cells, 0, 1)

        cells[0].await_rebalance(timeout_sec=15 * 60)

        pds = self.pds_size(cells)

        self.logger.warn("After rebalancing complate on nodes 0, 1. PDS")
        self.logger.warn(pds)

        control_utility.validate_indexes(check_assert=True)
        dump_2 = control_utility.idle_verify_dump(node=node, return_path=True)
        dump_2 = self.move_dump_to_logs(node, dump_2)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}')
        assert len(diff) == 0, diff


    def start_cells(self, ignite_version: str, cells_cnt: int, cell_nodes_cnt: int, cache_name: str,
                    data_storage: DataStorageConfiguration = None):
        """
        Start cells.
        """
        assert cells_cnt > 0

        cells = []

        cell = start_cell(self.test_context, ignite_version, [f'-D{self.ATTRIBUTE}=0'],
                          nodes_cnt=cell_nodes_cnt, cache_name=cache_name, data_storage=data_storage)
        cells.append(cell)

        if cells_cnt > 1:
            for i in range(1, cells_cnt):
                cells.append(start_cell(self.test_context, ignite_version, [f'-D{self.ATTRIBUTE}={i}'],
                                        nodes_cnt=cell_nodes_cnt, cache_name=cache_name, data_storage=data_storage,
                                        joined_cluster=cell))

        return cells

    def pds_size(self, cells):
        """
        Pds size.
        """
        assert len(cells) > 0

        res = []

        for cell in cells:
            cll = {}
            for node in cell.nodes:
                cmd = f'du -sk {IgnitePersistenceAware.WORK_DIR}/db/{node.account.hostname} | ' + "awk '{print $1}'"
                cll[node.account.hostname] = node.account.ssh_output(cmd).decode("utf-8").replace('\n', 'kb')

            res.append(cll)

        return res

    def stop_clean_idx_node_on_cell(self, cells: [IgniteService], *idx: int):
        for cell in cells:
            size = len(cell.nodes)

            for i in idx:
                assert i < size

                node = cell.nodes[i]

                cell.stop_node(node)
                cell.remove(node, cell.WORK_DIR)

    def start_idx_node_on_cell(self, cells: [IgniteService], *idx: int, timeout_sec=180):
        for cell in cells:
            size = len(cell.nodes)
            for i in idx:
                assert i < size

                node = cell.nodes[i]

                cell.start_node(node)

                cell.await_node_started(node, timeout_sec)

    def move_dump_to_logs(self, node, dump_path: str):
        """
        Move dump file to logs directory.
        @:return new path to dump_file.
        """
        node.account.ssh_output(f'mv {dump_path} {PersistenceAware.PATH_TO_LOGS_DIR}')

        return dump_path.replace(IgnitePersistenceAware.WORK_DIR, PersistenceAware.PATH_TO_LOGS_DIR)
