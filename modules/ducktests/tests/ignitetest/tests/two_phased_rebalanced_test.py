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

import time
from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_persistence import IgnitePersistenceAware
from ignitetest.tests.cellular_affinity_test import start_cell
from ignitetest.tests.snapshot_test import load
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223
class TwoPhasedRebalancedTest(IgniteTest):
    """
    Tests Cellular Affinity scenarios.
    """
    NUM_NODES = 3

    ATTRIBUTE = "CELL"

    CACHE_NAME = "test-cache"

    PREPARED_TX_CNT = 500  # possible amount at real cluster under load (per cell).

    @cluster(num_nodes=NUM_NODES * 3 + 1)
    @ignite_versions(str(DEV_BRANCH))
    def two_phased_rebalance_test(self, ignite_version):
        """
        Tests Cellular Affinity scenario (partition distribution).
        """
        cells = self.start_cells(ignite_version=ignite_version,
                                 cells_cnt=2,
                                 cell_nodes_cnt=4,
                                 cacheName=self.CACHE_NAME)

        ControlUtility(cells[0], self.test_context).activate()

        client_config = IgniteConfiguration(
            client_mode=True,
            version=IgniteVersion(ignite_version),
            discovery_spi=from_ignite_cluster(cells[0]),
        )

        streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.UuidStreamerApplication",
            params={
                "cacheName": "test-cache",
                "iterSize": 512 * 1024
            }
        )

        load(streamer)

        pds = self.pds_size(cells)

        self.logger.warn("PDS")
        self.logger.warn(pds)

        time.sleep(300)

    def start_cells(self, ignite_version: str, cells_cnt: int, cell_nodes_cnt: int, cacheName: str):
        """
        Start cells.
        """
        assert cells_cnt > 0

        cells = []

        cell = start_cell(self.test_context, ignite_version, [f'-D{self.ATTRIBUTE}=0'],
                          nodes_cnt=cell_nodes_cnt, cacheName=cacheName)
        cells.append(cell)

        if cells_cnt > 1:
            for i in range(1, cells_cnt):
                cells.append(start_cell(self.test_context, ignite_version, [f'-D{self.ATTRIBUTE}={i}'],
                                        nodes_cnt=cell_nodes_cnt, cacheName=cacheName, joined_cluster=cell))

        return cells

    def pds_size(self, cells: [IgniteService]):
        """
        Pds size.
        """
        assert len(cells) > 0

        res = []

        cmd = f'du -hs {IgnitePersistenceAware.WORK_DIR}/db/`hostname`'

        for cell in cells:
            cll = {}
            for node in cell.nodes:
                cll[node.account.hostname] = node.account.ssh_output(cmd)

            res.append(cll)

        return res




