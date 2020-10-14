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
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
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

    @cluster(num_nodes=NUM_NODES * 3 + 1)
    @ignite_versions(str(DEV_BRANCH))
    def two_phased_rebalance_test(self, ignite_version):
        """
        Tests Cellular Affinity scenario (partition distribution).
        """
        data_storage = DataStorageConfiguration(default=DataRegionConfiguration(persistent=True),
                                                wal_mode='NONE')

        cells = self.start_cells(ignite_version=ignite_version,
                                 cells_cnt=1,
                                 cell_nodes_cnt=4,
                                 cache_name=self.CACHE_NAME,
                                 data_storage=data_storage)

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
            timeout_sec=180,
            params={
                "cacheName": "test-cache",
                "iterSize": 10 * 1024
            }
        )

        deleter = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.DeleteDataApplication",
            timeout_sec=180,
            params={
                "cacheName": "test-cache",
                "iterSize": 8 * 1024
            }
        )

        load(streamer)

        pds = self.pds_size(cells)

        self.logger.warn("PDS")
        self.logger.warn(pds)

        load(deleter)

        pds = self.pds_size(cells)

        self.logger.warn("PDS")
        self.logger.warn(pds)

        time.sleep(10)

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
                cmd = f'du -hs {IgnitePersistenceAware.WORK_DIR}/db/{node.account.hostname} | ' + "awk '{print $1}'"
                cll[node.account.hostname] = node.account.ssh_output(cmd).decode("utf-8")

            res.append(cll)

        return res




