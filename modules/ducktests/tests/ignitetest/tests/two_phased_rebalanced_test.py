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
import ducktape
from ducktape.mark.resource import cluster
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration, Affinity

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.util import copy_file_to_dest
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223
class TwoPhasedRebalancedTest(IgniteTest):
    """
    Two-phase rebalancing test case.
    """
    NUM_NODES_CELL = 4

    NUM_CELL = 4

    ATTRIBUTE = "CELL"

    CACHE_NAME = "test-cache"

    # pylint: disable=R0914
    @cluster(num_nodes=NUM_NODES_CELL * NUM_CELL + 2)
    @ignite_versions(str(DEV_BRANCH))
    def two_phased_rebalancing_test(self, ignite_version):
        """
        Test case of two-phase rebalancing.
        Preparations.
            1. Start 4 cells.
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
                                 cells_cnt=self.NUM_CELL,
                                 cell_nodes_cnt=self.NUM_NODES_CELL,
                                 cache_name=self.CACHE_NAME,
                                 data_storage=data_storage)

        control_utility = ControlUtility(cells[0])
        control_utility.activate()

        client_config = IgniteConfiguration(client_mode=True,
                                            version=IgniteVersion(ignite_version),
                                            discovery_spi=from_ignite_cluster(cells[0]))

        streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.snapshot_test.DataLoaderApplication",
            params={"start": 0,
                    "cacheName": "test-cache",
                    "interval": 500 * 1024,
                    "valueSizeKb": 1}
        )

        deleter = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.DeleteDataApplication",
            shutdown_timeout_sec=15 * 60,
            params={"cacheName": "test-cache",
                    "size": 400 * 1024})

        streamer.run()

        node = cells[0].nodes[0]
        try:
            cells[0].await_event_on_node('Skipping checkpoint', node, timeout_sec=60)
        except ducktape.errors.TimeoutError as ex:
            self.logger.warn(ex)

        self.fix_pds_size(cells, "Step prepare, load data. PDS.")

        deleter.run()

        try:
            cells[0].await_event_on_node('Skipping checkpoint', node, timeout_sec=60)
        except ducktape.errors.TimeoutError as ex:
            self.logger.warn(ex)

        dump_1 = fix_data(control_utility, node, cells[0].log_dir)

        pds_before = self.fix_pds_size(cells, "After Delete 80%, PDS.")

        restart_with_clean_idx_node_on_cell(cells, [0, 1])

        for cell in cells:
            cell.await_rebalance(timeout_sec=15 * 60)

        self.fix_pds_size(cells, "After rebalancing complate on nodes 0, 1. PDS.")

        restart_with_clean_idx_node_on_cell(cells, [2, 3])

        try:
            cells[0].await_event_on_node('Skipping checkpoint', node, timeout_sec=60)
        except ducktape.errors.TimeoutError as ex:
            self.logger.warn(ex)

        for cell in cells:
            cell.await_rebalance(timeout_sec=15 * 60)

        pds_after = self.fix_pds_size(cells, "After rebalancing complate on nodes 2, 3. PDS.")

        check_pds_size(pds_before, pds_after)

        dump_2 = fix_data(control_utility, node, cells[0].log_dir)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}', allow_fail=True)
        assert not diff

    # pylint: disable=R0913
    def start_cells(self, ignite_version: str, cells_cnt: int, cell_nodes_cnt: int, cache_name: str,
                    data_storage: DataStorageConfiguration = None):
        """
        Start cells.
        """
        assert cells_cnt > 0

        cells = []

        cache_cfg = CacheConfiguration(name=cache_name, backups=self.NUM_NODES_CELL-1, affinity=Affinity(),
                                       atomicity_mode='TRANSACTIONAL')

        config = IgniteConfiguration(version=IgniteVersion(ignite_version), data_storage=data_storage,
                                     caches=[cache_cfg],
                                     metric_exporter='org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi')

        cell = start_cell(self.test_context, config, [f'-D{self.ATTRIBUTE}=0'], num_nodes=cell_nodes_cnt,)

        discovery_spi = from_ignite_cluster(cell)
        config = config._replace(discovery_spi=discovery_spi)

        cells.append(cell)

        if cells_cnt > 1:
            for i in range(1, cells_cnt):
                cells.append(start_cell(self.test_context, config, [f'-D{self.ATTRIBUTE}={i}'],
                                        num_nodes=cell_nodes_cnt))

        return cells

    def fix_pds_size(self, cells: [IgniteService], msg: str):
        """
        Pds size in megabytes.
        """
        assert len(cells) > 0

        res = {}

        for cell in cells:
            for node in cell.nodes:
                consistent_id = str(node.account.hostname).replace('.', '_').replace('-', '_')
                cmd = f'du -sm {cell.database_dir}/{consistent_id} | ' + "awk '{print $1}'"
                res[node.account.hostname] = int(node.account.ssh_output(cmd).decode("utf-8").rstrip())

        self.logger.warn(msg)

        for item in res.items():
            self.logger.warn(f'Host: {item[0]}, PDS {item[1]}mb')

        return res


def restart_with_clean_idx_node_on_cell(cells: [IgniteService], idxs: [int]):
    """
    Restart idxs nodes on cells with cleaning working directory.
    """
    stop_idx_node_on_cell(cells, idxs)
    clean_work_idx_node_on_cell(cells, idxs)
    start_idx_node_on_cell(cells, idxs)


def stop_idx_node_on_cell(cells: [IgniteService], idxs: [int]):
    """
    Stop idxs nodes on cells.
    """
    for cell in cells:
        size = len(cell.nodes)

        for i in idxs:
            assert i < size

            node = cell.nodes[i]

            cell.stop_node(node)

        for i in idxs:
            node = cell.nodes[i]
            cell.wait_node(node)


def clean_work_idx_node_on_cell(cells: [IgniteService], idxs: [int]):
    """
    Cleaning the working directory on idxs nodes in cells.
    """
    for cell in cells:
        size = len(cell.nodes)

        for i in idxs:
            assert i < size

            node = cell.nodes[i]

            assert not cell.pids(node)

            node.account.ssh(f'rm -rf {cell.work_dir}')


def start_idx_node_on_cell(cells: [IgniteService], idxs: [int]):
    """
    Start idxs nodes on cells.
    """
    for cell in cells:
        size = len(cell.nodes)

        for i in idxs:
            assert i < size

            node = cell.nodes[i]

            cell.start_node(node)

    for cell in cells:
        cell.await_started()


def check_pds_size(pds_before, pds_after):
    """
    Checks that the size of the pds has become smaller.
    """
    for host in pds_after:
        assert pds_after[host] < pds_before[host], f'Host {host}: size after = {pds_after[host]}, ' \
                                                   f'size before = {pds_before[host]}.'


def fix_data(control_utility: ControlUtility, node, log_dir):
    """
    :return: Path to idle-verify dump file.
    """
    control_utility.validate_indexes()
    control_utility.idle_verify()

    dump = control_utility.idle_verify_dump(node)

    return copy_file_to_dest(node, dump, log_dir)


# pylint: disable=R0913
def start_cell(test_context, config, jvm_opts: None, modules=None, num_nodes=4):
    """
    Starts cell.
    """
    ignites = IgniteService(test_context, config, modules=modules, num_nodes=num_nodes, jvm_opts=jvm_opts,
                            startup_timeout_sec=180)

    ignites.start()

    return ignites
