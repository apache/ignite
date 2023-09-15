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
Module contains index.bin rebuild tests.
"""
import time

from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration import DataStorageConfiguration, IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.rebalance.util import NUM_NODES
from ignitetest.tests.util import DataGenerationParams, preload_data
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


def get_file_sizes(nodes: list, file: str) -> dict:
    """
    Return file size in bytes.

    :param nodes: List of nodes.
    :param file: File to get size for.
    :return Dictionary with key as hostname value is files sizes on the node.
    """
    res = {}
    for node in nodes:
        out = IgniteAwareService.exec_command(node, f'du -sb {file}')

        data = out.split("\t")

        res[node.account.hostname] = int(data[0])

    return res


CACHE_NAME = "test-cache-1"


class IndexRebuildTest(IgniteTest):
    """
    Tests index.bin rebuild.
    """

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[50000], entry_size=[50], preloaders=[1], index_count=[3])
    def test_index_bin_rebuild(self, ignite_version, backups, cache_count, entry_count, entry_size, preloaders,
                               index_count):
        """
        Tests index.bin rebuild on node start.
        """

        data_gen_params = DataGenerationParams(backups=backups, cache_count=cache_count, entry_count=entry_count,
                                               entry_size=entry_size, preloaders=preloaders, index_count=index_count)

        ignites = self.start_ignite(ignite_version, data_gen_params)

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        control_utility.disable_baseline_auto_adjust()

        _, version, _ = control_utility.cluster_state()
        control_utility.set_baseline(version)

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            data_gen_params=data_gen_params)

        control_utility.deactivate()

        ignites.stop()

        wal_before_rebuild = get_file_sizes(ignites.nodes, ignites.wal_dir)
        idx_before_rebuild = get_file_sizes(ignites.nodes, ignites.index_file('*', CACHE_NAME))

        for node in ignites.nodes:
            IgniteAwareService.exec_command(
                node,
                f'rm {ignites.index_file(ignites.consistent_dir(node.account.externally_routable_ip), CACHE_NAME)}')

        start_time = round(time.time() * 1000)

        ignites.start(clean=False)

        control_utility.activate()

        timeout_sec = round(data_gen_params.entry_count / (len(ignites.nodes) * 250))

        ignites.await_event(f"Started indexes rebuilding for cache \\[name={CACHE_NAME}, grpName=null\\]",
                            from_the_beginning=True, timeout_sec=timeout_sec)
        ignites.await_event("Indexes rebuilding completed for all caches.", from_the_beginning=True,
                            timeout_sec=timeout_sec)

        control_utility.deactivate()

        ignites.stop()

        rebuild_time = round(time.time() * 1000) - start_time

        wal_after_rebuild = get_file_sizes(ignites.nodes, ignites.wal_dir)
        idx_after_rebuild = get_file_sizes(ignites.nodes, ignites.index_file('*', CACHE_NAME))

        wal_enlargement = {}
        for node in wal_before_rebuild:
            wal_enlargement[node] = wal_after_rebuild[node] - wal_before_rebuild[node]

        return {
            "preload_time": preload_time,
            "wal_before_rebuild": wal_before_rebuild,
            "wal_after_rebuild": wal_after_rebuild,
            "idx_before_rebuild": idx_before_rebuild,
            "idx_after_rebuild": idx_after_rebuild,
            "wal_enlargement_bytes": wal_enlargement,
            "rebuild_time_ms": rebuild_time
        }

    def start_ignite(self, ignite_version: str, data_gen_params: DataGenerationParams) -> IgniteService:
        """
        Start IgniteService:

        :param ignite_version: Ignite version.
        :param data_gen_params: Data generation parameters.
        :return: IgniteService.
        """
        node_count = self.available_cluster_size - data_gen_params.preloaders

        node_config = IgniteConfiguration(
            cluster_state='INACTIVE',
            auto_activation_enabled=False,
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                max_wal_archive_size=1000 * data_gen_params.data_region_max_size,
                wal_segment_size=50 * 1024 * 1024,
                default=DataRegionConfiguration(
                    persistence_enabled=True,
                    max_size=data_gen_params.data_region_max_size
                )
            ),
            metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"}
        )

        ignites = IgniteService(self.test_context, config=node_config, num_nodes=node_count)
        ignites.start()

        return ignites
