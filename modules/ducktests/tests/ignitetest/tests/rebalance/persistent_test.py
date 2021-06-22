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
Module contains persistent rebalance tests.
"""
from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.rebalance.util import DEFAULT_DATA_REGION_SZ, NUM_NODES, start_ignite, TriggerEvent, \
    preload_data, get_result, check_type_of_rebalancing, await_rebalance_start
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


# pylint: disable=W0223
class RebalancePersistentTest(IgniteTest):
    """
    Tests rebalance scenarios in persistent mode.
    """
    # pylint: disable=too-many-arguments, too-many-locals
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[5_000], entry_size=[50_000], preloaders=[1],
              thread_pool_size=[None], batch_size=[None], batches_prefetch_count=[None], throttle=[None])
    def test_node_join(self, ignite_version, backups, cache_count, entry_count, entry_size, preloaders,
                       thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Tests rebalance on node join.
        """
        ignites = start_ignite(self.test_context, ignite_version, TriggerEvent.NODE_JOIN, backups, cache_count,
                               entry_count, entry_size, preloaders, thread_pool_size, batch_size,
                               batches_prefetch_count, throttle, True)

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            preloaders, backups, cache_count, entry_count, entry_size)

        new_node = IgniteService(self.test_context, ignites.config._replace(discovery_spi=from_ignite_cluster(ignites)),
                                 num_nodes=1)
        new_node.start()

        control_utility.add_to_baseline(new_node.nodes)

        await_and_check_rebalance(new_node, new_node.nodes)

        control_utility.deactivate()

        nodes = ignites.nodes.copy()

        nodes.append(new_node.nodes[0])

        self.logger.debug(f'DB size after rebalance mb: {get_database_size_mb(nodes, ignites.database_dir)}')

        return get_result(new_node.nodes, preload_time, cache_count, entry_count, entry_size)

    # pylint: disable=too-many-arguments, too-many-locals
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[50_000], entry_size=[50_000], preloaders=[1],
              thread_pool_size=[None], batch_size=[None], batches_prefetch_count=[None], throttle=[None])
    def test_node_left(self, ignite_version, backups, cache_count, entry_count, entry_size, preloaders,
                       thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Tests rebalance on node left.
        """
        ignites = start_ignite(self.test_context, ignite_version, TriggerEvent.NODE_LEFT, backups, cache_count,
                               entry_count, entry_size, preloaders, thread_pool_size, batch_size,
                               batches_prefetch_count, throttle, True)

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            preloaders, backups, cache_count, entry_count, entry_size)

        self.logger.debug(f'DB size before rebalance mb: {get_database_size_mb(ignites.nodes, ignites.database_dir)}')

        node = ignites.nodes[-1]

        ignites.stop_node(node)
        assert ignites.wait_node(node)

        control_utility.remove_from_baseline([node])

        await_and_check_rebalance(ignites, ignites.alive_nodes)

        control_utility.deactivate()

        self.logger.debug(f'DB size after rebalance mb: {get_database_size_mb(ignites.nodes, ignites.database_dir)}')

        return get_result(ignites.nodes[:-1], preload_time, cache_count, entry_count, entry_size)

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[15_000], entry_size=[50_000], preloaders=[1],
              thread_pool_size=[None], batch_size=[None], batches_prefetch_count=[None], throttle=[None])
    def node_join_historical_test(self, ignite_version, backups, cache_count, entry_count, entry_size, preloaders,
                                  thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Test historycal rebalance.
        """

        preload_entries = 10_000

        version = IgniteVersion(ignite_version)

        ignite_config = IgniteConfiguration(
            version=version,
            data_storage=DataStorageConfiguration(
                max_wal_archive_size=max(2 * cache_count * entry_count * entry_size * (backups + 1),
                                         DEFAULT_DATA_REGION_SZ),
                default=DataRegionConfiguration(persistent=True,
                                                max_size=max(cache_count * entry_count * entry_size * (backups + 1),
                                                             DEFAULT_DATA_REGION_SZ)
                                                )),
            metric_exporter="org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi",
            rebalance_thread_pool_size=thread_pool_size,
            rebalance_batch_size=batch_size,
            rebalance_batches_prefetch_count=batches_prefetch_count,
            rebalance_throttle=throttle
        )

        ignites = IgniteService(self.test_context, ignite_config, num_nodes=len(self.test_context.cluster) - preloaders,
                                jvm_opts=['-DIGNITE_PDS_WAL_REBALANCE_THRESHOLD=0',
                                          '-DIGNITE_PREFER_WAL_REBALANCE=true'])
        ignites.start()

        control_utility = ControlUtility(ignites)
        control_utility.activate()

        preloader_config = ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites))

        preloader = IgniteApplicationService(
            self.test_context,
            preloader_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.rebalance.DataGenerationApplication",
            params={"backups": 1, "cacheCount": 1, "entrySize": 1, "from": 0, "to": preload_entries}
        )

        preloader.run()
        preloader.free()

        control_utility.deactivate()
        control_utility.activate()

        node = ignites.nodes[-1]

        ignites.stop_node(node)
        assert ignites.wait_node(node)

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            preloaders, backups, cache_count, entry_count, entry_size)

        control_utility.deactivate()
        control_utility.activate()

        self.logger.debug(f'DB size before rebalance mb.: {get_database_size_mb(ignites.nodes, ignites.database_dir)}')

        ignites.start_node(node)
        ignites.await_started()

        rebalance_nodes = [node]

        await_and_check_rebalance(ignites, rebalance_nodes, False)

        control_utility.deactivate()

        self.logger.debug(f'DB size after rebalance mb: {get_database_size_mb(ignites.nodes, ignites.database_dir)}')

        return get_result(rebalance_nodes, preload_time, cache_count, entry_count, entry_size)


def await_and_check_rebalance(service: IgniteService, rebalance_nodes: list, is_full: bool = True):
    """
    Check rebalance.

    :param service: IgniteService.
    :param rebalance_nodes: Ignite nodes in which rebalance will be awaited.
    :param is_full: Expected type of rebalancing.
    """

    await_rebalance_start(service)

    service.await_rebalance()

    check_type_of_rebalancing(rebalance_nodes, is_full=is_full)

    ControlUtility(service).idle_verify()


def get_database_size_mb(nodes: list, database_dir: str) -> dict:
    """
    Return databases size in megabytes.

    :param nodes: List of nodes.
    :param database_dir: Path to database directory.
    :return Dictionary with node hostname -> list directories with size in megabytes.

    {
        'ducker02': [
            '/mnt/service/work/db/wal -> 1664 mb.',
            '/mnt/service/work/db/ducker02 -> 540 mb.',
            '/mnt/service/work/db/marshaller -> 1 mb.',
            '/mnt/service/work/db/binary_meta -> 1 mb.',
            '/mnt/service/work/db -> 2204 mb.'
        ],
        ...
    }
    """
    res = {}
    for node in nodes:
        cmd = f'du -md1 {database_dir}'
        out, err = IgniteAwareService.exec_command_ex(node, cmd)

        assert not err, f"Command failed: '{cmd}'.\nError: '{err}'"

        node_res = list(map(lambda v: ' -> '.join(v.split('\t')[::-1]), out.splitlines()))
        node_res = list(map(lambda v: (v + ' mb.'), node_res))

        res[node.account.hostname] = node_res

    return res
