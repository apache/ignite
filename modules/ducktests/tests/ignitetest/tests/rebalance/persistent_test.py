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
import os

from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.rebalance import DEFAULT_DATA_REGION_SZ, NUM_NODES, start_ignite, TriggerEvent, preload_data, \
    get_result, check_type_of_rebalancing, await_rebalance_start
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
                               batches_prefetch_count, throttle)

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            preloaders, backups, cache_count, entry_count, entry_size)

        self.logger.warn(f'db size: {get_database_size_mb(ignites)}')

        new_node = IgniteService(self.test_context, ignites.config._replace(discovery_spi=from_ignite_cluster(ignites)),
                                 num_nodes=1)
        new_node.start()

        control_utility.add_to_baseline(new_node.nodes)

        new_node.await_rebalance(600)

        check_type_of_rebalancing(new_node.nodes)

        control_utility.idle_verify()

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
                               batches_prefetch_count, throttle)

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            preloaders, backups, cache_count, entry_count, entry_size)

        self.logger.warn(f'db size: {get_database_size_mb(ignites)}')

        node = ignites.nodes[-1]

        ignites.stop_node(node)
        ignites.wait_node(node, 30)

        control_utility.remove_from_baseline([node])

        ignites.await_rebalance(600)

        check_type_of_rebalancing(ignites.nodes[:-1])

        control_utility.idle_verify()

        return get_result(ignites.nodes[:-1], preload_time, cache_count, entry_count, entry_size)

    @cluster(num_nodes=NUM_NODES+1)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[50_000], entry_size=[50_000], preloaders=[1],
              thread_pool_size=[None], batch_size=[None], batches_prefetch_count=[None], throttle=[None])
    def test_node_left_during_rebalance_on_join_node(self, ignite_version, backups, cache_count, entry_count,
                                                     entry_size, preloaders, thread_pool_size, batch_size,
                                                     batches_prefetch_count, throttle):
        """
        Tests rebalance on node join, node left during rebalance.
        """

        ignites = start_ignite(self.test_context, ignite_version, TriggerEvent.NODE_JOIN, backups, cache_count,
                               entry_count, entry_size, preloaders, thread_pool_size, batch_size,
                               batches_prefetch_count, throttle)

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            preloaders, backups, cache_count, entry_count, entry_size)

        control_utility.deactivate()
        control_utility.activate()

        self.logger.debug(f'DB size before rebalance: {get_database_size_mb(ignites)}')

        new_node = IgniteService(self.test_context, ignites.config._replace(discovery_spi=from_ignite_cluster(ignites)),
                                 num_nodes=1)
        new_node.start()

        control_utility.add_to_baseline(new_node.nodes)

        await_rebalance_start(new_node.nodes)

        ignites.stop_node(ignites.nodes[-1])

        new_node.await_rebalance(600)

        check_type_of_rebalancing(new_node.nodes)

        control_utility.idle_verify()

        control_utility.deactivate()

        self.logger.debug(f'DB size after rebalance: {get_database_size_mb(ignites)}')

        return get_result(new_node.nodes, preload_time, cache_count, entry_count, entry_size)

    @cluster(num_nodes=NUM_NODES + 1)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[50_000], entry_size=[50_000], preloaders=[1],
              thread_pool_size=[None], batch_size=[None], batches_prefetch_count=[None], throttle=[None])
    def test_node_join_during_rebalance_on_left_node(self, ignite_version, backups, cache_count, entry_count,
                                                     entry_size, preloaders, thread_pool_size, batch_size,
                                                     batches_prefetch_count, throttle):
        """
        Tests rebalance on node left, node join during rebalance.
        """

        ignites = start_ignite(self.test_context, ignite_version, TriggerEvent.NODE_LEFT, backups, cache_count,
                               entry_count, entry_size, preloaders + 1, thread_pool_size, batch_size,
                               batches_prefetch_count, throttle)

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            preloaders, backups, cache_count, entry_count, entry_size)

        self.logger.warn(f'db size: {get_database_size_mb(ignites)}')

        node = ignites.nodes[-1]

        reb_nodes = ignites.nodes[:-1]

        ignites.stop_node(node)
        ignites.wait_node(node, 30)

        control_utility.remove_from_baseline([node], ignites.nodes[0])

        await_rebalance_start(reb_nodes)

        new_node = IgniteService(self.test_context, ignites.config._replace(discovery_spi=from_ignite_cluster(ignites)),
                                 num_nodes=1)
        new_node.start()

        return get_result(reb_nodes, preload_time, cache_count, entry_count, entry_size)

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
            java_class_name="org.apache.ignite.internal.ducktest.tests.rebalance.DataGenerationApplicationBatchPutAll",
            # jvm_opts=['-DIGNITE_PDS_WAL_REBALANCE_THRESHOLD=0', '-DIGNITE_PREFER_WAL_REBALANCE=true'],
            params={"backups": 1, "cacheCount": 1, "entrySize": 1, "from": 0, "to": preload_entries}
        )

        preloader.run()
        preloader.free()

        control_utility.deactivate()
        control_utility.activate()

        node = ignites.nodes[-1]

        ignites.stop_node(node)
        assert ignites.wait_node(node, 30)

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            preloaders, backups, cache_count, entry_count, entry_size,
            "org.apache.ignite.internal.ducktest.tests.rebalance.DataGenerationApplicationBatchPutAll"
        )

        control_utility.deactivate()
        control_utility.activate()

        self.logger.warn(f'DB size before rebalance: {get_database_size_mb(ignites)}')

        ignites.start_node(node)
        ignites.await_started()

        ignites.await_rebalance(600)

        check_type_of_rebalancing([node], is_full=False)

        control_utility.idle_verify()

        control_utility.deactivate()

        self.logger.warn(f'DB size after rebalance: {get_database_size_mb(ignites)}')

        return get_result([node], preload_time, 1, entry_count, entry_size)


def get_database_size_mb(ignites, check_error=True):
    """Executes the command passed on the self.nodes and returns out result as dict."""
    res = {}
    for node in ignites.nodes:
        cons_id = node.account.hostname.replace('.', '_').replace('-', '_')
        cmd = "du -ms %s | awk {'print $1'}" % os.path.join(ignites.database_dir, cons_id)
        out, err = IgniteAwareService.exec_command_ex(node, cmd)

        if check_error:
            assert len(err) == 0, f"Command failed: '{cmd}'.\nError: '{err}'"

        res[node.account.hostname] = out.replace('\n', 'mb.')

    return res
