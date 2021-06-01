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
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.rebalance import preload_data, get_result, start_ignite, TriggerEvent, NUM_NODES, \
    await_rebalance_start, await_rebalance_complete
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST


# pylint: disable=W0223
class RebalancePersistentTest(IgniteTest):
    """
    Tests rebalance scenarios in in-memory mode.
    """
    # pylint: disable=too-many-arguments, too-many-locals
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[50_000], entry_size=[50_000], preloaders=[1],
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

        return get_result(new_node.nodes, preload_time, cache_count, entry_count, entry_size)

    # pylint: disable=too-many-arguments, too-many-locals
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[15_000], entry_size=[50_000], preloaders=[1],
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

        new_node = IgniteService(self.test_context, ignites.config._replace(discovery_spi=from_ignite_cluster(ignites)),
                                 num_nodes=1)
        new_node.start()

        control_utility.add_to_baseline(new_node.nodes)

        await_rebalance_start(new_node.nodes)

        ignites.stop_node(ignites.nodes[-1])

        await_rebalance_complete(new_node.nodes, cache_count)

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

        ignites.stop_node(node)
        ignites.wait_node(node, 30)

        control_utility.remove_from_baseline([node], ignites.nodes[0])

        start_node, _ = await_rebalance_start(ignites)

        new_node = IgniteService(self.test_context, ignites.config._replace(discovery_spi=from_ignite_cluster(ignites)),
                                 num_nodes=1)
        new_node.start()

        await_rebalance_complete(ignites, start_node, cache_count)

        return get_result(new_node.nodes, preload_time, cache_count, entry_count, entry_size)


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
