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
Module provides test coverage for cluster rebalancing during rolling upgrade workflows.
"""
from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.rebalance.persistent_test import await_and_check_rebalance
from ignitetest.tests.rebalance.util import NUM_NODES, BaseRebalanceTest, start_ignite, await_rebalance_start, \
    get_result
from ignitetest.tests.util import preload_data
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import LATEST, DEV_BRANCH, IgniteVersion


class RollingUpgradeRebalanceTest(BaseRebalanceTest):
    """
    Test validates the rebalance workflow and includes the following steps:
        * Start the cluster.
        * Populate data using IgniteClientApp.
        * Trigger a rebalance by joining a node running the target version, and wait for the rebalance to complete.
    """
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(LATEST))
    @defaults(upgrade_version=[str(DEV_BRANCH)], force=[False], backups=[1], cache_count=[1], entry_count=[15_000],
              entry_size=[50_000], preloaders=[1], thread_pool_size=[None], batch_size=[None],
              batches_prefetch_count=[None], throttle=[None])
    def test_in_memory(self, ignite_version, upgrade_version, force, backups, cache_count, entry_count, entry_size,
                       preloaders, thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Tests rebalance in-memory.
        """
        return self._start_rebalance(ignite_version, upgrade_version, force, backups, cache_count, entry_count,
                                   entry_size, preloaders, thread_pool_size, batch_size, batches_prefetch_count,
                                   throttle, False)

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(LATEST))
    @defaults(upgrade_version=[str(DEV_BRANCH)], force=[False], backups=[1], cache_count=[1], entry_count=[5_000],
              entry_size=[50_000], preloaders=[1], thread_pool_size=[None], batch_size=[None],
              batches_prefetch_count=[None], throttle=[None])
    def test_pds(self, ignite_version, upgrade_version, force, backups, cache_count, entry_count, entry_size,
                 preloaders, thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Tests rebalance with persistence.
        """
        return self._start_rebalance(ignite_version, upgrade_version, force, backups, cache_count, entry_count,
                                   entry_size, preloaders, thread_pool_size, batch_size, batches_prefetch_count,
                                   throttle, True)

    def _start_rebalance(self, ignite_version, upgrade_version, force, backups, cache_count, entry_count, entry_size,
                         preloaders, thread_pool_size, batch_size, batches_prefetch_count, throttle, with_persistence):
        """
        Tests rebalance on node join with version upgrade.
        """
        reb_params = self.get_reb_params(thread_pool_size=thread_pool_size, batch_size=batch_size,
                                         batches_prefetch_count=batches_prefetch_count, throttle=throttle,
                                         persistent=with_persistence)

        data_gen_params = self.get_data_gen_params(backups=backups, cache_count=cache_count, entry_count=entry_count,
                                                   entry_size=entry_size, preloaders=preloaders)

        ignites = start_ignite(self.test_context, ignite_version, reb_params, data_gen_params)

        control_utility = ControlUtility(ignites)

        if with_persistence:
            control_utility.activate()

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            data_gen_params=data_gen_params)

        new_node = IgniteService(self.test_context, ignites.config._replace(discovery_spi=from_ignite_cluster(ignites)),
                                 num_nodes=1, modules=reb_params.modules)

        control_utility.enable_rolling_upgrade(IgniteVersion(upgrade_version).vstring, force)

        new_node.config._replace(version=upgrade_version)

        new_node.start()

        if with_persistence:
            control_utility.add_to_baseline(new_node.nodes)

            await_and_check_rebalance(new_node)

            control_utility.deactivate()
        else:
            await_rebalance_start(new_node)

            new_node.await_rebalance()

        return get_result(new_node.nodes, preload_time, cache_count, entry_count, entry_size)
