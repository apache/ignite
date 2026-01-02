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
Module contains rolling upgrade tests with new nodes introduced into the topology and older nodes gracefully removed.
"""
from ducktape.mark import defaults, matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_services
from ignitetest.tests.rebalance.persistent_test import await_and_check_rebalance
from ignitetest.tests.rolling_upgrade.util import BaseRollingUpgradeTest, PRELOADERS_COUNT, NUM_NODES
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import LATEST, DEV_BRANCH, IgniteVersion


class AddRemoveNodeUpgradeTest(BaseRollingUpgradeTest):

    @cluster(num_nodes=2 * NUM_NODES + PRELOADERS_COUNT)
    @ignite_versions(str(LATEST))
    @matrix(with_persistence=[True, False])
    @defaults(upgrade_version=[str(DEV_BRANCH)], force=[False], backups=[1], entry_count=[15_000])
    def test_add_remove_rolling_upgrade(self, ignite_version, upgrade_version, force, with_persistence,
                                        backups, entry_count):
        node_count = (self.test_context.expected_num_nodes - PRELOADERS_COUNT) // 2

        self.check_rolling_upgrade(ignite_version, upgrade_version, force, with_persistence,
                                   backups, entry_count, self._upgrade_ignite_cluster, node_count)

    def _upgrade_ignite_cluster(self, ignites, upgrade_version, force, with_persistence):
        control_sh = ControlUtility(ignites)

        control_sh.enable_rolling_upgrade(IgniteVersion(upgrade_version).vstring, force)

        self.logger.info("Starting rolling upgrade.")

        upgraded_nodes = []

        for ignite in ignites.nodes:
            new_node_cfg = ignites.config._replace(
                version=IgniteVersion(upgrade_version),
                discovery_spi=from_ignite_services([ignites] + upgraded_nodes)
            )

            new_node = IgniteService(self.test_context, new_node_cfg, num_nodes=1)

            new_node.start()

            control_sh = ControlUtility(new_node)

            if with_persistence:
                control_sh.add_to_baseline(new_node.nodes)

            await_and_check_rebalance(new_node)

            upgraded_nodes.append(new_node)

            ignites.stop_node(ignite)

            if with_persistence:
                control_sh.remove_from_baseline([ignite])

        self.logger.info("Cluster upgrade is complete.")

        control_sh.disable_rolling_upgrade()

        return upgraded_nodes
