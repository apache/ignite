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
Module contains in-place rolling upgrade tests
"""
from ducktape.mark import matrix, defaults

from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.tests.rolling_upgrade.util import BaseRollingUpgradeTest, NUM_NODES
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import IgniteVersion, LATEST, DEV_BRANCH


class InPlaceNodeUpgradeTest(BaseRollingUpgradeTest):
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(LATEST))
    @matrix(with_persistence=[True, False], upgrade_coordinator_first=[True, False])
    @defaults(upgrade_version=[str(DEV_BRANCH)], force=[False], backups=[1], entry_count=[15_000])
    def test_in_place_rolling_upgrade(self, ignite_version, upgrade_version, force, with_persistence,
                                      backups, entry_count, upgrade_coordinator_first):
        self.upgrade_coordinator_first = upgrade_coordinator_first

        self.check_rolling_upgrade(ignite_version, upgrade_version, force, with_persistence,
                                   backups, entry_count, self._upgrade_ignite_cluster)

    def _upgrade_ignite_cluster(self, ignites, upgrade_version, force, with_persistence):
        control_sh = ControlUtility(ignites)

        control_sh.enable_rolling_upgrade(IgniteVersion(upgrade_version).vstring, force)

        self.logger.info(
            f"Starting in-place rolling upgrade "
            f"{'with coordinator going first' if self.upgrade_coordinator_first else 'from the last node in the ring'}"
        )

        ignites.config = ignites.config._replace(version=IgniteVersion(upgrade_version))

        for ignite in ignites.nodes if self.upgrade_coordinator_first else reversed(ignites.nodes):
            self.logger.debug(f"Upgrading {ignites.who_am_i(ignite)}")

            ignites.stop_node(ignite)

            ignites.start_node(ignite)

            ignites.await_started([ignite])

        self.logger.info(f"Cluster upgrade is complete.")

        control_sh.disable_rolling_upgrade()

        return [ignites]
