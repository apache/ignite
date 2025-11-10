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
Module contains rolling upgrade tests
"""
from time import monotonic

from ducktape.mark import defaults, matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.tests.rebalance.util import NUM_NODES
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


class RollingUpgradeTest(IgniteTest):
    """
    Tests validates rolling upgrade
    """
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(init_version=[str(LATEST)], upgrade_version=[str(DEV_BRANCH)])
    @matrix(upgrade_coordinator=[True, False])
    def test_rolling_upgrade(self, ignite_version, init_version, upgrade_version, upgrade_coordinator: bool):
        self.logger.info(f"Initiating Rolling Upgrade test from {init_version} to {upgrade_version} "
                         f"starting from coordinator [{upgrade_coordinator}]")

        results = {}

        ignites = self.start_ignite_cluster(init_version, results)

        control_sh = ControlUtility(ignites)

        control_sh.enable_rolling_upgrade(upgrade_version)

        self.upgrade_ignite_cluster(ignites, upgrade_version, upgrade_coordinator, results)

        control_sh.disable_rolling_upgrade()

        ignites.stop()

        return results

    def start_ignite_cluster(self, ignite_version: str, results):
        self.logger.info("Cluster start-up.")

        ignite_cfg = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"})

        start = monotonic()

        ignites = IgniteService(self.test_context, ignite_cfg, num_nodes=self.test_context.expected_num_nodes)

        ignites.start()

        results['Ignite cluster start time (s)'] = round(monotonic() - start, 1)

        ignites.await_event(f"Topology snapshot \\[ver={self.test_context.expected_num_nodes}",
                            ignites.startup_timeout_sec, from_the_beginning=True)

        self.logger.info(f"Initial cluster is up [nodes={self.test_context.expected_num_nodes}].")

        return ignites

    def upgrade_ignite_cluster(self, ignites: IgniteService, upgrade_version: str, upgrade_coordinator: bool, results):
        self.logger.info(f"Starting rolling upgrade.")

        ignites.config = IgniteConfiguration(version=IgniteVersion(upgrade_version))

        ignite_upgraded = 0

        start = monotonic()

        for ignite in ignites.nodes if upgrade_coordinator else reversed(ignites.nodes):
            self.logger.debug(f"Updating {ignites.who_am_i(ignite)}")

            ignites.stop_node(ignite)

            ignites.start_node(ignite, clean=False)

            ignite_upgraded += 1

            exp_topology = self.test_context.expected_num_nodes + 2 * ignite_upgraded

            ignites.await_event(f"Topology snapshot \\[ver={exp_topology}", ignites.startup_timeout_sec,
                                from_the_beginning=True)

        results['Ignite cluster upgrade time (s)'] = round(monotonic() - start, 1)

        self.logger.info(f"Upgrade is complete.")
