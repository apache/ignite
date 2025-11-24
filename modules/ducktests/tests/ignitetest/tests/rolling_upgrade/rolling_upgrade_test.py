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
from ducktape.mark import defaults, matrix

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.tests.rebalance.util import NUM_NODES
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


class RollingUpgradeTest(IgniteTest):
    """
    Tests validates rolling upgrade
    """

    JAVA_CLIENT_CLASS_NAME = ("org.apache.ignite.internal.ducktest.tests.persistence_upgrade_test."
                              "DataLoaderAndCheckerApplication")

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(LATEST))
    @defaults(upgrade_version=[str(DEV_BRANCH)], force=[False])
    @matrix(upgrade_coordinator_first=[True, False])
    def test_rolling_upgrade_in_mem(self, ignite_version, upgrade_version, upgrade_coordinator_first, force):
        self._rolling_upgrade(ignite_version, upgrade_version, upgrade_coordinator_first, force, False)

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(LATEST))
    @defaults(upgrade_version=[str(DEV_BRANCH)], force=[False])
    @matrix(upgrade_coordinator_first=[True, False])
    def test_rolling_upgrade_pds(self, ignite_version, upgrade_version, upgrade_coordinator_first, force):
        self._rolling_upgrade(ignite_version, upgrade_version, upgrade_coordinator_first, force, True)

    def _rolling_upgrade(self, ignite_version, upgrade_version, upgrade_coordinator_first, force, with_persistence):
        self.logger.info(f"Initiating Rolling Upgrade test from {ignite_version} to {upgrade_version} with persistence "
                         f"enabled [{with_persistence}] starting from coordinator [{upgrade_coordinator_first}]")

        ignites = self._start_ignite_cluster_with_data_load(ignite_version, with_persistence)

        control_sh = ControlUtility(ignites)

        if with_persistence:
            control_sh.activate()

        control_sh.enable_rolling_upgrade(IgniteVersion(upgrade_version).vstring, force)

        self._upgrade_ignite_cluster(ignites, upgrade_version, upgrade_coordinator_first)

        control_sh.disable_rolling_upgrade()

        control_sh.idle_verify()

        assert len(ignites.alive_nodes) == self.test_context.expected_num_nodes, 'All nodes should be alive'

        ignites.stop()

    def _start_ignite_cluster_with_data_load(self, ignite_version: str, with_persistence):
        self.logger.info("Cluster start-up with data load.")

        ignite_cfg = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"})

        if with_persistence:
            ignite_cfg = ignite_cfg._replace(data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(persistence_enabled=True)))

        ignites = IgniteApplicationService(
            self.test_context,
            ignite_cfg,
            num_nodes=self.test_context.expected_num_nodes,
            java_class_name=self.JAVA_CLIENT_CLASS_NAME,
            params= {
                "check": False,
                "backups": 1
            })

        ignites.start()

        ignites.await_event(f"Topology snapshot \\[ver={self.test_context.expected_num_nodes}",
                            ignites.startup_timeout_sec, from_the_beginning=True)

        self.logger.info(f"Initial cluster is up [nodes={len(ignites.nodes)}].")

        return ignites

    def _upgrade_ignite_cluster(self, ignites: IgniteApplicationService, upgrade_version: str,
                                upgrade_coordinator_first: bool):
        self.logger.info(f"Starting rolling upgrade with data check for each node start-up.")

        ignites.config = ignites.config._replace(version=IgniteVersion(upgrade_version))

        ignites.params = {"check": True}

        ignite_upgraded = 0

        for ignite in ignites.nodes if upgrade_coordinator_first else reversed(ignites.nodes):
            self.logger.debug(f"Updating {ignites.who_am_i(ignite)}")

            ignites.stop_node(ignite)

            ignites.start_node(ignite)

            ignite_upgraded += 1

            exp_topology = self.test_context.expected_num_nodes + 2 * ignite_upgraded

            ignites.await_event(f"Topology snapshot \\[ver={exp_topology}", ignites.startup_timeout_sec,
                                from_the_beginning=True)

        self.logger.info(f"Upgrade is complete.")
