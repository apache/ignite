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
Utils for rolling upgrade tests.
"""
from typing import List

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster, from_ignite_services
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion

NUM_NODES = 4
PRELOADERS_COUNT = 1
JAVA_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.persistence_upgrade_test.DataLoaderAndCheckerApplication"


class BaseRollingUpgradeTest(IgniteTest):
    """
    Base class for rolling upgrade tests on an Ignite cluster.

    This class provides a template method `test_rolling_upgrade` that performs the
    following steps:
        1. Start an Ignite cluster with the given version.
        2. Preload data into the cluster.
        3. Perform a rolling upgrade via a provided upgrade function.
        4. Verify the data integrity after the upgrade.
        5. Ensure all nodes are alive and stop the cluster.
    """
    def check_rolling_upgrade(self, ignite_version, upgrade_version, force, with_persistence, backups, entry_count,
                              upgrade_func):
        """
        Template test for performing a rolling upgrade.

        :param ignite_version: Version of Ignite to start the cluster with.
        :param upgrade_version: Version to upgrade the cluster nodes to.
        :param force: Whether to force the upgrade.
        :param with_persistence: Enable persistence for the cluster nodes.
        :param backups: Number of backup copies for each cache.
        :param entry_count: Number of entries per cache.
        :param upgrade_func: Function performing the rolling upgrade on the cluster.
                             Must accept the following signature:
                             `(cluster: IgniteService, upgrade_version, force, with_persistence) -> List[IgniteService]`
        """
        self.logger.info(
            f"Initiating Rolling Upgrade test from {ignite_version} to {upgrade_version} "
            f"with {'persistent' if with_persistence else 'in-memory'} mode"
        )

        node_count = self.test_context.expected_num_nodes - PRELOADERS_COUNT

        ignites = self._start_ignite_cluster(ignite_version, node_count, with_persistence)

        app = self._configure_data_handler_app(ignites, backups, entry_count)

        self._preload_data(app)

        upgraded_ignites = upgrade_func(ignites, upgrade_version, force, with_persistence)

        self._check_data(upgraded_ignites, app)

        total_alive = sum(len(ignite.alive_nodes) for ignite in upgraded_ignites)

        assert total_alive == node_count, f"All nodes should be alive [expected={node_count}, actual={total_alive}]"

        for ignite in upgraded_ignites:
            ignite.stop()

    def _start_ignite_cluster(self, ignite_version, node_count, with_persistence):
        ignite_cfg = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"})

        if with_persistence:
            ignite_cfg = ignite_cfg._replace(data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(persistence_enabled=True)))

        ignites = IgniteService(self.test_context, ignite_cfg, num_nodes=node_count)

        ignites.start()

        self.logger.debug(f"Initial cluster is up [nodes={len(ignites.nodes)}].")

        control_sh = ControlUtility(ignites)

        if with_persistence:
            control_sh.activate()

        return ignites

    def _configure_data_handler_app(self, ignites, backups, entry_count):
        return IgniteApplicationService(
            self.test_context,
            config=ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            java_class_name=JAVA_CLASS_NAME,
            params={
                "backups": backups,
                "entryCount": entry_count
            })

    def _preload_data(self, app: IgniteApplicationService):
        app.params["check"] = False

        app.start()
        app.stop()

        self.logger.debug("Data generation is done.")

    def _check_data(self, upgraded_ignites: List[IgniteService], app: IgniteApplicationService):
        assert len(upgraded_ignites) > 0, "Upgraded cluster is empty!"

        app.config = app.config._replace(discovery_spi=from_ignite_services(upgraded_ignites))

        app.params["check"] = True

        app.start()
        app.stop()

        self.logger.debug("Data check is complete.")
