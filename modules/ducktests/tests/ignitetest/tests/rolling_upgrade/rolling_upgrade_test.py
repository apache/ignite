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
from ignitetest.tests.client_test import check_topology
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion

RU_NUM_NODES=3


class RollingUpgradeTest(IgniteTest):
    """
    Tests validates rolling upgrade
    """
    @cluster(num_nodes=RU_NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(init_version=[str(LATEST)], upgrade_version=[str(DEV_BRANCH)])
    @matrix(upgrade_coordinator=[True, False])
    def test_rolling_upgrade(self, ignite_version, init_version, upgrade_version, upgrade_coordinator):
        results = {}

        ignites = self.start_ignite_cluster(init_version, results)

        control_sh = ControlUtility(ignites[0])

        check_topology(control_sh, RU_NUM_NODES)

        self.upgrade_ignite_cluster(ignites, upgrade_version, upgrade_coordinator, results)

        check_topology(control_sh, 3 * RU_NUM_NODES)

        return results

    def start_ignite_cluster(self, ignite_version: str, results):
        self.logger.info("Cluster start-up.")

        ignites = []

        ignite_cfg = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"})

        start = monotonic()

        for i in range(RU_NUM_NODES):
            ignite = IgniteService(self.test_context, ignite_cfg, num_nodes=1)

            ignite.start()

            ignites.append(ignite)

        results['Ignite cluster start time (s)'] = round(monotonic() - start, 1)

        self.logger.info(f"Initial cluster is up [nodes={RU_NUM_NODES}].")

        return ignites

    def upgrade_ignite_cluster(self, ignites: list, ignite_version: str, upgrade_coordinator: bool, results):
        self.logger.info(f"Starting rolling upgrade.")

        nodes_iter = range(RU_NUM_NODES) if upgrade_coordinator else reversed(range(RU_NUM_NODES))

        start = monotonic()

        for i in nodes_iter:
            ignites[i].config = IgniteConfiguration(version=IgniteVersion(ignite_version))

            ignites[i].stop()

            ignites[i].start(clean=False)

        results['Ignite cluster upgrade time (s)'] = round(monotonic() - start, 1)

        self.logger.info(f"Upgrade is complete.")
