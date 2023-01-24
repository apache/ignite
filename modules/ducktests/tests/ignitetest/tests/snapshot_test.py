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
Module contains snapshot test.
"""

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, LATEST, DEV_BRANCH
from ignitetest.utils import cluster


class SnapshotTest(IgniteTest):
    """
    Test Snapshot.
    """
    SNAPSHOT_NAME = "test_snapshot"

    CACHE_NAME = "TEST_CACHE"

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def snapshot_test(self, ignite_version):
        """
        Basic snapshot test.
        """
        version = IgniteVersion(ignite_version)

        ignite_config = IgniteConfiguration(
            version=version,
            data_storage=DataStorageConfiguration(default=DataRegionConfiguration(persistence_enabled=True)),
            metric_exporters={'org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'}
        )

        nodes = IgniteService(self.test_context, ignite_config, num_nodes=self.available_cluster_size - 1)
        nodes.start()

        control_utility = ControlUtility(nodes)
        control_utility.activate()

        loader_config = IgniteConfiguration(client_mode=True, version=version, discovery_spi=from_ignite_cluster(nodes))

        loader = IgniteApplicationService(
            self.test_context,
            loader_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.snapshot_test.DataLoaderApplication",
            params={"start": 0, "cacheName": self.CACHE_NAME, "interval": 500_000, "valueSizeKb": 1}
        )

        loader.run()

        control_utility.validate_indexes()
        control_utility.idle_verify()

        node = nodes.nodes[0]

        dump_1 = control_utility.idle_verify_dump(node)

        control_utility.snapshot_create(self.SNAPSHOT_NAME)

        loader.params = {"start": 500_000, "cacheName": self.CACHE_NAME, "interval": 100_000, "valueSizeKb": 1}
        loader.run()

        dump_2 = control_utility.idle_verify_dump(node)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}', allow_fail=True)
        assert diff

        nodes.stop()
        nodes.restore_from_snapshot(self.SNAPSHOT_NAME)
        nodes.start(clean=False)

        control_utility.activate()
        control_utility.validate_indexes()
        control_utility.idle_verify()

        dump_3 = control_utility.idle_verify_dump(node)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_3}', allow_fail=True)
        assert not diff, diff
