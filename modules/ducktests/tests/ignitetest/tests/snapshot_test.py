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

from ducktape.mark import defaults
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, LATEST, DEV_BRANCH
from ignitetest.utils import cluster
from ignitetest.utils.data_loader.data_loader import DataLoader, DataLoadParams


class SnapshotTest(IgniteTest):
    """
    Test Snapshot.
    """
    SNAPSHOT_NAME = "test_snapshot"

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[600_000], entry_size=[1024], preloaders=[1])
    def snapshot_test(self, ignite_version, backups, cache_count, entry_count, entry_size, preloaders):
        """
        Basic snapshot test.
        """
        data_load_params = DataLoadParams(backups=backups, cache_count=cache_count,
                                          entry_count=entry_count, entry_size=entry_size, preloaders=preloaders)
        loader = DataLoader(self.test_context, data_load_params)

        version = IgniteVersion(ignite_version)

        ignite_config = IgniteConfiguration(
            version=version,
            data_storage=DataStorageConfiguration(
                max_wal_archive_size=2 * data_load_params.data_region_max_size,
                default=DataRegionConfiguration(persistent=True,
                                                max_size=data_load_params.data_region_max_size)),
            metric_exporter='org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'
        )

        nodes = IgniteService(self.test_context, ignite_config, num_nodes=self.available_cluster_size - preloaders)
        nodes.start()        

        control_utility = ControlUtility(nodes)
        control_utility.activate()

        loader.load_data(nodes, from_key=0, to_key=entry_count / 6 * 5)

        control_utility.validate_indexes()
        control_utility.idle_verify()

        node = nodes.nodes[0]

        dump_1 = control_utility.idle_verify_dump(node)

        control_utility.snapshot_create(self.SNAPSHOT_NAME)

        loader.load_data(nodes, from_key=entry_count / 6 * 5, to_key=entry_count)

        dump_2 = control_utility.idle_verify_dump(node)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}', allow_fail=True)
        assert diff

        nodes.stop()
        nodes.restore_from_snapshot(self.SNAPSHOT_NAME)
        nodes.start()

        control_utility.activate()
        control_utility.validate_indexes()
        control_utility.idle_verify()

        dump_3 = control_utility.idle_verify_dump(node)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_3}', allow_fail=True)
        assert not diff, diff
