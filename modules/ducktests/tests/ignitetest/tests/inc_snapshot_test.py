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
Module contains incremental snapshot test.
"""
import time

from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.util import DataGenerationParams, load_data
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH
from ignitetest.utils import cluster


class IncrementalSnapshotTest(IgniteTest):
    """
    Test base snapshot for incremental snapshots.
    """
    SNAPSHOT_NAME = "test_base_snapshot"

    @cluster(num_nodes=8)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(backups=[2], inc_create_period_sec=[60], inc_count=[5], loaders_count=[4])
    def incremental_snapshot_test(self, ignite_version, backups, inc_create_period_sec, inc_count, loaders_count):
        """
        Incremental snapshot test - run incremental snapshots concurrently with transactional load.
        :param ignite_version: Ignite version.
        :param backups: Number of cache backups.
        :param inc_create_period_sec: Period between creating incremental snapshots.
        :param inc_count: Incremental snapshots count.
        :param loaders_count: Transactional loaders count.
        :return: Incremental snapshots restoring statistics.
        """
        version = IgniteVersion(ignite_version)

        ignite_config = IgniteConfiguration(
            version=version,
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(
                    persistence_enabled=True
                ),
                wal_compaction_enabled=True
            ),
            metric_exporters={'org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'}
        )

        nodes = IgniteService(self.test_context, ignite_config, num_nodes=self.available_cluster_size - loaders_count)
        nodes.start()

        control_utility = ControlUtility(nodes)
        control_utility.activate()

        # Concurrently start transactional load and create incremental snapshots.
        apps = load_data(
            self.test_context,
            nodes.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(nodes)),
            DataGenerationParams(
                backups=backups,
                cache_count=1,
                entry_count=1_000_000_000,
                entry_size=100,
                index_count=1,
                preloaders=loaders_count,
                transactional=True))

        # TODO: preload should fix it.
        # Await all caches are created.
        for app in apps:
            app.await_started()

        control_utility.snapshot_create(self.SNAPSHOT_NAME)

        for i in range(1, inc_count + 1):
            control_utility.incremental_snapshot_create(self.SNAPSHOT_NAME, i)

            time.sleep(inc_create_period_sec)

        for app in apps:
            app.stop()

        inc_snp_restore_stat = {}

        for i in range(1, inc_count + 1):
            control_utility.destroy_caches(destroy_all=True)

            inc_snp_restore_stat[i] = control_utility.incremental_snapshot_restore(self.SNAPSHOT_NAME, i)

            control_utility.validate_indexes()
            control_utility.idle_verify()

        return inc_snp_restore_stat
