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
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.util import preload_data, DataGenerationParams
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH
from ignitetest.utils import cluster


class IncrementalSnapshotTest(IgniteTest):
    """
    Test base snapshot for incremental snapshots.
    """
    SNAPSHOT_NAME = "test_base_snapshot"

    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(backups=[2], inc_create_period_sec=[15], inc_count=[4], loaders_count=[4],
              entry_size=[1024], preload_count=[10_000])
    def incremental_snapshot_test(self, ignite_version, backups, inc_create_period_sec, inc_count, loaders_count,
                                  entry_size, preload_count):
        """
        Incremental snapshot test - run incremental snapshots concurrently with transactional load.
        :param ignite_version: Ignite version.
        :param backups: Number of cache backups.
        :param inc_create_period_sec: Period between creating incremental snapshots.
        :param inc_count: Incremental snapshots count.
        :param loaders_count: Data loaders count.
        :param entry_size: Cache entry value size.
        :param preload_count: Count of preloaded entries in full snapshot.
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

        nodes = IgniteService(self.test_context,
                              ignite_config,
                              num_nodes=self.available_cluster_size - 2 * loaders_count)
        nodes.start()

        control_utility = ControlUtility(nodes)
        control_utility.activate()

        preload_time = preload_data(
            self.test_context,
            nodes.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(nodes)),
            data_gen_params=DataGenerationParams(
                backups=backups,
                cache_count=1,
                entry_count=preload_count,
                entry_size=entry_size,
                preloaders=loaders_count,
                index_count=1,
                transactional=True
            ))

        print("Preload time = " + str(preload_time))

        loaders = []

        for _ in range(loaders_count):
            app = IgniteApplicationService(
                self.test_context,
                nodes.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(nodes)),
                java_class_name="org.apache.ignite.internal.ducktest.tests.ContinuousDataGenerationApplication",
                params={
                    "cacheCount": 1
                })

            app.start_async()
            loaders.append(app)

        control_utility.snapshot_create(self.SNAPSHOT_NAME)

        for i in range(1, inc_count + 1):
            control_utility.snapshot_create(self.SNAPSHOT_NAME, incremental=True)

            time.sleep(inc_create_period_sec)

        for app in loaders:
            app.stop()

        result = []

        for i in range(1, inc_count + 1):
            control_utility.destroy_caches(destroy_all=True)

            check_time = control_utility.snapshot_check(self.SNAPSHOT_NAME, i)

            restore_stat = control_utility.snapshot_restore(self.SNAPSHOT_NAME, i)

            result.append({
                "increment": i,
                "checkTimeSec": check_time,
                "restoreStat": restore_stat
            })

            control_utility.validate_indexes()
            control_utility.idle_verify()

        return result
