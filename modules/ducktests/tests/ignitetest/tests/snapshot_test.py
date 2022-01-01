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
import os

from ducktape.mark import defaults
from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.rebalance.util import preload_data, RebalanceParams
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, LATEST_2_10


class SnapshotTest(IgniteTest):
    """
    Test Snapshot.
    """
    SNAPSHOT_NAME = "test_snapshot"

    CACHE_NAME = "TEST_CACHE"

    @cluster(num_nodes=52)
    @ignite_versions(str(LATEST_2_10))
    @defaults(backups=[0], cache_count=[1], entry_count=[2_621_440_000], entry_size=[2_020], partitions_count=[16384],
              preloaders=[20])
    def snapshot_test(self, ignite_version,
                      backups, cache_count, entry_count, entry_size, partitions_count, preloaders):
        """
        Basic snapshot test.
        """
        version = IgniteVersion(ignite_version)

        ignite_config = IgniteConfiguration(
            version=version,
            data_storage=DataStorageConfiguration(default=DataRegionConfiguration(persistent=True)),
            metric_exporter='org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'
        )

        ignite = IgniteService(self.test_context, ignite_config, num_nodes=len(self.test_context.cluster) - preloaders)
        ignite.start()

        control_utility = ControlUtility(ignite)
        control_utility.activate()

        reb_params = RebalanceParams(backups=backups, cache_count=cache_count,
                                     entry_count=entry_count, entry_size=entry_size, partitions_count=partitions_count,
                                     preloaders=preloaders)

        self.logger.info("Start loading data[entry_count={0},entry_size={1},partition_count={2},preloaders={3}]"
                         .format(reb_params.entry_count, reb_params.entry_size, reb_params.partitions_count,
                                 reb_params.preloaders))

        preload_time = preload_data(
            self.test_context,
            ignite_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite)),
            rebalance_params=reb_params,
            timeout=7200)

        self.logger.info("Data preload finished[{0}]".format(preload_time))

        control_utility.validate_indexes()
        control_utility.idle_verify()

        # node = nodes.nodes[0]

        # dump_1 = control_utility.idle_verify_dump(node)

        control_utility.snapshot_create(self.SNAPSHOT_NAME)

        for i in range(0, len(self.test_context.cluster) - preloaders):
            ignite.nodes[i].account.ssh(
                f"ls -alFHhR {ignite.snapshots_dir} >> {os.path.join(ignite.log_dir, 'snapshot_stat.txt')}")
            ignite.nodes[i].account.ssh(
                f"du {ignite.snapshots_dir} >> {os.path.join(ignite.log_dir, 'snapshot_stat.txt')}")

        ignite.stop()
        # nodes.restore_from_snapshot(self.SNAPSHOT_NAME)
        # nodes.start()

        # control_utility.activate()
        # control_utility.validate_indexes()
        # control_utility.idle_verify()

        # dump_3 = control_utility.idle_verify_dump(node)

        # diff = node.account.ssh_output(f'diff {dump_1} {dump_3}', allow_fail=True)
        # assert not diff, diff
