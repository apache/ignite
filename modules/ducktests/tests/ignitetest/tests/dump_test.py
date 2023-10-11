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
Module contains cache dump tests.
"""
import os
import sys
from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.dump_utility import DumpUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.util import DataGenerationParams, preload_data
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH


class DumpTest(IgniteTest):
    """
    Test cache dump.
    """
    DUMP_NAME = "test_dump"
    CACHE_NAME = "test-cache"

    @cluster(num_nodes=5)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(nodes=[1, 3], backups=[1], cache_count=[1], entry_count=[50_000], entry_size=[1024], preloaders=[1])
    def dump_test(self, ignite_version, nodes, backups, cache_count, entry_count, entry_size, preloaders):
        """
        Basic dump test.
        """
        data_gen_params = DataGenerationParams(backups=backups, cache_count=cache_count, entry_count=entry_count,
                                               entry_size=entry_size, preloaders=preloaders)

        ignite_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                checkpoint_frequency=5000,
                default=DataRegionConfiguration(
                    persistence_enabled=True,
                    max_size=data_gen_params.data_region_max_size
                )
            ),
            metric_exporters={'org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'}
        )

        ignite = IgniteService(self.test_context, ignite_config, num_nodes=nodes)
        ignite.start()

        control_utility = ControlUtility(ignite)
        control_utility.activate()

        preload_data(
            self.test_context,
            ignite.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite)),
            data_gen_params=data_gen_params)

        dump_utility = DumpUtility(self.test_context, ignite)

        dump_utility.create(self.DUMP_NAME)

        control_utility.snapshot_check(self.DUMP_NAME)

        ignite.stop()

    @cluster(num_nodes=5)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(nodes=[3], backups=[1], cache_count=[1], entry_count=[50_000], entry_size=[1024], preloaders=[1])
    def dump_after_datastreamer_and_restart_test(self, ignite_version, nodes, backups, cache_count, entry_count,
                                                 entry_size, preloaders):
        """
        Test that entries loaded via the data streamer are dumped after the ignite restart.
        """
        data_gen_params = DataGenerationParams(backups=backups, cache_count=cache_count, entry_count=entry_count,
                                               entry_size=entry_size, preloaders=preloaders)

        ignite_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                checkpoint_frequency=5000,
                default=DataRegionConfiguration(
                    persistence_enabled=True,
                    max_size=data_gen_params.data_region_max_size
                )
            ),
            metric_exporters={'org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'}
        )

        ignite = IgniteService(self.test_context, ignite_config, num_nodes=nodes)
        ignite.start()

        control_utility = ControlUtility(ignite)
        control_utility.activate()

        preload_data(
            self.test_context,
            ignite.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite)),
            data_gen_params=data_gen_params)

        control_utility.deactivate()

        ignite.stop()

        ignite.start(clean=False)

        dump_utility = DumpUtility(self.test_context, ignite)

        dump_utility.create(self.DUMP_NAME)

        control_utility.snapshot_check(self.DUMP_NAME)

        ignite.stop()
