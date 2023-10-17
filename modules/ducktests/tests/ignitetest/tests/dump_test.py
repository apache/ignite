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
import re
from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.dump_utility import DumpUtility
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.util import DataGenerationParams, preload_data
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH

DUMP_NAME = "test_dump"
CACHE_NAME = "test-cache"


class DumpTest(IgniteTest):
    """
    Test cache dump.
    """

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

        result = self.get_data_region_size(ignite)

        result.update(self.create_dump(ignite))

        result.update(self.check_dump(ignite))

        return result

    def create_dump(self, ignite):
        dump_utility = DumpUtility(self.test_context, ignite)

        dump_create_time_ms = dump_utility.create(DUMP_NAME)

        dump_size = {}
        for node in ignite.nodes:
            dump_size[node.consistent_id] = IgniteAwareService.get_file_size(
                node, os.path.join(ignite.snapshots_dir, DUMP_NAME))

        return {
            "dump_create_time_ms": dump_create_time_ms,
            "dump_size": dump_size
        }

    @staticmethod
    def check_dump(ignite):
        control_utility = ControlUtility(ignite)

        output = control_utility.snapshot_check(DUMP_NAME)

        pattern = re.compile("Execution time: (?P<time_ms>\\d+) ms")
        match = pattern.search(output)

        return {
            "dump_check_time_ms": int(match.group("time_ms")) if match else None
        }

    @staticmethod
    def get_data_region_size(ignite):
        data_region_size = {}

        for node in ignite.nodes:
            mbean = node.jmx_client().find_mbean('.*group=io.*name="dataregion.default"')
            data_region_size[node.consistent_id] = int(next(mbean.TotalUsedSize))

        return {
            "data_region_size": data_region_size
        }
