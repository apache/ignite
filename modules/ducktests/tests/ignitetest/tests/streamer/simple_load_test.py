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
Module contains simple Ignite streamer test.
"""
from ducktape.mark import defaults, parametrize

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration.data_storage import DataStorageConfiguration, \
    DataRegionConfiguration
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.data_loader.data_loader import DataLoader, DataLoadParams, data_region_size
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, LATEST, DEV_BRANCH

NODE_COUNT = 8


class SimpleStreamerTest(IgniteTest):
    """
    Test Streamer.
    """

    @cluster(num_nodes=NODE_COUNT)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[50], preloaders=[2], threads=[4], jvm_opts=[['-Xmx1G']])
    @parametrize(entry_count=100_000, entry_size=1057)
    def just_load_test(self, ignite_version, backups, cache_count, entry_count, entry_size, preloaders, threads,
                       jvm_opts):
        """
        Basic streamer test.
        """
        data_load_params = DataLoadParams(backups=backups, cache_count=cache_count,
                                          entry_count=entry_count, entry_size=entry_size,
                                          preloaders=preloaders, threads=threads,
                                          jvm_opts=jvm_opts)
        loader = DataLoader(self.test_context, data_load_params)

        version = IgniteVersion(ignite_version)

        num_nodes = self.available_cluster_size - preloaders
        assert num_nodes > 0
        region_size = data_region_size(self, int(data_load_params.data_size / num_nodes))

        ignite_config = IgniteConfiguration(
            version=version,
            data_storage=DataStorageConfiguration(
                max_wal_archive_size=2 * region_size,
                default=DataRegionConfiguration(persistent=True,
                                                max_size=region_size)),
            metric_exporter='org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'
        )

        nodes = IgniteService(self.test_context, ignite_config, num_nodes=num_nodes, jvm_opts=['-Xmx1G'])
        nodes.start()

        control_utility = ControlUtility(nodes)
        control_utility.activate()

        loader.load_data(nodes)

        return loader.get_summary_report()
