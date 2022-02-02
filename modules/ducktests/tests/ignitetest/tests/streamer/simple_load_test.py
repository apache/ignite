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
from ducktape.mark.resource import cluster

from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import ignite_versions
from ignitetest.utils.data_loader.data_loader import DataLoader, DataLoadParams
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, LATEST, DEV_BRANCH

NODE_COUNT = 48
MAX_DATA_SEGMENT = 10_000_000_000
# NODE_COUNT = 4
# MAX_DATA_SEGMENT = 100_000_000

class SimpleStreamerTest(IgniteTest):
    """
    Test Streamer.
    """

    @cluster(num_nodes=NODE_COUNT - 1)
    # @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @ignite_versions(str(DEV_BRANCH))
    @defaults(backups=[1], cache_count=[50], preloaders=[1, 3], threads=[4, 16, 64, 128],
              jvm_opts=[['-Xmx10G'], ['-Xmx5G'], ['-Xmx2G']])
    @parametrize(entry_count=int((NODE_COUNT - 4) * MAX_DATA_SEGMENT / (1.2 * 50 * 2 * 133)), entry_size=133)
    @parametrize(entry_count=int((NODE_COUNT - 4) * MAX_DATA_SEGMENT / (1.2 * 50 * 2 * 1057)), entry_size=1057)
    @parametrize(entry_count=int((NODE_COUNT - 4) * MAX_DATA_SEGMENT / (1.2 * 50 * 2 * 5047)), entry_size=5047)
    # @defaults(backups=[1], cache_count=[10], preloaders=[1], threads=[2], jvm_opts=['-Xmx256m'])
    # @parametrize(entry_count=int((NODE_COUNT - 2) * MAX_DATA_SEGMENT * 0.8 / 50 / 2 / 133), entry_size=133)
    # @parametrize(entry_count=int((NODE_COUNT - 2) * MAX_DATA_SEGMENT * 0.8 / 50 / 2 / 1057), entry_size=1057)
    # @parametrize(entry_count=int((NODE_COUNT - 2) * MAX_DATA_SEGMENT * 0.8 / 50 / 2 / 5047), entry_size=5047)
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

        ignite_config = IgniteConfiguration(
            version=version,
            metric_exporter='org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'
        )

        nodes = loader.start_ignite(ignite_config)

        control_utility = ControlUtility(nodes)
        control_utility.activate()

        loader.load_data(nodes)

        return loader.get_summary_report()
