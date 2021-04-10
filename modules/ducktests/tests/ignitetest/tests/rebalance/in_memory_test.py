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
Module contains in-memory rebalance tests.
"""

from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.tests.rebalance import NodeJoinLeftScenario
from ignitetest.utils.version import IgniteVersion


# pylint: disable=W0223
class InMemoryTest(NodeJoinLeftScenario):
    """
    Tests rebalance scenarios in in-memory mode.
    """
    DEFAULT_DATA_REGION_SZ = 512 * 1024 * 1024

    # pylint: disable=too-many-arguments, too-many-locals
    def _build_config(self, ignite_version, backups, cache_count, entry_count, entry_size,
                      thread_pool_size, batch_size, batches_prefetch_count, throttle):
        return IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(max_size=max(
                    cache_count * entry_count * entry_size * (backups + 1),
                    self.DEFAULT_DATA_REGION_SZ))),
            metric_exporter="org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi",
            rebalance_thread_pool_size=thread_pool_size,
            rebalance_batch_size=batch_size,
            rebalance_batches_prefetch_count=batches_prefetch_count,
            rebalance_throttle=throttle)
