
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
This module contains transactions manipulation test through control utility.
"""

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility, ControlUtilityError
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteClientConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import version_if, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST_2_8, IgniteVersion, LATEST_2_7, V_2_8_0


class TransactionsTests(IgniteTest):
    NUM_NODES = 4
    CACHE_NAME = "TEST"

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    def test_print_tx(self, ignite_version):
        servers = self.__start_ignite_nodes(ignite_version, self.NUM_NODES - 2)

        long_tx_params = {
            'config': IgniteClientConfiguration(version=IgniteVersion(ignite_version),
                                                discovery_spi=from_ignite_cluster(servers)),

            'java_class_name': 'org.apache.ignite.internal.ducktest.tests.control_utility.LongRunningTransaction',
            'params': {'cacheName': self.CACHE_NAME, 'numTx': 2, 'keyPrefix': "TX_1_KEY"}
        }

        long_tx_1 = IgniteApplicationService(self.test_context, **long_tx_params)
        long_tx_1.start()

        # long_tx_params.update({'params': {'cacheName': self.CACHE_NAME, 'numTx': 10, 'keyPrefix': "TX_2_KEY"}})
        # long_tx_2 = IgniteApplicationService(self.test_context, **long_tx_params)
        # long_tx_2.start()

        control_utility = ControlUtility(servers, self.test_context)

        return control_utility.tx_info()

    def __start_ignite_nodes(self, version, num_nodes, timeout_sec=60):
        config = IgniteConfiguration(
            cluster_state="ACTIVE",
            version=IgniteVersion(version),
            caches=[CacheConfiguration(name=self.CACHE_NAME, atomicity_mode='TRANSACTIONAL')]
        )

        servers = IgniteService(self.test_context, config=config, num_nodes=num_nodes)

        servers.start(timeout_sec=timeout_sec)

        return servers

