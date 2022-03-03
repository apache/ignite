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
This module contains client tests
"""

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


class ClientERRTest(IgniteTest):
    """
    Test Clients
    """
    NUM_NODES = 4


    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_clients(self, ignite_version):
        """
        Test Clients
        """
        config = IgniteConfiguration(
            cluster_state="INACTIVE",
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(persistent=True))
        )

        servers = IgniteService(self.test_context,
                                config=config,
                                num_nodes=int(self.NUM_NODES/2),
                                startup_timeout_sec=120)

        servers.start()

        ControlUtility(cluster=servers).activate()

        client_cfg = config._replace(client_mode=True)

        app = IgniteApplicationService(
            self.test_context,
            client_cfg,
            num_nodes=int(self.NUM_NODES/2),
            startup_timeout_sec=120,
            java_class_name="org.apache.ignite.internal.ducktest.tests.client_test.IgniteCachePutClient",
            params={"cacheName": "test_cache", "pacing": 10}
        )

        app.start()
        app.stop()

        servers.stop()
