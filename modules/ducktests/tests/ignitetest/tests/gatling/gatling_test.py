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
This module contains gatling tests
"""

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.gatling.gatling import GatlingService
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster


class GatlingTest(IgniteTest):
    """
    Tests services implementations
    """
    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.gatling.GatlingRunnerApplication"

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    def test_ignite_app_start_stop(self, ignite_version):
        """
        Test that IgniteService and IgniteApplicationService correctly start and stop
        """
        server_config = IgniteConfiguration(version=IgniteVersion(ignite_version),
                                            caches=[CacheConfiguration(name='TEST-CACHE')],
                                            client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, server_config, 1)

        addresses = ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)

        # client_config = IgniteThinClientConfiguration(
        #     addresses=addresses,
        #     version=IgniteVersion(ignite_version))

        client_config = server_config._replace(client_mode=True,
                                               discovery_spi=from_ignite_cluster(ignite))

        gatling_clients = GatlingService(
            self.test_context,
            client_config,
            simulation_class_name="org.apache.ignite.internal.gatling.simulation.BasicSimulation",
            num_nodes=3)

        ignite.start()
        gatling_clients.run()
        ignite.stop()
