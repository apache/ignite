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
This module contains simple gatling simulation tests.
"""
from ducktape.mark import parametrize, defaults

from ignitetest.services.gatling.gatling import GatlingService
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class GatlingTest(IgniteTest):
    """
    Test gatling service implementation
    """

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(client_type=[IgniteServiceType.NODE, IgniteServiceType.THIN_CLIENT])
    @parametrize(simulation="org.apache.ignite.internal.ducktest.gatling.simulation.BasicSimulation")
    def basic_gatling_test(self, ignite_version, client_type, simulation):
        self._do_gatling(ignite_version, client_type, simulation)

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(client_type=[IgniteServiceType.NODE, IgniteServiceType.THIN_CLIENT])
    @parametrize(simulation="org.apache.ignite.internal.ducktest.gatling.simulation.TransactionsSimulation")
    def transactions_gatling_test(self, ignite_version, client_type, simulation):
        self._do_gatling(ignite_version, client_type, simulation)

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(client_type=[IgniteServiceType.NODE])
    @parametrize(simulation="org.apache.ignite.internal.ducktest.gatling.simulation.LockedInvokeSimulation")
    def invoke_gatling_test(self, ignite_version, client_type, simulation):
        self._do_gatling(ignite_version, client_type, simulation)

    def _do_gatling(self, ignite_version, client_type, simulation):
        """
        Test that GatlingService correctly executes simulations.
        """
        cache_templates = [
            CacheConfiguration(name="TEST-CACHE", cache_mode="PARTITIONED", atomicity_mode="TRANSACTIONAL",
                               backups=1, statistics_enabled=True)
        ]
        server_config = IgniteConfiguration(version=IgniteVersion(ignite_version),
                                            client_connector_configuration=ClientConnectorConfiguration(),
                                            caches=cache_templates)

        ignite = IgniteService(self.test_context, server_config, 2)

        if client_type == IgniteServiceType.THIN_CLIENT:
            addresses = ignite.nodes[0].account.hostname + ":" + str(ignite.config.client_connector_configuration.port)
            client_config = IgniteThinClientConfiguration(addresses=addresses)
        else:
            client_config = ignite.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite))

        gatling_clients = GatlingService(
            self.test_context,
            client_config,
            params={"simulation": simulation},
            num_nodes=2)

        ignite.start()
        gatling_clients.run()
        ignite.stop()
