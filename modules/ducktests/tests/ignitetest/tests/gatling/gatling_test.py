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
from ducktape.mark import matrix, parametrize
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.services.gatling.gatling import GatlingService
from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster


class GatlingTest(IgniteTest):
    """
    Test gatling service implementation
    """
    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(client_type=[IgniteServiceType.NODE],
            simulation=["org.apache.ignite.internal.gatling.simulation.SimulationInvoke"])
    # @matrix(client_type=[
    #     IgniteServiceType.THIN_CLIENT,
    #     IgniteServiceType.NODE
    # ],
    #     simulation=["org.apache.ignite.internal.gatling.simulation.SimulationTransactions"])
    def test_gatling_app_start_stop(self, ignite_version, client_type, simulation):
        """
        Test that GatlingService correctly start and stop both as node and thin client.
        """
        server_config = IgniteConfiguration(version=IgniteVersion(ignite_version),
                                            client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, server_config, 3)

        if client_type == IgniteServiceType.THIN_CLIENT:
            addresses = ignite.nodes[0].account.hostname + ":" + str(ignite.config.client_connector_configuration.port)
            client_config = IgniteThinClientConfiguration(addresses=addresses)
        else:
            client_config = ignite.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite))

        gatling_clients = GatlingService(
            self.test_context,
            client_config,
            # simulation_class_name="org.apache.ignite.gatling.examples.TransactionSimulation",
            simulation_class_name=simulation,
            num_nodes=3)

        ignite.start()
        gatling_clients.run()
        ignite.stop()
