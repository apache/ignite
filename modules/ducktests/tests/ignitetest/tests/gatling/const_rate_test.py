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
from ducktape.mark import parametrize

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


class ConstantRateSimulationTest(IgniteTest):
    """
    Test gatling service implementation
    """

    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(client_type=IgniteServiceType.NODE,
                 profile="org.apache.ignite.internal.ducktest.gatling.scenario.PutGetProfile",
                 rps=20,
                 duration=10,
                 client_nodes=2)
    def test_simulation_with_fixed_rate(self, ignite_version, client_type, profile, rps, duration, client_nodes):
        """
        Test that GatlingService correctly start and stop both as node and thin client.
        """
        simulation = "org.apache.ignite.internal.ducktest.gatling.simulation.ConstantRpsSimulation"
        cache_templates = [
            CacheConfiguration(name="TEST-CACHE", cache_mode="PARTITIONED", atomicity_mode="TRANSACTIONAL",
                               backups=1, statistics_enabled=True)
        ]
        server_config = IgniteConfiguration(version=IgniteVersion(ignite_version),
                                            client_connector_configuration=ClientConnectorConfiguration(),
                                            caches=cache_templates)

        ignite = IgniteService(self.test_context, server_config, self.available_cluster_size - client_nodes)

        if client_type == IgniteServiceType.THIN_CLIENT:
            addresses = ignite.nodes[0].account.hostname + ":" + str(ignite.config.client_connector_configuration.port)
            client_config = IgniteThinClientConfiguration(addresses=addresses)
        else:
            client_config = ignite.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite))

        gatling_clients = GatlingService(
            self.test_context,
            client_config,
            simulation_class_name=simulation,
            num_nodes=client_nodes,
            jvm_opts=[f"-Dprofile={profile}", f"-Drps={rps}", f"-Dduration={duration}"])

        ignite.start()
        gatling_clients.run()
        ignite.stop()
