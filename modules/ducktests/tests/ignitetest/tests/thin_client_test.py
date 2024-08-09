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
This module contains client tests.
"""
import time
from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration, \
    DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


class ThinClientTest(IgniteTest):
    """
    cluster - cluster size.
    JAVA_CLIENT_CLASS_NAME - running classname.
    to use with ssl enabled:
    export GLOBALS='{"ssl":{"enabled":true}}' .
    """

    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientSelfTestApplication"

    JAVA_CLIENT_CLASS_NAME_CON = \
        "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientContiniusApplication"

    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="server_version")
    @ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="thin_client_version")
    def test_thin_client_compatibility(self, server_version, thin_client_version):
        """
        Thin client compatibility test.
        """

        server_config = IgniteConfiguration(version=IgniteVersion(server_version),
                                            client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, server_config, 1)

        addresses = [ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)]

        thin_clients = IgniteApplicationService(self.test_context,
                                                IgniteThinClientConfiguration(addresses=addresses,
                                                                              version=IgniteVersion(
                                                                                  thin_client_version)),
                                                java_class_name=self.JAVA_CLIENT_CLASS_NAME,
                                                num_nodes=1)

        ignite.start()
        thin_clients.run()
        ignite.stop()

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(test_params=[{"connectClients": 150, "putClients": 0, "putAllClients": 0, "runTime": 86400, "freeze": True},
                         {"connectClients": 150, "putClients": 0, "putAllClients": 0, "runTime": 86400, "freeze": False},
                         {"connectClients": 150, "putClients": 2, "putAllClients": 0, "runTime": 86400, "freeze": False},
                         {"connectClients": 150, "putClients": 0, "putAllClients": 2, "runTime": 86400, "freeze": False}])
    def test_thin_client_connect_time(self, ignite_version, test_params):
        """
        Thin client connect time test.
        Determine how thin client connection time depend on cluster load
        Demonstrate 4 situations:
        - Cluster have too much clients
        - Cluster have many put's
        - Cluster have some putAll jobs
        - One node hung for some time
        """

        server_config = \
            IgniteConfiguration(version=IgniteVersion(ignite_version),
                                data_storage=DataStorageConfiguration(default=DataRegionConfiguration(persistent=True)),
                                client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, server_config, 2)

        addresses = ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)

        thin_clients = IgniteApplicationService(self.test_context,
                                                IgniteThinClientConfiguration(addresses=addresses,
                                                                              version=IgniteVersion(
                                                                                  ignite_version)),
                                                java_class_name=self.JAVA_CLIENT_CLASS_NAME_CON,
                                                params={"connectClients": test_params["connectClients"],
                                                        "putClients": test_params["putClients"],
                                                        "putAllClients": test_params["putAllClients"],
                                                        "runTime": test_params["runTime"]},
                                                num_nodes=2)

        ignite.start()

        ControlUtility(cluster=ignite).activate()

        thin_clients.start_async()

        if test_params["freeze"]:
            thin_clients.await_event("START WAITING", 120, True)
            for _ in range(7200):
                ignite.freeze_node(ignite.nodes[0])
                time.sleep(3)
                ignite.unfreeze_node(ignite.nodes[0])
                time.sleep(7)

        thin_clients.await_event("IGNITE_APPLICATION_FINISHED", 90000)

        thin_clients.stop(force_stop=True)

        ignite.stop()
