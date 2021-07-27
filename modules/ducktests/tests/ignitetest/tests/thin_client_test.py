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


# pylint: disable=W0223
class ThinClientTest(IgniteTest):
    """
    cluster - cluster size.
    JAVA_CLIENT_CLASS_NAME - running classname.
    to use with ssl enabled:
    export GLOBALS='{"ssl":{"enabled":true}}' .
    """

    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientConnectAndWaitClient"

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

        addresses = ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)

        thin_clients = IgniteApplicationService(self.test_context,
                                                IgniteThinClientConfiguration(addresses=addresses,
                                                                              version=IgniteVersion(
                                                                                  thin_client_version)),
                                                java_class_name=self.JAVA_CLIENT_CLASS_NAME,
                                                num_nodes=1)

        ignite.start()
        thin_clients.run()
        ignite.stop()

    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH))
    def test_thin_client_max_reconnect(self, ignite_version):
        """
        Thin client compatibility test.
        """

        server_config = IgniteConfiguration(version=IgniteVersion(ignite_version),
                                            data_storage=DataStorageConfiguration(
                                                default=DataRegionConfiguration(persistent=True)),
                                            client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, server_config, 2)

        addresses = ignite.nodes[1].account.hostname + ":" + str(server_config.client_connector_configuration.port)

        thin_clients = IgniteApplicationService(self.test_context,
                                                IgniteThinClientConfiguration(addresses=addresses,
                                                                              version=IgniteVersion(
                                                                                  ignite_version)),
                                                java_class_name=self.JAVA_CLIENT_CLASS_NAME,
                                                num_nodes=10,
                                                startup_timeout_sec=60)

        ignite.start()
        ControlUtility(cluster=ignite).activate()

        # ignite.freeze_node(ignite.nodes[0])
        for i in range(20):
            thin_clients.start_async()
            thin_clients.await_started()
            thin_clients.stop()

        # time.sleep(3)

        # ignite.unfreeze_node(ignite.nodes[0])

        # thin_clients.await_started()

        ControlUtility(cluster=ignite).baseline()

        ControlUtility(cluster=ignite).deactivate()

        ignite.stop()

