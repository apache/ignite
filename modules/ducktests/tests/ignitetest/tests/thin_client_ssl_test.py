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
This module contains ssl tests.
"""
from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.services.utils.ssl.connector_configuration import ConnectorConfiguration
from ignitetest.services.utils.ssl.ssl_params import SslParams, DEFAULT_SERVER_KEYSTORE, DEFAULT_CLIENT_KEYSTORE, \
    DEFAULT_ADMIN_KEYSTORE
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST


# pylint: disable=W0223
class ThinClientSslTest(IgniteTest):
    """
    cluster - cluster size.
    JAVA_CLIENT_CLASS_NAME - running classname.
    """

    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientSelfTestApplication"

    @cluster(num_nodes=2)
    @matrix(server_version=[str(DEV_BRANCH), str(LATEST)], thin_client_version=[str(DEV_BRANCH), str(LATEST)])
    def test_ssl_connection(self, server_version, thin_client_version):
        """
        Thin client ssh connection test.
        """
        root_dir = self.test_context.globals.get("install_root", "/opt")

        server_ssl = SslParams(root_dir=root_dir, key_store_jks=DEFAULT_SERVER_KEYSTORE)

        server_config = IgniteConfiguration(
            version=IgniteVersion(server_version), ssl_params=server_ssl,
            connector_configuration=ConnectorConfiguration(ssl_enabled=True, ssl_params=server_ssl),
            client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, server_config, 1)

        addresses = ignite.nodes[0].account.hostname + ":" + \
                    str(server_config.client_connector_configuration.port)

        thin_clients = IgniteApplicationService(
            self.test_context,
            IgniteThinClientConfiguration(addresses=addresses,
                                          version=IgniteVersion(thin_client_version),
                                          ssl_params=SslParams(root_dir=root_dir,
                                                               key_store_jks=
                                                               DEFAULT_CLIENT_KEYSTORE,
                                                               user='client',
                                                               password='123456')),
            java_class_name=self.JAVA_CLIENT_CLASS_NAME)

        ignite.start()
        thin_clients.run()
        ignite.stop()
