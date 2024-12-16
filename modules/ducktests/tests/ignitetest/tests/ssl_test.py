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
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.path import get_shared_root_path
from ignitetest.services.utils.ssl.connector_configuration import ConnectorConfiguration
from ignitetest.services.utils.ssl.ssl_params import SslParams, DEFAULT_SERVER_KEYSTORE, DEFAULT_CLIENT_KEYSTORE, \
    DEFAULT_ADMIN_KEYSTORE
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST


class SslTest(IgniteTest):
    """
    Ssl test.
    """

    @cluster(num_nodes=3)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_ssl_connection(self, ignite_version):
        """
        Test that IgniteService, IgniteApplicationService correctly start and stop with ssl configurations.
        And check ControlUtility with ssl arguments.
        """
        shared_root = get_shared_root_path(self.test_context.globals)

        server_ssl = SslParams(shared_root, key_store_jks=DEFAULT_SERVER_KEYSTORE)

        server_configuration = IgniteConfiguration(
            version=IgniteVersion(ignite_version), ssl_params=server_ssl)

        ignite = IgniteService(self.test_context, server_configuration, num_nodes=2,
                               startup_timeout_sec=180)

        client_configuration = server_configuration._replace(
            client_mode=True,
            ssl_params=SslParams(shared_root, key_store_jks=DEFAULT_CLIENT_KEYSTORE))

        app = IgniteApplicationService(
            self.test_context,
            client_configuration,
            java_class_name="org.apache.ignite.internal.ducktest.tests.smoke_test.SimpleApplication",
            startup_timeout_sec=180)

        admin_ssl = SslParams(shared_root, key_store_jks=DEFAULT_ADMIN_KEYSTORE)
        control_utility = ControlUtility(cluster=ignite, ssl_params=admin_ssl)

        ignite.start()
        app.start()

        control_utility.cluster_state()

        app.stop()
        ignite.stop()
