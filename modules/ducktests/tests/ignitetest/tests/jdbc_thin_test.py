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
This module contains Thin JDBC driver tests.
"""

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinJdbcConfiguration
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


class JdbcThinTest(IgniteTest):
    """
    Thin JDBC driver test.
    """
    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="server_version")
    @ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="thin_jdbc_version")
    def test_simple_insert_select(self, server_version, thin_jdbc_version):
        """
        Smoke test ensuring the Thin JDBC driver just works doing simple SQL queries
        and that the compationility between Ignite versions is preserved.
        """
        server_config = IgniteConfiguration(
            version=IgniteVersion(server_version),
            client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, server_config, 1)

        ignite.start()

        ControlUtility(ignite).activate()

        address = ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)

        app = IgniteApplicationService(
            self.test_context,
            IgniteThinJdbcConfiguration(
                version=IgniteVersion(thin_jdbc_version),
                addresses=[address]
            ),
            java_class_name="org.apache.ignite.internal.ducktest.tests.jdbc.JdbcThinSelfTestApplication",
            num_nodes=1)

        app.start()
        app.await_stopped()

        ignite.stop()
