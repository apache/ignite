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
This module contains tests for calcite engine
"""
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinJdbcConfiguration
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion, LATEST

from ignitetest.utils.bean import Bean


class CalciteTest(IgniteTest):
    """
    Calcite engine tests
    """
    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_std_sql_operators_via_thin_jdbc(self, ignite_version):
        """
        This test validates the proper functioning of SQL operators through the thin JDBC client on the Calcite engine.
        """
        config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            client_connector_configuration=ClientConnectorConfiguration(),
            sql_configuration=Bean("org.apache.ignite.configuration.SqlConfiguration", query_engines_configuration=[
                    Bean("org.apache.ignite.calcite.CalciteQueryEngineConfiguration", default=True)]))

        ignite = IgniteService(self.test_context, config, num_nodes=1, modules=["calcite"])

        ignite.start()

        address = ignite.nodes[0].account.hostname + ":" + str(config.client_connector_configuration.port)

        app = IgniteApplicationService(
            self.test_context,
            IgniteThinJdbcConfiguration(
                addresses=[address]
            ),
            java_class_name="org.apache.ignite.internal.ducktest.tests.calcite.CalciteTestingApplication",
            num_nodes=1)

        app.start()

        app.await_stopped()

        ignite.stop()
