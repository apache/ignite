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
This module contains ssl tests
"""

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, SslContextFactory
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion, LATEST_2_7, LATEST_2_8, LATEST_2_9


# pylint: disable=W0223
class SslTest(IgniteTest):
    """
    Ssl test
    """
    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    def test_ssl_connection(self, ignite_version):
        """
        Ssl test
        """
        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version))

        ignite = IgniteService(self.test_context, server_configuration, num_nodes=2,
                               startup_timeout_sec=180)

        ssl_context_factory = SslContextFactory(ignite, key_store_jks="server.jks")

        server_configuration.ssl_context_factory = ssl_context_factory

        control_utility = ControlUtility(cluster=ignite, key_store_jks="admin.jks")

        # client_configuration = server_configuration._replace(client_mode=True,
        #                                                      discovery_spi=from_ignite_cluster(ignite))
        # app = IgniteApplicationService(
        #     self.test_context,
        #     client_configuration,
        #     java_class_name="org.apache.ignite.internal.ducktest.tests.smoke_test.SimpleApplication")

        ignite.start()
        control_utility.cluster_state()
        # app.start()
        ignite.stop()

    # @cluster(num_nodes=3)
    # @ignite_versions(str(DEV_BRANCH))
    # def test_ignite_app_start_stop(self, ignite_version):
    #     """
    #     Test that IgniteService and IgniteApplicationService correctly start and stop
    #     """
    #     server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version))
    #
    #     ignite = IgniteService(self.test_context, server_configuration, num_nodes=1)
    #
    #     client_configuration = server_configuration._replace(client_mode=True,
    #                                                          discovery_spi=from_ignite_cluster(ignite))
    #     app = IgniteApplicationService(
    #         self.test_context,
    #         client_configuration,
    #         java_class_name="org.apache.ignite.internal.ducktest.tests.smoke_test.SimpleApplication")
    #
    #     ignite.start()
    #     app.start()
    #     app.stop()
    #     ignite.stop()
