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
This module contains smoke tests that checks that services work
"""

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.spark import SparkService
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.zk.zookeeper import ZookeeperService
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class SmokeServicesTest(IgniteTest):
    """
    Tests services implementations
    """

    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    def test_ignite_start_stop(self, ignite_version):
        """
        Test that IgniteService correctly start and stop
        """
        ignite = IgniteService(self.test_context, IgniteConfiguration(version=IgniteVersion(ignite_version)),
                               num_nodes=1)
        print(self.test_context)
        ignite.start()
        ignite.stop()

    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    def test_ignite_app_start_stop(self, ignite_version):
        """
        Test that IgniteService and IgniteApplicationService correctly start and stop
        """
        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version))

        ignite = IgniteService(self.test_context, server_configuration, num_nodes=1)

        client_configuration = server_configuration._replace(client_mode=True,
                                                             discovery_spi=from_ignite_cluster(ignite))
        app = IgniteApplicationService(
            self.test_context,
            client_configuration,
            java_class_name="org.apache.ignite.internal.ducktest.tests.smoke_test.SimpleApplication")

        ignite.start()
        app.start()
        app.stop()
        ignite.stop()

    @cluster(num_nodes=2)
    def test_spark_start_stop(self):
        """
        Test that SparkService correctly start and stop
        """
        spark = SparkService(self.test_context, num_nodes=2)
        spark.start()
        spark.stop()

    @cluster(num_nodes=3)
    def test_zk_start_stop(self):
        """
        Test that ZookeeperService correctly start and stop
        """
        zookeeper = ZookeeperService(self.test_context, num_nodes=3)
        zookeeper.start()
        zookeeper.stop()
