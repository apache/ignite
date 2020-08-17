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

from ducktape.mark import parametrize

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.spark import SparkService
from ignitetest.services.zk.zookeeper import ZookeeperService
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH


# pylint: disable=W0223
class SmokeServicesTest(IgniteTest):
    """
    Tests services implementations
    """
    def __init__(self, test_context):
        super(SmokeServicesTest, self).__init__(test_context=test_context)

    def setUp(self):
        pass

    def teardown(self):
        pass

    @parametrize(version=str(DEV_BRANCH))
    def test_ignite_start_stop(self, version):
        """
        Test that IgniteService correctly start and stop
        """
        ignite = IgniteService(
            self.test_context,
            num_nodes=1,
            version=version)
        ignite.start()
        ignite.stop()

    @parametrize(version=str(DEV_BRANCH))
    def test_ignite_app_start_stop(self, version):
        """
        Test that IgniteService and IgniteApplicationService correctly start and stop
        """
        ignite = IgniteService(
            self.test_context,
            num_nodes=1,
            version=version)

        app = IgniteApplicationService(
            self.test_context,
            java_class_name="org.apache.ignite.internal.ducktest.tests.smoke_test.SimpleApplication",
            version=version)

        ignite.start()
        app.start()
        app.stop()
        ignite.stop()

    def test_spark_start_stop(self):
        """
        Test that SparkService correctly start and stop
        """
        spark = SparkService(self.test_context, num_nodes=2)
        spark.start()
        spark.stop()

    def test_zk_start_stop(self):
        """
        Test that ZookeeperService correctly start and stop
        """
        zookeeper = ZookeeperService(self.test_context, num_nodes=2)
        zookeeper.start()
        zookeeper.stop()
