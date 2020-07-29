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
This module contains spark integration test.
"""

from ducktape.mark import parametrize

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite_spark_app import SparkIgniteApplicationService
from ignitetest.services.spark import SparkService
from ignitetest.tests.utils.ignite_test import IgniteTest
from ignitetest.tests.utils.version import DEV_BRANCH


# pylint: disable=W0223
class SparkIntegrationTest(IgniteTest):
    """
    Test performs:
    1. Start of Spark cluster.
    2. Start of Spark client application.
    3. Checks results of client application.
    """

    def __init__(self, test_context):
        super(SparkIntegrationTest, self).__init__(test_context=test_context)
        self.spark = None
        self.ignite = None

    def setUp(self):
        pass

    def teardown(self):
        self.spark.stop()
        self.ignite.stop()

    @parametrize(version=str(DEV_BRANCH))
    def test_spark_client(self, version):
        """
        Performs test scenario.
        """
        self.spark = SparkService(self.test_context, version=version, num_nodes=2)
        self.spark.start()

        self.ignite = IgniteService(self.test_context, version=version, num_nodes=1)
        self.ignite.start()

        self.stage("Starting sample data generator")

        IgniteApplicationService(
            self.test_context,
            java_class_name="org.apache.ignite.internal.ducktest.tests.spark_integration_test."
                            "SampleDataStreamerApplication",
            params="cache,1000",
            version=version).run()

        self.stage("Starting Spark application")

        SparkIgniteApplicationService(
            self.test_context,
            "org.apache.ignite.internal.ducktest.tests.spark_integration_test.SparkApplication",
            params="spark://" + self.spark.nodes[0].account.hostname + ":7077",
            version=version,
            timeout_sec=120).run()
