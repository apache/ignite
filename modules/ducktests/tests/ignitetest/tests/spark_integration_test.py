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

from ignitetest.benchmarks.ignite_test import IgniteTest
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite_spark_app import SparkIgniteApplicationService
from ignitetest.services.spark import SparkService


class SparkIntegrationTest(IgniteTest):
    """
    Test performs:
    1. Start of Spark cluster.
    2. Start of Spark client application.
    3. Checks results of client application.
    """

    @staticmethod
    def properties(client_mode="false"):
        return """
            <property name="clientMode" value="{client_mode}"/>
        """.format(client_mode=client_mode)

    def __init__(self, test_context):
        super(SparkIntegrationTest, self).__init__(test_context=test_context)
        self.spark = SparkService(test_context, num_nodes=2)
        self.ignite = IgniteService(test_context, num_nodes=1)

    def setUp(self):
        self.spark.start()
        self.ignite.start()

    def teardown(self):
        self.spark.stop()
        self.ignite.stop()

    def test_spark_client(self):
        self.stage("Starting sample data generator")

        IgniteApplicationService(self.test_context,
                                 java_class_name="org.apache.ignite.internal.test.SampleDataStreamerApplication",
                                 params="cache,1000",
                                 properties=self.properties(client_mode="true")).run()

        self.stage("Starting Spark application")

        SparkIgniteApplicationService(self.test_context,
                                      "org.apache.ignite.internal.test.SparkApplication",
                                      params="spark://" + self.spark.nodes[0].account.hostname + ":7077",
                                      timeout_sec=120).run()
