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

from ducktape.tests.test import Test

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_client_app import IgniteClientApp, SparkIgniteClientApp
from ignitetest.services.spark import SparkService


class SparkIntegrationTest(Test):
    """
    Test performs:
    1. Start of Spark cluster.
    2. Start of Spark client application.
    3. Checks results of client application.
    """

    def __init__(self, test_context):
        super(SparkIntegrationTest, self).__init__(test_context=test_context)
        self.spark = SparkService(test_context, num_nodes=2)
        self.ignite = IgniteService(test_context, num_nodes=1)

    def setUp(self):
        # starting all nodes except last.
        self.spark.start()
        self.ignite.start()

    def teardown(self):
        self.spark.stop()
        self.ignite.stop()

    def test_spark_client(self):
        self.logger.info("Spark integration test.")

        IgniteClientApp(self.test_context,
                        java_class_name="org.apache.ignite.internal.test.IgniteApplication").run()

        SparkIgniteClientApp(self.test_context, self.spark.nodes[0]).run()
